"""The bridge's sessions: connecting, connection slots, transactions and the catalog."""

from __future__ import annotations

import codecs
import contextlib
import functools
import hashlib
import logging
import os
import random
import secrets
import stat
import threading
import time
from typing import Any, Callable, Iterable, Sequence

from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _capture_descriptor,
    _catalog_column,
    _field,
    _sysmaster_database_name,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _PROMOTE_UNIQUE_INDEX_OPTION,
    _option_bool,
    _resolve_daemon_reservation,
    _resolve_snapshot_reader_threads,
    _select_unique_index_key,
    _serialized_sqli_operation,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CATALOG_BATCH,
    _CATALOG_COLUMNS_SQL,
    _CONNECTION_BACKLOG_RANK_LEVELS,
    _CONNECTION_HINT_MAX_AGE_SECONDS,
    _CONNECTION_LIMIT_CONFIG_VERSION,
    _CONNECTION_SLOT_HEARTBEAT_SECONDS,
    _CONNECTION_SLOT_JOIN_SECONDS,
    _CONNECTION_SLOT_LEASE_SECONDS,
    _CONNECTION_STARTUP_JITTER_OPTION,
    _DAEMON_RESERVATION_OPTION,
    _DAEMON_SLOT_MARKER_OPTION,
    _DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS,
    _DEFAULT_DAEMON_CONNECTION_RESERVATION,
    _DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    _IDENTIFIER,
    _LAKEBASE_ENDPOINT_HINT_KEY,
    _LAKEBASE_SWEEP_MAX_SECONDS,
    _LAKEBASE_SWEEP_MIN_SECONDS,
    _LAKEBASE_WAITER_CONNECTION,
    _LAKEBASE_WAITER_STATE,
    _LAKEBASE_WAITER_TTL_SECONDS,
    _POISON_OPERATION_LOCK_SECONDS,
    _PRIMARY_KEY_SQL,
    _SLOT_CLAIM_RECONNECTS,
    _SLOT_CLASS_CDC_DAEMON,
    _SLOT_CLASS_CONSUMER,
    _SLOT_CLASS_COUNT,
    _SLOT_CLASS_SNAPSHOT_DAEMON,
    _SNAPSHOT_DAEMON_SLOT_MARKER_OPTION,
    _UNIQUE_INDEX_SQL,
    InformixError,
    _lakebase_waiter_lock,
    _m,
    _split_identity,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _SLOT_SWEEP_RANK_FLOOR,
    ConnectionCapacityUnavailable,
    Table,
    _open_state_connection,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _open_state_directory,
    _remove_candidate_tree_at,
)
from databricks.labs.community_connector.sources.informix.informix_unload import _deep_size
from databricks.labs.community_connector.sources.informix.lakebase_state import (
    connection_lost_error_types,
)
from databricks.labs.community_connector.sources.informix.lakebase_state_store import (
    LakebaseState,
    acquire_slot,
    dequeue_waiter,
    enqueue_waiter,
    heartbeat_slot,
    heartbeat_waiter,
    pool_progress_token,
    publish_backlog_hint,
    publish_connection_limit,
    read_backlog_hints,
    release_slot,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import seed_slots
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    SqliProtocolError,
    SqliTransportError,
    informix_locale_encoding,
)


class _BridgeSessionMixin:
    # Docstring: The bridge's sessions: connecting, connection slots, transactions and the
    # catalog.

    def _call_flag(self, name: str, default: Any = None) -> Any:
        # Docstring: What the connector's call under way set for this thread: a capacity
        # bypass, a slot-wait budget, a backlog rank (the connector's _call_context).
        return getattr(getattr(self, "_call_context", None), name, default)

    def _apply_startup_jitter(self) -> None:
        # Docstring: Sleep a one-time jittered delay before this reader's first connection.
        #
        # Spreads a pipeline's startup connection/CDC-session opens over a window so dozens of
        # flows do not hit syscdcv1 simultaneously. One-shot per reader instance: the flag is
        # set before sleeping, so a failed connect that retries does not re-delay, and every
        # connection after the first is immediate.

        if getattr(self, "_startup_jitter_applied", True):
            return
        self._startup_jitter_applied = True
        try:
            jitter = float(self.options.get(_CONNECTION_STARTUP_JITTER_OPTION, "0") or 0)
        except (TypeError, ValueError):
            jitter = 0.0
        if jitter > 0:
            time.sleep(random.uniform(0, jitter))

    def _ensure_connected(self) -> None:
        # Tests and injected bridges built with ``object.__new__`` already
        # supply a ready transport and intentionally bypass ``__init__``.
        if getattr(self, "_connected", True):
            return
        with self._connect_lock:
            if self._connected:
                return
            # Before acquiring a slot -- a jittering reader holds nothing while it waits.
            self._apply_startup_jitter()
            bypass_capacity = bool(self._call_flag("bypass_capacity", False))
            # A reconnect after reset_transport still holds its slot and heartbeat; claiming
            # another would strand the first, renewed until the process exits. Its connect
            # failing keeps it too, for the retry: release_connection frees it in the end.
            held = getattr(self, "_lakebase_slot", None) is not None
            if not bypass_capacity and not held:
                budget = self._call_flag("attempt_budget")
                budget = float(budget) if budget else None
                # Lakebase can reap the state connection while this bridge sits idle
                # between reads, or drop it during a long wait for a slot, which psycopg
                # reports only once a query fails: reopen it and claim again. A live
                # connection's error is real. The wait as a whole keeps its limit: each
                # further claim gets what is left of it, not the whole of it again.
                limit = float(
                    self.options.get(
                        "connection.wait.timeout.seconds",
                        str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
                    )
                )
                limit = limit if budget is None else min(limit, budget)
                started, remaining = time.monotonic(), budget
                # A daemon with no budget of its own waits by turnover: its limit is a
                # window without any slot changing hands, begun again at each claim (see
                # _acquire_connection_slot), not a span of wall-clock time to share out.
                # Given what was left of that time, at least a second, a claim after a
                # long wait on a pool turning over failed within that second.
                turnover = budget is None and any(
                    self.options.get(marker) == "true"
                    for marker in (_DAEMON_SLOT_MARKER_OPTION, _SNAPSHOT_DAEMON_SLOT_MARKER_OPTION)
                )
                for attempt in range(_SLOT_CLAIM_RECONNECTS + 1):
                    try:
                        self._acquire_connection_slot(remaining)
                        break
                    except connection_lost_error_types():
                        connection = getattr(self, "_lakebase_conn", None)
                        if connection is not None and not connection.closed:
                            raise
                        if attempt >= _SLOT_CLAIM_RECONNECTS:
                            raise
                        self._lakebase_conn = None
                        remaining = (
                            None if turnover else max(limit - (time.monotonic() - started), 1.0)
                        )
            try:
                self._connect_transport()
                self._connected = True
            except Exception:
                # The session may be open already: a check after the login failed. Not
                # closed, it stayed open on the server while its slot went to another
                # reader, outside max.concurrent.connections, and release_connection
                # skips a bridge that is not connected.
                close = getattr(self.transport, "close", None)
                if callable(close):
                    with contextlib.suppress(Exception):
                        close()
                if not held:
                    self._release_connection_slot()
                raise

    def _session_probe_failed(self, error: BaseException) -> None:
        # Docstring: After a session setup statement failed: raise if the connection broke or
        # its slot's lease was lost; otherwise connect again, since a refused statement ends
        # the SQLI session. A broken connection is not taken for a refusal: the caller
        # retries, and the probe runs again on the next connection.

        if isinstance(error, (SqliTransportError, OSError)):
            raise error
        lease_lost = getattr(self, "_connection_lease_lost", None)
        if lease_lost is not None and lease_lost.is_set():
            raise error
        close = getattr(self.transport, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()
        connect = getattr(self.transport, "connect", None)
        if callable(connect):
            connect()

    def _end_ansi_transaction(self, *, rollback: bool = False) -> None:
        # Docstring: End the transaction an operation's statements opened in an ANSI database,
        # where every statement opens one and it keeps what it read locked until a COMMIT
        # WORK. Left open, a CDC shard leader's catalog and log position reads held their
        # locks on systables and the rest through a whole catch-up, and DDL on the tables
        # waited. A snapshot's transaction ends itself (_repeatable_read_transaction). A
        # failed operation's is rolled back.

        if self.__dict__.get("_is_ansi_database") and getattr(self.transport, "uncommitted", False):
            try:
                self.transport.execute_command("ROLLBACK WORK" if rollback else "COMMIT WORK")
            except (SqliProtocolError, OSError):
                # A session that broke under it ended its transaction, and the operation
                # stands: it read and returned. The next one connects again.
                self.reset_transport()

    def _connect_transport(self) -> None:
        connect = getattr(self.transport, "connect", None)
        if connect:
            connect()
        self._check_database_locale()
        # Whether each operation's transaction is the bridge's to end (_end_ansi_transaction):
        # known only to a snapshot's before, which a shard leader's reader never reads.
        execute = getattr(self.transport, "execute", None)
        if (
            self.__dict__.get("_is_ansi_database") is None
            and not self.__dict__.get("_ansi_probe_refused")
            and callable(execute)
        ):
            try:
                rows = execute(
                    "SELECT is_ansi FROM sysmaster:sysdatabases WHERE name = ?",
                    (_sysmaster_database_name(self.config["database"]),),
                )
                # Unknown where no row says (a restricted view of sysmaster): left for the
                # snapshot to ask, which fails on it (47), not taken for a database without.
                if len(rows) == 1:
                    self._is_ansi_database = bool(int(_field(rows[0], "is_ansi", 0)))
            except (SqliProtocolError, OSError) as error:
                self._session_probe_failed(error)
                # Refused, not asked again: the refusal ended the session, and each later
                # connect opened two, as the locale and newline probes don't.
                self._ansi_probe_refused = True
            except (AttributeError, KeyError, TypeError, ValueError):
                pass  # left to the first snapshot to find out, as before
        # Informix takes a string value as a quoted literal within the statement, and by
        # default (ALLOW_NEWLINE 0) refuses one that holds a newline with -282, which
        # also ends the session. A keyset bound or a looked-up key can hold one. The
        # procedure allows them for this session (probed on 14.10: it must be sent as a
        # command, and a value with a newline then compares as any other).
        command = getattr(self.transport, "execute_command", None)
        if not callable(command):
            return
        if not self.__dict__.get("_newline_refused"):
            try:
                command("EXECUTE PROCEDURE ifx_allow_newline('t')")
            except Exception as error:  # pylint: disable=broad-except
                # A refusal ends the session: connect again, and do not ask a second time.
                # A broken connection or a lost lease is no refusal: _session_probe_failed
                # raises for those, before anything is marked (a lease shutdown closing the
                # client shows as "session is not open", which is neither).
                self._session_probe_failed(error)
                self._newline_refused = True
                # 446: Informix did not allow newlines in quoted strings for this session
                # ({}); a string value that holds a newline fails with SQL error -282
                logging.getLogger(__name__).warning(_m(446, error))
        # In an ANSI database the probes opened a transaction: ended here, as an operation's
        # is, so that one connecting outside an operation does not hold its locks. A failure
        # fails the connect.
        if self.__dict__.get("_is_ansi_database") and getattr(self.transport, "uncommitted", False):
            command("COMMIT WORK")

    def _check_database_locale(self) -> None:
        # Docstring: Fail a DB_LOCALE whose codeset is not the database's.
        #
        # The server sends every text in the database's codeset, and the client decodes it
        # in that of its DB_LOCALE (InformixSqliClient._text_encoding). The default
        # en_US.819 on a UTF-8 database decodes without an error, every byte being a
        # Latin-1 character, and lands garbled text. The database's own locale is in
        # sysmaster. Checked once per bridge; a client without a DB_LOCALE has nothing to
        # check.

        if self.__dict__.get("_database_locale_checked"):
            return
        configured = getattr(self.transport, "db_locale", None)
        database = (getattr(self, "config", None) or {}).get("database")
        execute = getattr(self.transport, "execute", None)
        if not isinstance(configured, str) or not database or not callable(execute):
            return
        try:
            rows = execute(
                "SELECT dbs_collate FROM sysmaster:sysdbslocale WHERE dbs_dbsname = ?",
                (_sysmaster_database_name(database),),
            )
        except Exception as error:  # pylint: disable=broad-except
            # A refusal ends the session: connect again, and do not ask a second time.
            # A broken connection or a lost lease raises first (see above).
            self._session_probe_failed(error)
            self._database_locale_checked = True
            # 456: Could not read the locale of the database from sysmaster ({}); its
            # text is decoded in the codeset of DB_LOCALE without that check
            logging.getLogger(__name__).warning(_m(456, error))
            return
        row = rows[0] if isinstance(rows, list) and rows else {}
        actual = str((row.get("dbs_collate") if isinstance(row, dict) else "") or "").strip()
        if actual:
            try:
                same = (
                    codecs.lookup(informix_locale_encoding(actual)).name
                    == codecs.lookup(informix_locale_encoding(configured)).name
                )
            except (LookupError, ValueError, SqliProtocolError):
                # A locale name the connector has no codec for: no telling whether it is
                # DB_LOCALE's codeset under another name, so it is not refused.
                same = True
            if not same:
                # 455: DB_LOCALE is '{}' but database '{}' has the locale '{}': set DB_LOCALE
                # to it, since Informix sends the database's text in its codeset
                raise InformixError(_m(455, configured, database, actual))
        self._database_locale_checked = True

    @staticmethod
    def _connection_identity_from(source: dict[str, Any]) -> str:
        # Docstring: Identify the Informix endpoint whose capacity is being shared.
        #
        # Deliberately excludes the database name: capacity is a property of the
        # server process, so every database on one server draws from one pool.
        #
        # Takes the mapping so a holder (which has a parsed ``config``) and a waiter
        # (which has only ``options``) derive the *same* identity. They must: the
        # holder publishes the backlog hint and the waiter reads it, so a divergence
        # here would leave every hint silently unreadable rather than failing
        # loudly.

        return "\0".join(
            (
                str(source.get("hostname", "")).strip().rstrip(".").casefold(),
                str(int(source.get("port", 9088) or 9088)),
                str(source.get("server", "")).strip().casefold(),
            )
        )

    def _connection_identity(self) -> str:
        return self._connection_identity_from(getattr(self, "config", None) or self.options)

    def _lakebase(self) -> Any:
        # Docstring: Return this process's Lakebase state handle, provisioning on first use.

        existing = getattr(self, "_lakebase_state", None)
        if existing is not None:
            return existing
        state = LakebaseState(self.options, self._connection_identity())
        state.provision()
        self._lakebase_state = state
        return state

    def _lakebase_connection(self) -> Any:
        # Docstring: Return a per-bridge Postgres connection, reconnecting if it has died.
        #
        # One connection per bridge rather than a pool: a bridge serves a single
        # table's reads sequentially, and the endpoint permits 450 connections
        # (measured), which is ample for a pipeline's flows.

        connection = _open_state_connection(getattr(self, "_lakebase_conn", None))
        if connection is not None:
            return connection
        connection = self._lakebase().connect()
        self._lakebase_conn = connection
        return connection

    def _lakebase_namespace(self) -> str:
        # Docstring: Namespace every row by the Informix endpoint sharing the capacity.

        return hashlib.sha256(self._connection_identity().encode()).hexdigest()[:24]

    def _connection_hints_enabled(self) -> bool:
        # Parsed as every other flag is: "False", "0" and "no" turned it off too.
        return _option_bool(self.options, "connection.backlog.hint.enabled", True)

    def _publish_connection_backlog_hint(self) -> None:
        # Docstring: Record the endpoint's current log position for waiters to read.
        #
        # Called by a reader that still holds a slot, which is the only participant
        # able to ask the server anything. One monotonic upsert: ``GREATEST`` makes
        # concurrent publications converge, so every holder may publish and the
        # value can never regress -- no per-quantum election is needed.
        #
        # Advisory. Every failure is swallowed, because a missing hint only degrades
        # waiters to the unprioritised behaviour they had before hints existed and
        # must never fail a read that otherwise succeeded.

        if not self._connection_hints_enabled():
            return
        try:
            publish_backlog_hint(
                self._lakebase_connection(),
                self._lakebase_namespace(),
                _LAKEBASE_ENDPOINT_HINT_KEY,
                self.current_lsn(),
            )
        except Exception:
            # 15: Informix backlog hint publication failed
            logging.getLogger(__name__).debug(_m(15), exc_info=True)

    @classmethod
    def _read_connection_backlog_hint_at(cls, options: dict[str, str]) -> int | None:
        # Docstring: Return the published endpoint log position from Postgres, or None.
        #
        # Called by a *waiter*, which holds no slot and has no bridge, so it cannot
        # reuse a bridge's connection. The handle is cached per process and keyed by
        # endpoint identity; a waiter reads once per sweep, so one shared connection
        # per process is enough.
        #
        # Advisory, so every failure yields None and the caller falls back to
        # unprioritised waiting rather than failing.

        identity = cls._connection_identity_from(options)
        namespace = hashlib.sha256(identity.encode()).hexdigest()[:24]
        key = None
        try:
            with _lakebase_waiter_lock():
                # Keyed by the endpoint and what the state was provisioned for: the
                # project, database and a digest of the password. By the endpoint alone a
                # warm process kept the state of an old lakebase.password or project, and
                # every sweep failed its login until the process ended.
                fresh = LakebaseState(options, identity)
                key = (namespace, *fresh._provision_key())
                state = _LAKEBASE_WAITER_STATE.get(key)
                if state is None:
                    state = fresh
                    state.provision()
                    _LAKEBASE_WAITER_STATE[key] = state
                connection = _open_state_connection(_LAKEBASE_WAITER_CONNECTION.get(key))
                if connection is None:
                    connection = state.connect()
                    _LAKEBASE_WAITER_CONNECTION[key] = connection
                hints = read_backlog_hints(
                    connection, namespace, max_age_seconds=_CONNECTION_HINT_MAX_AGE_SECONDS
                )
            return hints.get(_LAKEBASE_ENDPOINT_HINT_KEY)
        except Exception:
            with _lakebase_waiter_lock():
                # Drop a connection that may be broken so the next read reopens, and the
                # state it came from: the next one provisions again, from the process's
                # cache when nothing changed.
                broken = _LAKEBASE_WAITER_CONNECTION.pop(key, None)
                _LAKEBASE_WAITER_STATE.pop(key, None)
            if broken is not None:
                with contextlib.suppress(Exception):
                    broken.close()
            # 16: Informix Lakebase backlog hint read failed
            logging.getLogger(__name__).debug(_m(16), exc_info=True)
            return None

    def _daemon_connection_reservation(self, slot_count: int) -> int:
        reservation = int(
            self.options.get(
                _DAEMON_RESERVATION_OPTION,
                str(_DEFAULT_DAEMON_CONNECTION_RESERVATION),
            )
        )
        if not 0 <= reservation < slot_count:
            # At slot_count a daemon could never claim a slot at all, stranding the
            # sharded CDC session and the snapshot drain permanently.
            # 17: Option '{}' must be >= 0 and < max.concurrent.connections ({})
            raise ValueError(_m(17, _DAEMON_RESERVATION_OPTION, slot_count))
        # A 0 (unset) reservation reserves a third of the pool by default (rounded up to
        # at least 1 whenever the pool has 2+ slots, 0 only for a single-slot pool), so a
        # fresh consumer bootstrap read always has slots the daemons cannot pin. Set a
        # positive value to give the daemons more of the pool (down to reserving a single
        # slot); a single-slot pool reserves nothing because it cannot spare one. Resolved
        # through the shared helper so the snapshot-reader-thread default tracks the same value.
        return _resolve_daemon_reservation(self.options, slot_count)

    def _snapshot_daemon_connection_reservation(self, slot_count: int) -> int:
        # Docstring: Floor for the snapshot-drain daemon: one band below the CDC daemon.
        #
        # The CDC daemon is deferrable and floored at ``daemon.connection.reservation``;
        # a drain is bootstrap work that must make progress, so it sits ``snapshot.reader
        # .threads`` slots lower, giving it that many slots the CDC daemon cannot claim.
        # The band below it (``daemon.connection.reservation - snapshot.reader.threads``
        # slots) stays reachable only by consumer bootstrap reads at floor 0. When the
        # pool cannot spare a private drain band the floor collapses to 0, where the drain
        # still reaches the CDC-free low slots (consumer reads release per microbatch, so
        # those slots churn) -- it simply loses its dedicated headroom.

        daemon_reservation = self._daemon_connection_reservation(slot_count)
        # Reserve a band exactly as wide as the number of drain workers the pool will
        # actually spawn -- resolved through the same module helper the connector's
        # _snapshot_reader_thread_count uses, so a pool-relative default
        # (max.concurrent.connections // 3) and an explicit override both stay in lockstep
        # with the reservation. A flat constant here would under-reserve once the default
        # scales with the pool.
        threads = _resolve_snapshot_reader_threads(self.options)
        return max(0, daemon_reservation - threads)

    def _connection_sweep_rank_scale(self) -> float:
        # Docstring: Shrink the gap between acquisition sweeps for a further-behind reader.
        #
        # The connector publishes the rank alongside the attempt budget, so this
        # needs no offset access of its own. Returns 1.0 when no rank is present,
        # which leaves the cadence byte-identical to the pre-rank behaviour.
        #
        # Linear between 1.0 (unranked) and ``_SLOT_SWEEP_RANK_FLOOR`` (top rank).
        # Bounded deliberately: a freed slot goes to whoever sweeps next, so halving
        # the gap is enough to bias that race, while an unbounded shrink would
        # reintroduce exactly the metadata storm the wide interval exists to prevent.

        raw = self._call_flag("attempt_rank", 0)
        if not raw:
            return 1.0
        try:
            rank = int(raw)
        except (TypeError, ValueError):
            return 1.0
        top = _CONNECTION_BACKLOG_RANK_LEVELS - 1
        if rank <= 0 or top <= 0:
            return 1.0
        fraction = min(1.0, rank / top)
        return 1.0 - (1.0 - _SLOT_SWEEP_RANK_FLOOR) * fraction

    def _connection_waiter_class(self) -> int:
        # Docstring: The requester class this reader enqueues under for the round-robin policy.
        #
        # Derived from the private slot markers already on the reader's options, checked
        # daemon-first so a daemon reader is never misclassified as a plain consumer. The
        # class only interleaves the queue; it does not change which slots a reader may
        # claim (that is the reservation ``floor``).

        if self.options.get(_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION) == "true":
            return _SLOT_CLASS_SNAPSHOT_DAEMON
        if self.options.get(_DAEMON_SLOT_MARKER_OPTION) == "true":
            return _SLOT_CLASS_CDC_DAEMON
        return _SLOT_CLASS_CONSUMER

    # pylint: disable-next=too-many-statements
    def _acquire_connection_slot(self, budget_seconds: float | None = None) -> None:
        # Docstring: Claim a capacity slot from Postgres.
        #
        # Structurally the same wait loop as the Volume path -- the same timeout,
        # the same rank-scaled cadence, the same reservation floor -- but each pass
        # is a single atomic statement instead of a per-slot ``mkdir`` sweep, so
        # there is no fence to check afterwards and no partial claim to unwind.

        namespace = self._lakebase_namespace()
        slot_count = int(
            self.options.get(
                "max.concurrent.connections",
                str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
            )
        )
        floor = 0
        # The sharded CDC daemon draws from the same pool but, unlike a consumer read,
        # never releases per microbatch: a busy CDC shard re-reads in a tight loop. With
        # ``cdc.shared.reader.threads`` threads on each of two channels, it can pin every
        # slot and starve a fresh consumer bootstrap read (which then times out and fails
        # the query). Floor it above the daemon reservation so those low slots stay
        # reachable by consumer reads.
        is_cdc_daemon = self.options.get(_DAEMON_SLOT_MARKER_OPTION) == "true"
        if is_cdc_daemon:
            floor = max(floor, self._daemon_connection_reservation(slot_count))
        # The snapshot-drain daemon also holds its slot for the whole scan, but it is
        # bootstrap work that must finish to unblock its consumer -- so it floors *below*
        # the CDC daemon into a band the CDC daemon cannot claim, rather than sharing the
        # CDC floor and being starved by a saturated CDC daemon.
        is_snapshot_daemon = self.options.get(_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION) == "true"
        if is_snapshot_daemon:
            floor = max(floor, self._snapshot_daemon_connection_reservation(slot_count))
        connection_wait_timeout = float(
            self.options.get(
                "connection.wait.timeout.seconds",
                str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
            )
        )
        if budget_seconds is not None:
            connection_wait_timeout = min(connection_wait_timeout, budget_seconds)
        sweep_scale = self._connection_sweep_rank_scale()
        sweep_min = _LAKEBASE_SWEEP_MIN_SECONDS * sweep_scale
        sweep_max = _LAKEBASE_SWEEP_MAX_SECONDS * sweep_scale
        connection = self._lakebase_connection()
        # Capacity is the row count, so the rows must exist before the first
        # claim. Seeding is idempotent and only ever adds rows, and both writes change
        # nothing until the limit does, so a bridge makes them once per limit rather
        # than on every claim. Both commit before the claim, so a claim that gives up
        # leaves them in place.
        seeded = (namespace, slot_count)
        if getattr(self, "_seeded_connection_limit", None) != seeded:
            seed_slots(connection, namespace, slot_count)
            publish_connection_limit(
                connection, namespace, slot_count, _CONNECTION_LIMIT_CONFIG_VERSION
            )
            self._seeded_connection_limit = seeded
        owner = secrets.token_hex(16)
        deadline = time.monotonic() + connection_wait_timeout
        # Slot-wait liveness (the two daemon roles). A daemon is not a consumer microbatch: it must
        # not fail merely because the pool is *full* -- only when it is *wedged*. So its deadline is
        # a liveness watchdog keyed on the pool's progress token: as long as slots keep being
        # claimed, or the snapshot drains holding them keep staging pages (the token advances --
        # other work is going on and will recycle capacity), reset the deadline; give up only
        # after a whole window with neither. The snapshot drain additionally pings its
        # waiting consumer each turnover. The CDC shard daemon uses the same watchdog so it holds
        # its queue ticket long enough for the round-robin rotation to reach it -- a round is paid
        # in turnover, not wall-clock -- rather than failing fast on a short budget and re-queuing
        # at the back; on a genuine wedge it gives up and its consumers degrade to direct reads.
        # Other callers keep the plain deadline (a consumer that cannot get a slot should fail its
        # microbatch and retry, not wait out a backfill).
        liveness = is_snapshot_daemon or is_cdc_daemon
        progress_token = pool_progress_token(connection, namespace) if liveness else None
        # Fair queue: enqueue a ticket so acquisition waits its turn instead of a raw race,
        # then heartbeat it each sweep and drop it the moment we win or give up. The ticket
        # carries this reader's requester class so the round-robin policy can interleave
        # classes.
        ticket_id = enqueue_waiter(
            connection,
            namespace,
            owner,
            floor=floor,
            ceiling=slot_count,
            waiter_class=self._connection_waiter_class(),
        )
        try:
            while True:
                # Renew our place and reap any crashed/timed-out waiters so a dead ticket
                # cannot block us; a lost state connection here is transient, so let it
                # surface and be retried by the normal reconnect path. If our own ticket
                # was reaped (a stall longer than the waiter TTL between two sweeps), take
                # a fresh one: acquisition grants only to a live ticket, so sweeping on with
                # a dead one could never win. The new ticket joins the back of its class's
                # rotation, which is the fair outcome for a waiter that went dark.
                if not heartbeat_waiter(
                    connection, namespace, ticket_id, waiter_ttl=_LAKEBASE_WAITER_TTL_SECONDS
                ):
                    # 18: Informix connection-slot waiter ticket {} was reaped; re-enqueuing
                    logging.getLogger(__name__).warning(
                        _m(18, ticket_id),
                    )
                    ticket_id = enqueue_waiter(
                        connection,
                        namespace,
                        owner,
                        floor=floor,
                        ceiling=slot_count,
                        waiter_class=self._connection_waiter_class(),
                    )
                slot = acquire_slot(
                    connection,
                    namespace,
                    owner,
                    slot_count=slot_count,
                    floor=floor,
                    lease_seconds=_CONNECTION_SLOT_LEASE_SECONDS,
                    ticket_id=ticket_id,
                    waiter_ttl=_LAKEBASE_WAITER_TTL_SECONDS,
                    waiter_class=self._connection_waiter_class(),
                    num_classes=_SLOT_CLASS_COUNT,
                )
                if slot is not None:
                    self._connection_slot = f"slot-{slot.slot_id:04d}"
                    self._lakebase_slot = slot
                    # Work counts for the lease it was done under (note_progress).
                    self._slot_progressed = False
                    stop = threading.Event()
                    # Told which lease it was: a later read's is not this heartbeat's to end.
                    lost = functools.partial(self._poison_connection_slot_lease, slot, stop)
                    heartbeat = threading.Thread(
                        target=self._heartbeat_connection_slot,
                        args=(slot, stop, lost),
                        daemon=True,
                        name="informix-lakebase-slot-heartbeat",
                    )
                    self._connection_slot_heartbeat_stop = stop
                    self._connection_slot_heartbeat = heartbeat
                    heartbeat.start()
                    return
                if liveness:
                    # Poll the turnover token: if any slot was claimed, or a drain holding one
                    # did work, since the last sweep the pool is busy and will free capacity,
                    # so extend the window and tell the consumer's watchdog the drain is still
                    # live. If it is frozen, fall through to the deadline check -- the pool is
                    # genuinely wedged.
                    token = pool_progress_token(connection, namespace)
                    if token != progress_token:
                        progress_token = token
                        deadline = time.monotonic() + connection_wait_timeout
                        if self._slot_wait_liveness is not None:
                            self._slot_wait_liveness()
                if time.monotonic() >= deadline:
                    if liveness:
                        # 19: Timed out waiting for an Informix connection-capacity slot: the pool
                        # made no progress for {:g}s (no slot was claimed, and no snapshot drain
                        # holding one staged a page or read its table's changes), so capacity is
                        # wedged rather than merely busy. Raise connection.wait.timeout.seconds or
                        # max.concurrent.connections, or reduce concurrent daemon/consumer load.
                        raise ConnectionCapacityUnavailable(_m(19, connection_wait_timeout))
                    # 20: Timed out waiting for an Informix connection-capacity slot after {:g}
                    # seconds
                    raise ConnectionCapacityUnavailable(_m(20, connection_wait_timeout))
                time.sleep(random.uniform(sweep_min, sweep_max))
        finally:
            # Drop our ticket whether we won, timed out, or errored -- a lingering ticket
            # would wrongly hold back younger waiters until it aged out. Best-effort: a
            # dequeue failure must never mask a successful acquire or the real timeout.
            # A statement that failed left the transaction aborted, so the dequeue failed
            # too and the ticket held younger waiters back until it aged out: roll back.
            try:
                dequeue_waiter(_open_state_connection(connection), namespace, ticket_id)
            except Exception:  # noqa: BLE001 - ticket ages out via the reaper anyway
                # 21: snapshot slot waiter dequeue failed
                logging.getLogger(__name__).debug(_m(21), exc_info=True)

    def note_progress(self) -> None:
        # Docstring: Count work done holding this bridge's slot as the pool's progress, at
        # the lease's next renewal (see _POOL_PROGRESS_TOKEN): a reader waiting for a slot
        # behind a long snapshot drain then sees a busy pool, not a wedged one.

        if getattr(self, "_lakebase_slot", None) is not None:
            self._slot_progressed = True

    def _heartbeat_connection_slot(
        self,
        slot: Any,
        stop: threading.Event,
        on_lease_lost: Callable[[], None] | None = None,
    ) -> None:
        # Docstring: Renew a Postgres lease until released, poisoning it if renewal fails.
        #
        # Uses its own connection: the bridge's connection is busy serving reads,
        # and psycopg connections are not safe to share across threads.

        def report_lease_lost() -> None:
            # Mirrors the Volume heartbeat: a lease whose owner is already
            # releasing must not be poisoned, or the releaser (which sets ``stop``
            # then joins this thread) and the poisoner (which wants the owner's
            # operation lock) would wait on each other.
            if stop.is_set() or on_lease_lost is None:
                return
            on_lease_lost()

        connection: Any = None
        # The lease runs from its last renewal: Postgres frees the slot for another
        # reader that long after renewed_at. Timed from the first failed renewal instead,
        # at least a heartbeat later, the transport outlived the lease by up to a minute.
        # Started here, as the slot was just acquired.
        renewed = time.monotonic()
        try:
            while not stop.wait(_CONNECTION_SLOT_HEARTBEAT_SECONDS):
                try:
                    if connection is None or connection.closed:
                        # No longer than the lease has left, or a connect waiting out its
                        # own timeout outlived it.
                        left = _CONNECTION_SLOT_LEASE_SECONDS - (time.monotonic() - renewed)
                        connection = self._lakebase().connect(timeout=max(1.0, left))
                    # Taken before the renewal, so work done during it counts at the next.
                    progressed = getattr(self, "_slot_progressed", False)
                    self._slot_progressed = False
                    try:
                        held = (
                            heartbeat_slot(connection, slot, progressed=True)
                            if progressed
                            else heartbeat_slot(connection, slot)
                        )
                    except BaseException:
                        # Counted at the renewal that succeeds.
                        self._slot_progressed = self._slot_progressed or progressed
                        raise
                    if not held:
                        # The guard rejected us: the lease was reclaimed and now
                        # belongs to someone else. Nothing to retry.
                        report_lease_lost()
                        return
                    renewed = time.monotonic()
                except Exception:
                    now = time.monotonic()
                    try:
                        if connection is not None:
                            connection.close()
                    except Exception:
                        pass
                    connection = None
                    # A transient error must not drop a healthy lease, but once the next
                    # renewal would come after the lease has run out, another reader may
                    # reclaim the slot first, so the transport has to go now.
                    if now - renewed + _CONNECTION_SLOT_HEARTBEAT_SECONDS >= (
                        _CONNECTION_SLOT_LEASE_SECONDS
                    ):
                        # 22: Informix Lakebase slot not renewed for {:.0f}s, and its lease runs
                        # out before the next renewal; treating the lease as lost: {}
                        logging.getLogger(__name__).warning(
                            _m(22, now - renewed, slot),
                            exc_info=True,
                        )
                        report_lease_lost()
                        return
                    # 23: Informix Lakebase slot renewal failed; will retry
                    logging.getLogger(__name__).debug(_m(23), exc_info=True)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    def _release_connection_slot(self) -> None:
        # Docstring: Free a Postgres lease, tolerating a lease already reclaimed.

        slot = getattr(self, "_lakebase_slot", None)
        # Stop the heartbeat before releasing, and clear the handles first so a
        # re-entrant release cannot signal the same thread twice. Without this the
        # thread outlives the lease it was renewing: the slot is freed, another
        # reader claims it and bumps the epoch, and the stale heartbeat's next
        # renewal is rejected -- which it correctly reports as a lost lease,
        # poisoning a transport whose owner had already finished with it.
        stop = getattr(self, "_connection_slot_heartbeat_stop", None)
        heartbeat = getattr(self, "_connection_slot_heartbeat", None)
        self._connection_slot_heartbeat_stop = None
        self._connection_slot_heartbeat = None
        if stop is not None:
            stop.set()
        if heartbeat is not None and heartbeat is not threading.current_thread():
            # Returns as soon as the thread exits, so an ordinary release pays
            # nothing. The bound matters only if a renewal is mid-flight, and the
            # epoch guard makes a late renewal harmless anyway.
            heartbeat.join(timeout=_CONNECTION_SLOT_JOIN_SECONDS)
        if slot is None:
            return
        self._lakebase_slot = None
        # release_connection publishes a backlog hint only while a slot is held.
        self._connection_slot = None
        try:
            try:
                release_slot(self._lakebase_connection(), slot)
            except Exception:  # pylint: disable=broad-except
                # Reaped while the read ran (the lease heartbeat has its own connection), or
                # left in a failed transaction by the advisory backlog-hint write: release
                # over a fresh one.
                stale, self._lakebase_conn = getattr(self, "_lakebase_conn", None), None
                with contextlib.suppress(Exception):
                    stale.close()
                release_slot(self._lakebase_connection(), slot)
        except Exception:
            # Releasing is best effort: the lease expires on its own, so a failed
            # release costs one slot for one lease period rather than a stranded
            # slot as it did on the Volume.
            # 24: Informix could not release Lakebase connection slot {}; its lease will expire
            # instead
            logging.getLogger(__name__).warning(
                _m(24, slot),
                exc_info=True,
            )

    def _poison_connection_slot_lease(
        self, slot: Any = None, stop: threading.Event | None = None
    ) -> None:
        # Docstring: Abandon a lease that can no longer be renewed, closing the transport.
        #
        # Called from the heartbeat when renewal is rejected or fails persistently.
        # The reader has provably lost its capacity slot, so continuing to use the
        # SQLI connection would exceed the configured concurrency: another reader
        # already owns the slot. Nothing here is Volume-specific -- it is about the
        # Informix transport and the operation in flight on it. ``slot`` and ``stop`` are
        # the heartbeat's: a slot released meanwhile is left alone, and so is one claimed
        # since, which is another read's.

        def superseded() -> bool:
            held = getattr(self, "_lakebase_slot", None)
            return slot is not None and ((stop is not None and stop.is_set()) or held is not slot)

        if superseded():
            return
        lease_lost = getattr(self, "_connection_lease_lost", None)
        if lease_lost is not None:
            # Publish loss before waiting. The active operation can finish its
            # bounded socket work, but its wrapper must discard the result.
            lease_lost.set()
        operation_lock = getattr(self, "_operation_lock", None)
        context: Any = contextlib.nullcontext()
        # Whether an operation is still in flight when the transport is closed under it.
        in_flight = False
        if operation_lock is not None:
            # Bound the wait for the active operation. The lease is already
            # published as lost, so the operation's wrapper will discard its result
            # regardless; what remains is closing the transport and freeing the
            # slot. Waiting without a bound risks pairing with a releaser that is
            # joining this thread. The guard in the heartbeat covers the ordinary
            # release, and this covers the residue: a reader whose operation
            # outlives the lease still gets its transport closed and slot freed
            # here rather than leaving the lease poisoned indefinitely.
            # Taken with a time limit and handed to the stack below, which a with cannot do.
            # pylint: disable-next=consider-using-with
            if operation_lock.acquire(timeout=_POISON_OPERATION_LOCK_SECONDS):
                context = contextlib.ExitStack()
                context.callback(operation_lock.release)
            else:
                in_flight = True
                # 25: Informix connection-slot lease loss could not take the operation lock within
                # {:.0f}s; closing the transport while an operation is still in flight: {}
                logging.getLogger(__name__).warning(
                    _m(25, _POISON_OPERATION_LOCK_SECONDS, getattr(self, "_connection_slot", None)),
                )
        with context:
            with self._connect_lock:
                if superseded():
                    # Released while this waited for the operation lock: release_connection
                    # closed the transport and cleared the loss, unless it ran before the loss
                    # was set. A slot held now is a later read's, its transport too.
                    if lease_lost is not None and getattr(self, "_lakebase_slot", None) is None:
                        lease_lost.clear()
                    return
                # 26: Informix connection-slot lease renewal failed persistently; closing SQLI
                # transport after the active operation exits
                logging.getLogger(__name__).error(_m(26))
                self._connected = False
                if in_flight and lease_lost is not None:
                    # The operation still running must find the lease lost when it ends,
                    # or it would return a result read without a slot, or take the closed
                    # socket for a network drop and retry in place. Said before the
                    # close: the close is what wakes that operation, and it looks at once.
                    self._lease_loss_unseen = True
                try:
                    close = getattr(self.transport, "close", None)
                    if close is not None and in_flight:
                        # Under an operation still reading: the socket is shut down
                        # first, which is what wakes it (see InformixSqliClient.close).
                        try:
                            close(abrupt=True)
                        except TypeError:  # a transport whose close takes no such word
                            close()
                    elif close is not None:
                        close()
                except Exception:
                    # 27: Failed to close SQLI transport after connection-slot lease loss
                    logging.getLogger(__name__).warning(
                        _m(27),
                        exc_info=True,
                    )
                finally:
                    self._release_connection_slot()
                    # With an operation in flight the loss stands until one has seen it.
                    if lease_lost is not None and not in_flight:
                        lease_lost.clear()

    @classmethod
    def _remove_connection_slot_tree(cls, path: str) -> None:
        parent, name = os.path.split(path)
        parts = os.path.normpath(path).split(os.sep)
        if len(parts) >= 5 and parts[1] == "Volumes":
            root = os.path.join(os.sep, *parts[1:5])
            descriptor = _open_state_directory(root, parent)
        else:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(parent or ".", flags)
        try:
            info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                os.unlink(name, dir_fd=descriptor)
                return
            _remove_candidate_tree_at(descriptor, name)
            os.rmdir(name, dir_fd=descriptor)
        finally:
            os.close(descriptor)

    def release_connection(self) -> None:
        # Docstring: Release a finite worker poll's SQLI session without discarding metadata.

        operation_lock = getattr(self, "_operation_lock", None)
        context = operation_lock if operation_lock is not None else contextlib.nullcontext()
        with context:
            with self._connect_lock:
                # Both locks held: no operation runs and no lease shutdown is under way,
                # so a lost lease left standing has nobody left to see it. Taken down
                # here, or the next read's first operation would fail on it before it
                # could claim a slot.
                lease_lost = getattr(self, "_connection_lease_lost", None)
                if lease_lost is not None:
                    lease_lost.clear()
                self._lease_loss_unseen = False
                if not self._connected:
                    self._release_connection_slot()
                    return
                # Publish the backlog hint here: this is the last point at which
                # the connection is still live *and* the slot is still held, which
                # is exactly the position a waiter cannot reach. Advisory, so it
                # must never turn a completed read into a failed one.
                slot = getattr(self, "_connection_slot", None)
                if slot is not None:
                    self._publish_connection_backlog_hint()
                try:
                    close = getattr(self.transport, "close", None)
                    if close:
                        close()
                finally:
                    self._connected = False
                    self._release_connection_slot()

    def reset_transport(self) -> None:
        # Docstring: Drop a dead SQLI transport but keep the connection slot held.
        #
        # Used to recover from a mid-read connection drop: close the (already
        # broken) transport and mark the bridge disconnected so the next
        # ``_ensure_connected`` reconnects, *without* releasing and re-acquiring
        # the connection slot. Releasing the slot here would reintroduce capacity
        # contention on what is a transient network blip, and the caller is
        # retrying the same read in place under the slot it already owns.

        operation_lock = getattr(self, "_operation_lock", None)
        context = operation_lock if operation_lock is not None else contextlib.nullcontext()
        with context:
            with self._connect_lock:
                if not self._connected:
                    return
                try:
                    close = getattr(self.transport, "close", None)
                    if close:
                        close()
                except Exception:
                    # 28: Failed to close SQLI transport during reset; reconnecting anyway
                    logging.getLogger(__name__).warning(
                        _m(28),
                        exc_info=True,
                    )
                finally:
                    self._connected = False

    def _metadata_max_bytes(self) -> int | None:
        # Docstring: ``metadata.max.bytes`` as a catalog query's result bound; ``0`` is unbounded.

        return int(self.options.get("metadata.max.bytes", str(64 << 20))) or None

    @_serialized_sqli_operation
    def list_tables(self) -> list[dict[str, Any]]:
        self._ensure_connected()
        maximum = self._metadata_max_bytes()
        # Every base table, the system catalog's included. Informix's CDC API refuses only
        # views, temporary and non-logged tables and unlogged databases, so nothing else is
        # left out by name or owner; tabtype 'T' already leaves views and synonyms out.
        columns = self.transport.execute(
            _CATALOG_COLUMNS_SQL.format(", t.owner, t.tabname")
            + " ORDER BY t.owner, t.tabname, c.colno",
            max_result_bytes=maximum,
        )
        keys = self.transport.execute(
            _PRIMARY_KEY_SQL.format(",t.owner,t.tabname") + " ORDER BY t.owner,t.tabname",
            max_result_bytes=maximum,
        )
        columns_by_table: dict[tuple[str, str], list[Any]] = {}
        for row in columns:
            identity = (
                str(_field(row, "owner", 8)),
                str(_field(row, "tabname", 9)),
            )
            columns_by_table.setdefault(identity, []).append(row)
        keys_by_table = {
            (str(_field(row, "owner", 16)), str(_field(row, "tabname", 17))): [row] for row in keys
        }
        # A table without a primary key is keyed by a UNIQUE index here too, as
        # _describe_table keys it: the connector caches these tables, so a later read of
        # one must see the key a direct lookup would.
        unique_by_table: dict[tuple[str, str], list[Any]] = {}
        if _option_bool(self.options, _PROMOTE_UNIQUE_INDEX_OPTION, True) and any(
            table not in keys_by_table for table in columns_by_table
        ):
            for row in self.transport.execute(
                _UNIQUE_INDEX_SQL.format(",t.owner,t.tabname") + " ORDER BY t.owner,t.tabname",
                max_result_bytes=maximum,
            ):
                table = (str(_field(row, "owner", 17)), str(_field(row, "tabname", 18)))
                if table not in keys_by_table:
                    unique_by_table.setdefault(table, []).append(row)
        result = []
        retained_bytes = (
            _deep_size(result)
            + _deep_size(columns)
            + _deep_size(keys)
            + _deep_size(unique_by_table)
            if maximum
            else 0
        )
        for (owner, name), table_columns in columns_by_table.items():
            try:
                table = self._table_from_catalog_rows(
                    owner,
                    name,
                    table_columns,
                    keys_by_table.get((owner, name), []),
                    unique_by_table.get((owner, name), []),
                )
            except InformixError as error:
                # Not the whole list for one table (see _table_map).
                table = {"owner": owner, "name": name, "unreadable": str(error)}
            if maximum:
                retained_bytes += _deep_size(table)
            if maximum and retained_bytes > maximum:
                # 29: Informix metadata discovery exceeded metadata.max.bytes={}
                raise InformixError(_m(29, maximum))
            result.append(table)
        return result

    @_serialized_sqli_operation
    def get_table(self, identity: str) -> dict[str, Any]:
        self._ensure_connected()
        parts = _split_identity(identity)
        if len(parts) != 3:
            # 30: Invalid logical table identity {!r}
            raise InformixError(_m(30, identity))
        return self._describe_table(parts[1], parts[2])

    @_serialized_sqli_operation
    def table_versions(self, tabids: Sequence[int]) -> dict[int, tuple[str, str, int]]:
        # Docstring: ``systables.version`` of the tables with these IDs, as table ID ->
        # (owner, name, version), in one catalog query per _CATALOG_BATCH IDs.
        #
        # Informix bumps the version on every change to a table's definition -- a column
        # added, changed, dropped, or renamed, an index or constraint created or dropped, a
        # default changed, the table renamed -- and on UPDATE STATISTICS. An ID missing from
        # the result no longer names a table.

        self._ensure_connected()
        maximum = self._metadata_max_bytes()
        wanted = sorted({int(tabid) for tabid in tabids})
        versions: dict[int, tuple[str, str, int]] = {}
        for start in range(0, len(wanted), _CATALOG_BATCH):
            batch = wanted[start : start + _CATALOG_BATCH]
            rows = self.transport.execute(
                "SELECT tabid, owner, tabname, version FROM systables "
                f"WHERE tabtype = 'T' AND tabid IN ({', '.join('?' * len(batch))})",
                tuple(batch),
                max_result_bytes=maximum,
            )
            for row in rows:
                versions[int(_field(row, "tabid", 0))] = (
                    # The padding of a CHAR column only: a delimited name can begin
                    # with a space.
                    str(_field(row, "owner", 1)).rstrip(" "),
                    str(_field(row, "tabname", 2)).rstrip(" "),
                    int(_field(row, "version", 3)),
                )
        return versions

    @_serialized_sqli_operation
    def get_tables(self, identities: Sequence[str]) -> dict[str, dict[str, Any]]:
        # Docstring: Describe several tables as get_table does, as identity -> table, in
        # two or three catalog queries per _CATALOG_BATCH tables rather than per table; an
        # identity missing from the result names no table. See _describe_tables.

        self._ensure_connected()
        wanted: dict[tuple[str, str], str] = {}
        for identity in identities:
            parts = _split_identity(identity)
            if len(parts) != 3:
                # 30: Invalid logical table identity {!r}
                raise InformixError(_m(30, identity))
            wanted[(parts[1], parts[2])] = identity
        described = self._describe_tables(wanted)
        return {wanted[table]: raw for table, raw in described.items()}

    def _describe_tables(
        self, tables: Iterable[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        # Docstring: _describe_table for several (owner, name) tables at once, as
        # (owner, name) -> table; a table missing from the result doesn't exist.
        #
        # The queries select by table name and keep the owners asked for, so a same-named
        # table of another owner is read and dropped. The UNIQUE-index query runs only for
        # the tables without a primary key, as in _describe_table.

        maximum = self._metadata_max_bytes()
        wanted = set(tables)
        names = sorted({name for _, name in wanted})
        columns: dict[tuple[str, str], list[Any]] = {}
        keys: dict[tuple[str, str], list[Any]] = {}
        unique_indexes: dict[tuple[str, str], list[Any]] = {}

        def rows_by_table(sql: str, batch: list[str], owner_at: int, into: dict) -> None:
            for row in self.transport.execute(
                sql + f" AND t.tabname IN ({', '.join('?' * len(batch))})",
                tuple(batch),
                max_result_bytes=maximum,
            ):
                table = (
                    str(_field(row, "owner", owner_at)).rstrip(" "),
                    str(_field(row, "tabname", owner_at + 1)).rstrip(" "),
                )
                if table in wanted:
                    into.setdefault(table, []).append(row)

        for start in range(0, len(names), _CATALOG_BATCH):
            batch = names[start : start + _CATALOG_BATCH]
            rows_by_table(
                _CATALOG_COLUMNS_SQL.format(", t.owner, t.tabname"),
                batch,
                8,
                columns,
            )
            rows_by_table(
                _PRIMARY_KEY_SQL.format(",t.owner,t.tabname"),
                batch,
                16,
                keys,
            )
        keyless = sorted({table[1] for table in wanted if table in columns and table not in keys})
        if keyless and _option_bool(self.options, _PROMOTE_UNIQUE_INDEX_OPTION, True):
            for start in range(0, len(keyless), _CATALOG_BATCH):
                rows_by_table(
                    _UNIQUE_INDEX_SQL.format(",t.owner,t.tabname"),
                    keyless[start : start + _CATALOG_BATCH],
                    17,
                    unique_indexes,
                )
        described: dict[tuple[str, str], dict[str, Any]] = {}
        for table in wanted:
            if table not in columns:
                continue
            ordered = sorted(columns[table], key=lambda row: int(_field(row, "colno", 3)))
            try:
                described[table] = self._table_from_catalog_rows(
                    table[0],
                    table[1],
                    ordered,
                    keys.get(table, [])[:1],
                    unique_indexes.get(table, []),
                )
            except InformixError as error:
                # Its own, as list_tables marks one: a shard's batch failed with it, and every
                # table of the shard went to direct reads (see _ShardCatalog).
                described[table] = {"owner": table[0], "name": table[1], "unreadable": str(error)}
        return described

    @staticmethod
    def _capture_table(capture: dict[str, Any]) -> tuple[str, str, str]:
        # Docstring: A capture's native identity as (database, owner, name).

        native = str(capture["identity"])
        try:
            database, qualified = native.split(":", 1)
            owner, name = _split_identity(qualified)
        except ValueError as error:
            # 31: Invalid native table identity {!r}
            raise InformixError(_m(31, native)) from error
        return database, owner, name

    def _assert_capture_layout(
        self,
        capture: dict[str, Any],
        encoding: str,
        catalog: dict[tuple[str, str], dict[str, Any]],
    ) -> None:
        # Docstring: Fail before decoding rows when catalog metadata changed mid-session.
        #
        # ``catalog`` is the read's captured tables as _describe_tables read them, once,
        # when the first CDC metadata record arrived (see read_changes).

        native = str(capture["identity"])
        database, owner, name = self._capture_table(capture)
        described = catalog.get((owner, name))
        if described is None:
            # 32: Informix schema changed for {!r} during CDC; run a full refresh before decoding
            # additional records
            raise InformixError(_m(32, native))
        if described.get("unreadable"):
            raise InformixError(described["unreadable"])
        refreshed = _capture_descriptor(Table.parse(described, database), encoding)

        def layout(value: dict[str, Any]) -> dict[str, tuple[Any, ...]]:
            return {
                str(column["name"]): (
                    str(column["type_name"]),
                    int(column.get("length") or 0),
                    column.get("precision"),
                    column.get("scale"),
                    str(column.get("encoding") or "utf-8"),
                )
                for column in value["descriptors"]
            }

        # Every captured column must still exist with the layout it is decoded by. Matched
        # by name, not position: a column selection captures a subset of the catalog, and
        # the CDC metadata record already fixes the order rows arrive in.
        refreshed_layout = layout(refreshed)
        capture_layout = layout(capture)
        captured_unchanged = all(
            refreshed_layout.get(column) == capture_layout[column] for column in capture["columns"]
        )
        if not captured_unchanged:
            # 32: Informix schema changed for {!r} during CDC; run a full refresh before decoding
            # additional records
            raise InformixError(_m(32, native))

    def _describe_table(self, owner: str, name: str) -> dict[str, Any]:
        columns = self.transport.execute(
            _CATALOG_COLUMNS_SQL.format("") + " AND t.owner = ? AND t.tabname = ? ORDER BY c.colno",
            (owner, name),
            max_result_bytes=self._metadata_max_bytes(),
        )
        keys = self.transport.execute(
            _PRIMARY_KEY_SQL.format("") + " AND t.owner=? AND t.tabname=?",
            (owner, name),
            max_result_bytes=self._metadata_max_bytes(),
        )
        unique_indexes: list[Any] = []
        # No primary key: fall back to a UNIQUE index (see _select_unique_index_key). Only
        # queried when the table is physically key-less and the feature is enabled, so a
        # normal keyed table pays nothing extra. The index name is fetched so the selection
        # can break ties on it deterministically.
        if not keys and _option_bool(self.options, _PROMOTE_UNIQUE_INDEX_OPTION, True):
            unique_indexes = self.transport.execute(
                _UNIQUE_INDEX_SQL.format("") + " AND t.owner=? AND t.tabname=?",
                (owner, name),
                max_result_bytes=self._metadata_max_bytes(),
            )
        return self._table_from_catalog_rows(owner, name, columns, keys, unique_indexes)

    def _table_from_catalog_rows(
        self,
        owner: str,
        name: str,
        columns: list[Any],
        keys: list[Any],
        unique_indexes: list[Any] | None = None,
    ) -> dict[str, Any]:
        parsed_columns = [_catalog_column(row) for row in columns]
        for column in parsed_columns:
            if column["type_name"] in {"INT8", "SERIAL8"}:
                column["cdc_supported"] = True
            if column["type_name"] == "DATETIME":
                start, end = (column["length"] >> 8) & 0xF, column["length"] & 0xF
                column["cdc_supported"] = (
                    start in {0, 2, 4, 6, 8, 10}
                    and end in {0, 2, 4, 6, 8, 10, 11, 12, 13, 14, 15}
                    and end >= start
                )
        by_number = {
            int(_field(row, "colno", 3)): str(_field(row, "colname", 0)) for row in columns
        }
        primary_keys = []
        # Per-column index direction, parallel to primary_keys: True == the column
        # is stored DESC in the key index (a negative catalog part number). Kept so
        # a mixed-direction key can be paged in the index's own order instead of
        # forcing an all-ascending sort. Normalized to [] when every column is ASC
        # so the common case carries no extra state and behaves exactly as before.
        key_descending: list[bool] = []
        # A UNIQUE index with nullable column(s) that could stand in as the key only if the
        # per-table allow.nullable.index option is set. Reported as a catalog fact (columns
        # + directions); the connector promotes it, since that option is table-level.
        nullable_index_key: list[tuple[str, bool]] = []
        if keys:
            for position in range(16):
                column_number = int(_field(keys[0], f"part{position + 1}", position))
                if column_number != 0:
                    primary_keys.append(by_number[abs(column_number)])
                    key_descending.append(column_number < 0)
        elif unique_indexes:
            # No primary-key constraint: stand a UNIQUE index in for it (deterministically
            # chosen -- see _select_unique_index_key). The resulting key is catalog-derived,
            # so a schema refresh re-derives it; an explicit primary.keys option still
            # overrides it downstream in _apply_primary_key_override.
            nullable_by_name = {
                column["name"]: bool(column["nullable"]) for column in parsed_columns
            }
            index_parts = [
                (
                    str(_field(index, "idxname", 0)),
                    [
                        int(_field(index, f"part{position + 1}", position + 1))
                        for position in range(16)
                    ],
                )
                for index in unique_indexes
            ]
            selected = _select_unique_index_key(index_parts, by_number, nullable_by_name)
            primary_keys = [name for name, _ in selected]
            key_descending = [is_desc for _, is_desc in selected]
            if not primary_keys:
                # No all-NOT-NULL unique index qualified. Offer the nullable-relaxed
                # candidate for the connector to promote when allow.nullable.index is set.
                nullable_index_key = _select_unique_index_key(
                    index_parts, by_number, nullable_by_name, allow_nullable=True
                )
        try:
            tabid = int(_field(columns[0], "tabid", 4))
        except (IndexError, KeyError, TypeError, ValueError) as error:
            # 33: Informix catalog metadata for {}.{} is missing tabid
            raise InformixError(_m(33, owner, name)) from error
        if tabid <= 0:
            # 34: Informix catalog metadata for {}.{} has invalid tabid {}
            raise InformixError(_m(34, owner, name, tabid))
        return {
            "database": self.config["database"],
            "owner": owner,
            "name": name,
            "columns": parsed_columns,
            "primary_keys": primary_keys,
            # Only carry directions for a genuinely mixed/descending key; an
            # all-ascending key stays [] so nothing downstream changes for it.
            "key_descending": key_descending if any(key_descending) else [],
            # The nullable-relaxed unique-index candidate as [[name, descending], ...],
            # promotable only when the table sets allow.nullable.index.
            "nullable_index_key": [[name, is_desc] for name, is_desc in nullable_index_key],
            "incarnation": str(tabid),
        }

    @_serialized_sqli_operation
    def column_defaults(self, identity, columns):
        # Docstring: Return which of ``columns`` have a non-NULL default in the catalog.
        #
        # ``sysdefaults`` holds one row per column with an explicit default; type ``N`` is
        # ``DEFAULT NULL``, which leaves existing rows NULL like no default at all, so it is
        # not reported.

        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *columns):
            if not _IDENTIFIER.fullmatch(identifier):
                # 35: Unsafe column-default identifier {!r}
                raise InformixError(_m(35, identifier))
        rows = self.transport.execute(
            "SELECT c.colname, d.type FROM systables t "
            "JOIN syscolumns c ON t.tabid = c.tabid "
            "JOIN sysdefaults d ON d.tabid = c.tabid AND d.colno = c.colno "
            "WHERE t.tabtype = 'T' AND t.owner = ? AND t.tabname = ?",
            (owner, name),
            max_result_bytes=self._metadata_max_bytes(),
        )
        wanted = set(columns)
        return sorted(
            {
                str(_field(row, "colname", 0)).rstrip(" ")
                for row in rows
                if str(_field(row, "colname", 0)).rstrip(" ") in wanted
                and str(_field(row, "type", 1)).strip().upper() != "N"
            }
        )
