"""The pure-Python SQLI/CDC bridge to an Informix server, and its change reads."""

from __future__ import annotations

import logging
import sys
import threading
import time
from typing import Any, Callable

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    CdcFrameParser,
    CdcProtocolError,
    ColumnDescriptor,
    OpenTransactionRecords,
    cdc_routine,
    decode_frame,
    metadata_column_names,
)
from databricks.labs.community_connector.sources.informix.informix_bridge_reads import (
    _BridgeReadMixin,
)
from databricks.labs.community_connector.sources.informix.informix_bridge_session import (
    _BridgeSessionMixin,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _column_descriptor,
    _expect_zero,
    _field,
    _is_timeout_error,
    _load_factory,
    _options_set,
    _refuse_code_loading_options,
    _transport_closed,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _bridge_config,
    _null_byte_mode,
    _option_bool,
    _serialized_sqli_operation,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CDC_READ_GROW_SECONDS,
    _CDC_READ_SHRINK_SECONDS,
    _CDC_READ_START_BYTES,
    _CDC_RECORD_MIN_BYTES,
    _DATA_OPS,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _cdc_text_error,
    _stale_capture_label_error,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _redirect_allowlist,
)
from databricks.labs.community_connector.sources.informix.informix_unload import _deep_size
from databricks.labs.community_connector.sources.informix.sqli import InformixSqliClient
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    PasswordAuthenticationProvider,
    add_informix_exception_note,
    reconnectable_error,
)


class PurePythonInformixBridge(
    _BridgeSessionMixin,
    _BridgeReadMixin,
):
    # Docstring: Pure-Python SQLI/CDC bridge validated against disposable Informix 15.
    #
    # A test gives it a transport of its own by setting ``transport`` on an instance, or
    # replaces the bridge with set_bridge_factory: no connection option names code to load.

    def __init__(self, options: dict[str, str]) -> None:
        # As the connector reads them: a bridge built directly (the ANSI live test) read
        # "Encrypt" or "Bridge.Factory" as unset, and an empty "port" as a number.
        self.options = options = _options_set(options)
        authentication = options.get("authentication.mode", "password").lower()
        if authentication not in {"password", "pam"}:
            # 14: Authentication mode {!r} is unsupported; use password or pam.
            raise InformixError(_m(14, authentication))
        self.config = _bridge_config(options)
        # Resolved once and applied in both decode paths: the CDC decode via decode_frame
        # (see read_changes) and the snapshot/query decode inside the SQLI client.
        self._null_byte = _null_byte_mode(options)
        _refuse_code_loading_options(options)
        provider_path = options.get("authentication.provider.factory")
        if provider_path and authentication != "pam":
            # The factory answers PAM challenges only, which a password login never gets:
            # it was imported and run anyway, with every option, secrets included.
            logging.getLogger(__name__).warning(
                "Option 'authentication.provider.factory' is ignored: it is for "
                "authentication.mode pam"
            )
            provider_path = None
        provider = (
            _load_factory(provider_path)(options)
            if provider_path
            else PasswordAuthenticationProvider(
                self.config["password"], options.get("authentication.pam.echo.response")
            )
        )
        self.transport = InformixSqliClient(
            self.config["hostname"],
            self.config["port"],
            self.config["database"],
            self.config["user"],
            self.config["password"],
            server_name=self.config["server"],
            db_locale=self.config["db_locale"],
            client_locale=self.config["client_locale"],
            tls=self.config["tls"],
            ca_file=self.config["ca_file"],
            pad_varchar=self.config["pad_varchar"],
            null_byte=self._null_byte,
            authentication_mode=authentication,
            authentication_provider=provider,
            pam_max_rounds=int(options.get("authentication.pam.max.rounds", "16")),
            login_timeout=float(options.get("authentication.login.timeout", "30")),
            redirect_enabled=_option_bool(options, "redirect.enabled", False),
            redirect_allowlist=_redirect_allowlist(options.get("redirect.allowlist", "")),
            redirect_max=int(options.get("redirect.max", "3")),
        )
        self._connected = False
        self._connect_lock = threading.Lock()
        self._operation_lock = threading.RLock()
        self._connection_lease_lost = threading.Event()
        self._connection_slot: str | None = None
        self._connection_slot_heartbeat_stop: threading.Event | None = None
        self._connection_slot_heartbeat: threading.Thread | None = None
        # Optional callback the snapshot-drain reader installs so a worker blocked in
        # ``_acquire_connection_slot`` can report "still alive, pool is turning over" up to
        # the waiting consumer's liveness watchdog. None (the default) for every other read.
        self._slot_wait_liveness: Callable[[], None] | None = None
        # Set when the slot's holder did work since the lease's last renewal (note_progress).
        self._slot_progressed = False
        # Startup jitter is one-shot: set once the first connection has applied its delay so
        # steady-state polls and reconnects are never delayed.
        self._startup_jitter_applied = False

    @_serialized_sqli_operation
    # pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
    def read_changes(
        self,
        tables,
        start_lsn,
        timeout_seconds,
        max_records,
        budget=None,
        until=None,
        seconds=None,
    ):
        # ``max_records`` is Informix's cap on one read, and the records a poll stops at;
        # ``budget`` raises the latter, for a caller that wants a backlog in one session.
        # Such a caller names where the poll stops sooner: at a commit above ``until``, or
        # after ``seconds``. A table that changes steadily never lets the session go idle,
        # so without them the poll would run until the budget is read. Unlike the budget,
        # these stop it with transactions still open, as a table with writers that overlap
        # always has: the caller resumes from the oldest of them.
        budget = budget or max_records
        poll_started = time.monotonic()
        self._ensure_connected()
        set_socket_timeout = getattr(self.transport, "set_socket_timeout", None)
        previous_socket_timeout = getattr(self.transport, "socket_timeout", None)
        server_row = self.transport.execute(
            "SELECT env_value FROM sysmaster:sysenv WHERE env_name='INFORMIXSERVER'"
        )[0]
        server = str(_field(server_row, "env_value", 0))
        session_row = self.transport.execute(
            f"EXECUTE FUNCTION {cdc_routine('cdc_opensess')}(?, 0, ?, ?, 1, 1)",
            (server, timeout_seconds, max_records),
        )[0]
        session = int(_field(session_row, "session_id", 0))
        if session < 0:
            # 40: cdc_opensess failed with Informix error {}
            raise InformixError(_m(40, session))
        labels: dict[int, tuple[ColumnDescriptor, ...]] = {}
        captures = []
        # The socket timeout in force: the session's own for its calls, and for each
        # read what its size can take.
        socket_timeout = session_timeout = None
        try:
            if set_socket_timeout is not None:
                socket_timeout = session_timeout = max(
                    float(previous_socket_timeout or 30), 2 * timeout_seconds + 5.0
                )
                set_socket_timeout(socket_timeout)
            for label, capture in enumerate(tables, 1):
                native = capture["identity"]
                columns = tuple(capture["columns"])
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_set_fullrowlogging')}(?, 1)",
                        (native,),
                    ),
                    "cdc_set_fullrowlogging",
                )
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_startcapture')}(?, 0, ?, ?, ?)",
                        (session, native, ",".join(columns), label),
                    ),
                    "cdc_startcapture",
                )
                captures.append(native)
                labels[label] = tuple(_column_descriptor(c) for c in capture["descriptors"])
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_activatesess')}(?, ?)",
                    (session, start_lsn),
                ),
                "cdc_activatesess",
            )
            parser = CdcFrameParser(int(self.options.get("cdc.max.frame.bytes", str(16 << 20))))
            # Set by the connector for one poll (_read_changes_with_reconnect), else None.
            spill = getattr(self, "cdc_spill", None)
            records = []
            budget_records = 0
            halted = False
            open_transactions = OpenTransactionRecords()
            max_transaction_records = int(self.options.get("cdc.max.transaction.records", "100000"))
            max_poll_records = int(self.options.get("cdc.max.poll.records", "200000"))
            max_poll_bytes = int(self.options.get("cdc.max.poll.bytes", "0"))
            read_bytes = int(self.options.get("cdc.read.bytes", "32000"))
            requested = min(read_bytes, _CDC_READ_START_BYTES)
            empty_reads = 0
            timed_out = False
            retained_bytes = 0
            # What each open transaction's changes add to retained_bytes, which they leave
            # when it spills.
            held_bytes: dict[int, int] = {}
            truncated = False
            completed_transactions = 0
            metadata_labels: set[int] = set()
            # Every captured table's catalog entry, read in one batch when the first
            # metadata record arrives. Full-row logging is on for each of them by then, so
            # Informix rejects an ALTER TABLE, and the batch sees what each table's own
            # metadata record describes, as reading each table at its record did.
            capture_catalog: dict[tuple[str, str], dict[str, Any]] | None = None

            def _spill_open(tx_id: int) -> None:
                # Its changes, and any DISCARD among them, go to the spill in order; its
                # BEGIN stays here for the transaction buffer, with a SPILLING record per
                # table so that it still shows which tables it holds open. Informix reuses
                # transaction IDs, so only what follows its latest BEGIN is its own: an
                # earlier transaction with this ID stays as it was.
                nonlocal retained_bytes
                begin = max(
                    (
                        index
                        for index, r in enumerate(records)
                        if r.get("tx_id") == tx_id and r["op"] == "BEGIN"
                    ),
                    default=-1,
                )
                later = records[begin + 1 :]
                records[begin + 1 :] = [r for r in later if r.get("tx_id") != tx_id]
                own = [r for r in later if r.get("tx_id") == tx_id]
                begin_lsn = int((records[begin] if begin >= 0 else own[0])["lsn"])
                for table, live in spill.start(tx_id, begin_lsn, own).items():
                    records.append({"op": "SPILLING", "tx_id": tx_id, "lsn": live, "table": table})
                open_transactions.release(tx_id)
                retained_bytes -= held_bytes.pop(tx_id, 0)

            def _stop_at_poll_bound(bound: str) -> None:
                # The poll bounds cap what one poll may retain, not how much the
                # stream may contain. An interleaved long-running transaction
                # keeps this loop reading far past cdc.max.records, so under a
                # bulk load the bound is reached routinely rather than
                # exceptionally. End the poll and return the transactions that
                # already completed: they advance the checkpoint, and the next
                # poll resumes from the oldest still-open BEGIN.
                if not completed_transactions:
                    # Nothing completed, so there is no progress to checkpoint.
                    # Returning empty would leave the flow retrying the same
                    # bound forever, so surface the stall instead.
                    # 54: A CDC poll exceeded {} before any transaction completed
                    raise InformixError(_m(54, bound))
                # 55: Informix CDC poll reached {} after {} completed transactions; ending the poll
                # early and resuming from the oldest open transaction
                logging.getLogger(__name__).warning(
                    _m(55, bound, completed_transactions),
                )

            # cdc.max.records is a soft boundary: once crossed, finish every
            # transaction already observed so a transaction larger than the
            # native boundary can make progress. An idle open transaction is
            # still bounded by Informix's TIMEOUT control frame.
            while (
                (budget_records < budget or open_transactions)
                and not halted
                and empty_reads < 1
                and not timed_out
                and not truncated
            ):
                if socket_timeout is not None:
                    # As long as this read can take: cdc.timeout for each record it needs.
                    needed = min(max_records, requested // _CDC_RECORD_MIN_BYTES + 1)
                    wanted = max(
                        float(previous_socket_timeout or 30), timeout_seconds * needed + 5.0
                    )
                    if wanted != socket_timeout:
                        socket_timeout = wanted
                        set_socket_timeout(wanted)
                read_started = time.monotonic()
                chunk = self.transport.read_lodata(session, requested)
                read_seconds = time.monotonic() - read_started
                if len(chunk) >= requested and read_seconds < _CDC_READ_GROW_SECONDS:
                    requested = min(read_bytes, requested * 2)
                elif read_seconds > _CDC_READ_SHRINK_SECONDS:
                    # The backlog is read: what comes now comes at the table's own pace,
                    # and a read any larger than the start waits for it again.
                    requested = min(read_bytes, _CDC_READ_START_BYTES)
                if not chunk:
                    empty_reads += 1
                    continue
                empty_reads = 0
                for frame in parser.feed(chunk):
                    try:
                        record = decode_frame(
                            frame, labels, null_byte=getattr(self, "_null_byte", "keep")
                        )
                    except CdcProtocolError as error:
                        if isinstance(error.__cause__, UnicodeDecodeError):
                            raise _cdc_text_error(frame, error, start_lsn) from error
                        raise _stale_capture_label_error(frame, error, start_lsn) from error
                    if max_poll_bytes and not (
                        # A spilled transaction's changes are not retained.
                        spill is not None
                        and record["op"] in _DATA_OPS
                        and spill.holds(int(record.get("tx_id") or 0))
                    ):
                        size = len(frame) + _deep_size(record)
                        retained_bytes += size
                        if record["op"] in _DATA_OPS:
                            held = int(record.get("tx_id") or 0)
                            held_bytes[held] = held_bytes.get(held, 0) + size
                    if max_poll_bytes and retained_bytes > max_poll_bytes:
                        _stop_at_poll_bound(f"cdc.max.poll.bytes={max_poll_bytes}")
                        truncated = True
                        break
                    if record["op"] == "TIMEOUT":
                        # Informix represents an idle CDC timeout as a real,
                        # non-empty protocol frame. Treat it as the terminal
                        # condition for this finite poll instead of waiting for
                        # max_records timeout frames.
                        timed_out = True
                    label = record.get("label", record.get("capture_label"))
                    if record["op"] == "METADATA" and label in labels:
                        if label in metadata_labels:
                            # 56: Informix emitted a second CDC metadata layout for capture label
                            # {}; run a full refresh before continuing
                            raise InformixError(_m(56, label))
                        descriptors = {column.name: column for column in labels[label]}
                        encoding = next(iter(descriptors.values())).encoding
                        names = metadata_column_names(record["metadata"], encoding)
                        if set(names) != set(descriptors):
                            # 57: CDC metadata for capture label {} does not match the requested
                            # columns
                            raise InformixError(_m(57, label))
                        if capture_catalog is None:
                            capture_catalog = self._describe_tables(
                                {self._capture_table(capture)[1:] for capture in tables}
                            )
                        self._assert_capture_layout(tables[label - 1], encoding, capture_catalog)
                        labels[label] = tuple(descriptors[name] for name in names)
                        metadata_labels.add(label)
                    if label and 1 <= label <= len(tables):
                        record["table"] = tables[label - 1]["logical_identity"]
                    if len(records) >= max_poll_records:
                        _stop_at_poll_bound(f"cdc.max.poll.records={max_poll_records}")
                        truncated = True
                        break
                    # A transaction that outgrew cdc.spill.records keeps its changes on
                    # the staging Volume (see _CdcSpill), not in this poll's records.
                    tx_id = int(record.get("tx_id") or 0)
                    spilling = spill is not None and spill.holds(tx_id)
                    if spilling and record["op"] in _DATA_OPS:
                        if spill.add(record):
                            records.append(
                                {
                                    "op": "SPILLING",
                                    "tx_id": tx_id,
                                    "lsn": int(record["lsn"]),
                                    "table": record.get("table"),
                                }
                            )
                    else:
                        records.append(record)
                    if record["op"] not in {"METADATA", "TIMEOUT"}:
                        budget_records += 1
                    if record["op"] == "COMMIT":
                        completed_transactions += 1
                        if until is not None and int(record["lsn"]) > until:
                            halted = True
                    if record["op"] == "BEGIN":
                        open_transactions.begin(tx_id)
                        held_bytes.pop(tx_id, None)
                    elif record["op"] in _DATA_OPS and not spilling:
                        open_transactions.append(tx_id, int(record["lsn"]))
                        if spill is not None and open_transactions.held(tx_id) > spill.threshold:
                            _spill_open(tx_id)
                    elif record["op"] == "DISCARD":
                        open_transactions.discard(tx_id, int(record["lsn"]))
                        if spilling:
                            spill.discard(tx_id, int(record["lsn"]))
                    elif record["op"] in {"COMMIT", "ROLLBACK"}:
                        open_transactions.finish(tx_id)
                        held_bytes.pop(tx_id, None)
                        if spilling and record["op"] == "ROLLBACK":
                            spill.drop(tx_id)
                        elif spilling:
                            # Ahead of the COMMIT, so the transaction carries its pages.
                            records[-1:-1] = spill.finish(tx_id, int(record["lsn"]))
                    while (
                        spill is not None
                        and open_transactions.buffered > max_transaction_records
                        and open_transactions.held(open_transactions.largest())
                    ):
                        # Interleaved transactions, none past cdc.spill.records alone, hold
                        # more than the bound between them: spill the largest.
                        _spill_open(open_transactions.largest())
                    if open_transactions.buffered > max_transaction_records:
                        # 58: An open CDC transaction exceeded cdc.max.transaction.records={}
                        raise InformixError(_m(58, max_transaction_records))
                    if timed_out:
                        # Ignore anything following the terminal timeout in this
                        # session. The checkpoint remains at the last complete
                        # transaction, so a later poll safely replays it.
                        break
                    if halted:
                        # As far as the caller asked: what follows is the next poll's.
                        break
                halted = halted or (
                    seconds is not None and time.monotonic() - poll_started > seconds
                )
            # A poll that reached cdc.max.records with every transaction complete stops on
            # purpose, so its last cdc.read.bytes read can end inside the next record. Like a
            # truncated poll, it leaves that partial frame for the next poll, which
            # replays from the last complete transaction.
            stopped_at_budget = budget_records >= budget and not open_transactions
            if parser.buffered_bytes and not (
                timed_out or truncated or stopped_at_budget or halted
            ):
                # 59: CDC read ended with an incomplete native frame
                raise InformixError(_m(59))
            return records
        finally:
            cleanup_errors = []
            if set_socket_timeout is not None and socket_timeout != session_timeout:
                # A large read's timeout is not for the calls that end the session: they
                # answer at once or have stalled.
                try:
                    set_socket_timeout(session_timeout)
                except Exception as error:
                    cleanup_errors.append(error)
            lease_lost = getattr(self, "_connection_lease_lost", None)
            if lease_lost is None or not lease_lost.is_set():
                for native in captures:
                    if _transport_closed(self.transport):
                        # A failed call closed the connection, and the CDC session with
                        # it: the calls left could fail only for want of a connection.
                        break
                    try:
                        _expect_zero(
                            self.transport.execute(
                                f"EXECUTE FUNCTION {cdc_routine('cdc_endcapture')}(?, 0, ?)",
                                (session, native),
                            ),
                            "cdc_endcapture",
                        )
                    except Exception as error:  # preserve an active CDC failure
                        cleanup_errors.append(error)
                if not _transport_closed(self.transport):
                    try:
                        _expect_zero(
                            self.transport.execute(
                                f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)",
                                (session,),
                            ),
                            "cdc_closesess",
                        )
                    except Exception as error:  # preserve an active CDC failure
                        cleanup_errors.append(error)
            # Put the timeout back after a lost lease too: the client keeps it for every
            # later connection, so a skipped restore made it the new default.
            if set_socket_timeout is not None and previous_socket_timeout is not None:
                try:
                    set_socket_timeout(float(previous_socket_timeout))
                except Exception as error:
                    cleanup_errors.append(error)
            if cleanup_errors:
                self._settle_poll_cleanup(cleanup_errors, sys.exc_info()[1])

    def _settle_poll_cleanup(
        self, cleanup_errors: list[BaseException], active_error: BaseException | None
    ) -> None:
        # Docstring: What a CDC poll's failed session teardown comes to: a note on the
        # poll's own failure, or after a whole read a failure of its own (60) -- unless the
        # connection dropped or stalled, which no reconnect loop retried (the 60 failed the
        # flow). The session ends with the connection: it is dropped, keeping the slot, so
        # the next poll reconnects, as a validation's teardown does (41), and the poll
        # returns what it read.

        if active_error is not None:
            for error in cleanup_errors:
                add_informix_exception_note(
                    active_error, f"Informix CDC cleanup also failed: {error}"
                )
            return
        if not all(_is_timeout_error(e) or reconnectable_error(e) for e in cleanup_errors):
            # 60: Informix CDC session cleanup failed
            raise InformixError(_m(60)) from cleanup_errors[0]
        self.reset_transport()
        # 483: Informix CDC poll read its changes, but ending its session failed on a
        # dropped or stalled connection; resetting the transport, which ends the session
        logging.getLogger(__name__).warning(_m(483), exc_info=cleanup_errors[0])
