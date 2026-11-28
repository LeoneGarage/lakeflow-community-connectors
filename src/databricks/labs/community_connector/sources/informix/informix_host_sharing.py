"""Sharing CDC shards between the flow processes of a host, and the snapshot drain pool."""

from __future__ import annotations

import contextlib
import functools
import importlib
import logging
import math
import os
import queue
import secrets
import threading
import time
from collections import deque
from dataclasses import replace
from typing import Any, Callable, Iterator, Sequence

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    ColumnDescriptor,
    interval_layout,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _datetime_qualifier_supported,
    _float32,
    _snapshot_unsupported_columns,
    _unsupported_column_list,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _VARIABLE_DECIMAL_DEFAULT,
    _is_variable_scale_decimal,
    _parse_variable_decimal_type,
    _snapshot_scan_columns,
    _variable_decimal_column_overrides,
    _variable_decimal_target,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _DEFAULT_SNAPSHOT_READER_THREADS,
    _SNAPSHOT_DRAIN_IDLE_EXIT_SECONDS,
    _SNAPSHOT_DRAIN_IDLE_SECONDS,
    _SNAPSHOT_SCAN_OPTION,
    _UNL_DEFAULT_DELIMITER,
    _UNL_DEFAULT_ESCAPE,
    _UNL_DELIMITER_OPTION,
    _UNL_ESCAPE_OPTION,
    _UNL_NO_ESCAPE,
    _UNLOAD_FLOAT_TYPES,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    Table,
    _optional_int,
)
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _HOST_SHARD_ACCEPT_FAILURES,
    _HOST_SHARD_ENDPOINTS,
    _HOST_SHARD_REPLY_SECONDS,
    _SHARED_CDC_SHARDS,
    _SHARED_CDC_THREADS,
    _host_shard_own_poll,
    _host_shard_stop_leading,
    _HostShardEndpoints,
    _shared_cdc_registry_lock,
    _shared_cdc_shard,
    _unwire_table,
    _unwire_view,
    _wire_table,
    _wire_view,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    CommittedTransaction,
    TransactionBuffer,
    _lsn,
    _operation,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import informix_locale_encoding


def _host_shard_socket_timeouts(sock) -> None:
    # Docstring: Bound each blocking send and receive on ``sock`` by the reply timeout.
    #
    # Kernel timeouts rather than ``socket.settimeout``, which would make the descriptor
    # non-blocking, which Connection's reads don't expect.

    import socket
    import struct

    seconds = _HOST_SHARD_REPLY_SECONDS
    timeval = struct.pack("ll", int(seconds), int(seconds % 1 * 1_000_000))
    sock.setblocking(True)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, timeval)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeval)


def _host_shard_serve(
    endpoints: _HostShardEndpoints, key: tuple, connection, authkey: bytes
) -> None:
    # Docstring: Authenticate one follower, then answer its polls until it disconnects.
    #
    # The handshake runs here, not in the accept thread, so a follower that stalls in
    # it holds up only itself. The handshake and each message are bounded like the
    # follower's; the wait for its next poll is not, as a follower may poll rarely.

    import multiprocessing.connection
    import socket

    try:
        try:
            # A duplicate descriptor shares the socket, and so its options.
            with socket.fromfd(connection.fileno(), socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                _host_shard_socket_timeouts(sock)
            multiprocessing.connection.deliver_challenge(connection, authkey)
            multiprocessing.connection.answer_challenge(connection, authkey)
        except Exception:  # noqa: BLE001 - a failed or broken handshake
            return
        while True:
            try:
                connection.poll(None)
                request = connection.recv()
            except (EOFError, OSError):
                return
            leading = endpoints.leading.get(key)
            if leading is None:
                # No longer this process's shard: hang up, so the follower takes over.
                return
            try:
                view = _host_shard_own_poll(
                    endpoints,
                    key,
                    leading["reader_factory"],
                    request["config"],
                    request["identity"],
                    _unwire_table(request["table"]),
                    request["capture"],
                    request["commit"],
                    request["floor"],
                    request.get("owner"),
                )
                after = request.get("after")
                if view is not None and after is not None:
                    # The follower already holds every transaction up to ``after``.
                    view = replace(
                        view, committed=[tx for tx in view.committed if tx.commit_lsn > after]
                    )
                reply = ("ok", None if view is None else _wire_view(view))
            except Exception as error:  # noqa: BLE001 - the follower reads directly instead
                reply = ("error", f"{type(error).__name__}: {error}"[:500])
            try:
                try:
                    connection.send(reply)
                except (OSError, ValueError):
                    raise
                except Exception as error:  # noqa: BLE001 - e.g. a value that doesn't pickle
                    connection.send(("error", f"{type(error).__name__}: {error}"[:500]))
            except (OSError, ValueError):
                return
    finally:
        with contextlib.suppress(OSError):
            connection.close()


def _host_shard_accept(
    endpoints: _HostShardEndpoints, key: tuple, listener, authkey: bytes
) -> None:
    # Docstring: Accept followers while this process leads the shard.
    #
    # Accept failing again and again (out of descriptors, say) gives the shard up:
    # the socket goes while the lock is still held, so it can't remove a successor's,
    # then the lock, so another process can lead.

    failures = 0
    while True:
        try:
            connection = listener.accept()
        except Exception as error:  # noqa: BLE001 - retried, then the shard is given up
            if key not in endpoints.leading:
                return
            failures += 1
            # 144: Host-shared CDC shard {}-{} could not accept a follower ({})
            logging.getLogger(__name__).warning(
                _m(144, key[-2], key[-1], error),
            )
            if failures < _HOST_SHARD_ACCEPT_FAILURES:
                time.sleep(min(5.0, 0.05 * 2**failures))
                continue
            # The shard goes with the lead, as an idle one retires: no poll of this
            # process reads a shard it does not lead (_host_shard_own_poll), its own
            # included. Kept, it read on for its own subscriber, a second session beside
            # the successor's, until the TTL; its socket and key files outlived the lock.
            with endpoints.lock, _shared_cdc_registry_lock():
                shard = None
                if key in endpoints.leading:
                    shard = _SHARED_CDC_SHARDS.pop(key, None)
                    _SHARED_CDC_THREADS.pop(key, None)
                    _host_shard_stop_leading(endpoints, key, locked=True)
                    # 145: Process {} gave up host-shared CDC shard {}-{}
                    logging.getLogger(__name__).warning(_m(145, os.getpid(), key[-2], key[-1]))
            if shard is not None:
                # Its thread then finds no subscriber and lets its session go. Each
                # subscriber takes up from its checkpoint, as after a restart.
                with shard.condition:
                    identities = list(shard.subscribers)
                for identity in identities:
                    shard.evict(identity)
            return
        failures = 0
        if key not in endpoints.leading:
            connection.close()
            return
        threading.Thread(
            target=_host_shard_serve,
            args=(endpoints, key, connection, authkey),
            daemon=True,
            name=f"informix-host-shard-serve-{key[-2]}-{key[-1]}",
        ).start()


def _host_shard_try_lead(
    endpoints: _HostShardEndpoints, key: tuple, reader_factory, config: dict[str, Any]
) -> bool:
    # Docstring: Lead the shard if no other process holds its lock; True when this one does.
    #
    # The auth key is written before the listener exists, and only a process holding the
    # lock replaces the socket, so a stale socket file of a dead leader is never live.
    # An error other than the lock being taken -- an unusable directory included --
    # marks host sharing unavailable here, so this process shares within itself.

    with endpoints.lock:
        if key in endpoints.leading:
            return True
        if endpoints.unavailable:
            return False
        descriptor = None
        try:
            # POSIX's alone (flock, AF_UNIX, a user id, O_NOFOLLOW): elsewhere host sharing is
            # off, as for an unusable directory, where an ImportError failed the read.
            import fcntl
            import multiprocessing.connection

            lock_path, socket_path, key_path = endpoints.paths(key)
            descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(descriptor)
                return False
            authkey = secrets.token_bytes(32)
            staged = f"{key_path}.{secrets.token_hex(8)}"
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW
            with open(os.open(staged, flags, 0o600), "wb") as out:
                out.write(authkey)
            os.replace(staged, key_path)
            if os.path.lexists(socket_path):
                os.unlink(socket_path)
            # Followers connect all at once when an update starts: queue them.
            listener = multiprocessing.connection.Listener(
                socket_path, family="AF_UNIX", backlog=128
            )
        except (OSError, ImportError, AttributeError) as error:
            if descriptor is not None:
                os.close(descriptor)
            endpoints.unavailable = f"{type(error).__name__}: {error}"
            # 146: Host-shared CDC shards are unavailable in process {} ({}); sharing within the
            # process instead
            logging.getLogger(__name__).warning(
                _m(146, os.getpid(), endpoints.unavailable),
            )
            return False
        endpoints.leading[key] = {
            "lock_fd": descriptor,
            "listener": listener,
            "reader_factory": reader_factory,
        }
    _shared_cdc_shard(key, reader_factory=reader_factory, **config)
    threading.Thread(
        target=_host_shard_accept,
        args=(endpoints, key, listener, authkey),
        daemon=True,
        name=f"informix-host-shard-{key[-2]}-{key[-1]}",
    ).start()
    # 147: Process {} leads host-shared CDC shard {}-{}
    logging.getLogger(__name__).info(_m(147, os.getpid(), key[-2], key[-1]))
    return True


def _host_shard_request(endpoints: _HostShardEndpoints, key: tuple, request: dict[str, Any]):
    # Docstring: Send one poll to the shard's leader over this process's connection to it.
    #
    # Connecting, the handshake and every send and receive time out, so a leader that
    # holds its lock but stops serving can't hang the poll.

    import multiprocessing.connection
    import socket

    with endpoints.lock:
        guard = endpoints.client_locks.setdefault(key, threading.Lock())
    with guard:
        connection = endpoints.clients.get(key)
        if connection is None:
            _lock_path, socket_path, key_path = endpoints.paths(key)
            with open(key_path, "rb") as handle:
                authkey = handle.read()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                _host_shard_socket_timeouts(sock)
                sock.connect(socket_path)
                connection = multiprocessing.connection.Connection(sock.detach())
            try:
                multiprocessing.connection.answer_challenge(connection, authkey)
                multiprocessing.connection.deliver_challenge(connection, authkey)
            except BaseException:
                connection.close()
                raise
            endpoints.clients[key] = connection
        try:
            connection.send(request)
            if not connection.poll(_HOST_SHARD_REPLY_SECONDS):
                # 148: the host shard leader did not answer the poll
                raise TimeoutError(_m(148))
            return connection.recv()
        except BaseException:
            endpoints.clients.pop(key, None)
            try:
                connection.close()
            except OSError:
                pass
            raise


def _host_shard_poll(
    key,
    reader_factory,
    config,
    identity,
    table,
    capture,
    commit,
    floor,
    endpoints: _HostShardEndpoints | None = None,
    owner: str | None = None,
):
    # Docstring: A host-shared shard's view for one consumer poll, or None to read directly.
    #
    # Leads the shard when nobody does, polls the leader otherwise, and takes over when
    # the leader is gone. Where the host can't share at all, shares within the process.
    # ``owner`` names the flow polling (see _SharedCdcShard.subscribe).

    endpoints = _HOST_SHARD_ENDPOINTS if endpoints is None else endpoints
    local = (key, reader_factory, config, identity, table, capture, commit, floor, owner)
    # The leader sends only transactions after those this follower holds from earlier
    # polls, so each crosses the socket once. A checkpoint moved back past them, a floor
    # moved back (an older open transaction the view they came from lacked), a new capture
    # (they were decoded under the old one), or a poll without a view, starts over with
    # the whole buffer; leading drops them.
    held_commit, held_floor, held_capture, held = endpoints.mirrors.pop(
        (key, identity, owner), (commit, floor, capture, ())
    )
    if key in endpoints.leading or endpoints.unavailable:
        return _host_shard_own_poll(endpoints, *local)
    if (
        key not in endpoints.clients
        and _host_shard_try_lead(endpoints, key, reader_factory, config)
    ) or endpoints.unavailable:
        return _host_shard_own_poll(endpoints, *local)
    if held_commit > commit or held_floor > floor or held_capture != capture:
        held = ()
    request = {
        "config": config,
        "identity": identity,
        "table": _wire_table(table),
        "capture": capture,
        "commit": commit,
        "floor": floor,
        "after": held[-1].commit_lsn if held else None,
        "owner": owner,
    }
    try:
        status, payload = _host_shard_request(endpoints, key, request)
    except (OSError, EOFError, TimeoutError, ValueError) as error:
        if _host_shard_try_lead(endpoints, key, reader_factory, config) or endpoints.unavailable:
            return _host_shard_own_poll(endpoints, *local)
        # 149: Host-shared CDC shard {}-{} leader unreachable ({}); reading directly
        logging.getLogger(__name__).debug(
            _m(149, key[-2], key[-1], error),
        )
        return None
    except Exception as error:  # noqa: BLE001 - e.g. a failed auth handshake
        # 150: Host-shared CDC shard {}-{} poll failed ({}); reading directly
        logging.getLogger(__name__).debug(
            _m(150, key[-2], key[-1], error),
        )
        return None
    if status != "ok":
        # 151: Host-shared CDC shard {}-{} leader refused a poll ({}); reading directly
        logging.getLogger(__name__).warning(
            _m(151, key[-2], key[-1], payload),
        )
        return None
    if payload is None:
        return None
    view = _unwire_view(payload)
    committed = [tx for tx in held if tx.commit_lsn > commit] + view.committed
    endpoints.mirrors[(key, identity, owner)] = (commit, floor, capture, tuple(committed))
    return replace(view, committed=committed)


class _SnapshotDrainPool:
    # Docstring: Bounded pool that drains ``initial``-mode snapshots off the microbatch thread.
    #
    # A fixed number of driver-resident daemon workers (``snapshot.reader.threads``) pull
    # drain jobs off a shared queue; each builds a private reader, runs the ordinary inline
    # snapshot drain (which stages the pages and publishes the manifest durably), then
    # closes the reader to release its connection slot. Because at most K workers drain at
    # once, at most K slots are ever held by snapshots -- the fairness guarantee that keeps
    # streaming readers from being starved. A consumer subscribes a job and waits, holding
    # no slot, for its result. All state is guarded by ``condition``.
    #
    # The consumer's ``wait`` is a *liveness watchdog*, not a total-duration deadline: the
    # drain reports progress (via ``report_progress``) each time it stages a page, and the
    # wait trips only after progress has gone quiet for a whole idle window. Liveness is
    # scoped by state so a job is neither failed early nor kept alive on someone else's work:
    #
    # - A **running** job (a worker has picked it up) is measured from *its own* progress --
    #   a staged page, or, while its worker is still acquiring a connection slot, a turnover
    #   ping from the slot wait -- so a hung fetch on its worker trips even while other
    #   workers advance, but a worker legitimately queued behind a busy pool does not.
    # - A **queued** job (waiting for a free worker) is measured from *pool-wide* progress,
    #   so it waits its turn behind a large, healthy drain instead of failing while queued,
    #   and only trips when the whole pool goes quiet.
    #
    # Because progress is tied to *actual staged pages* (plus a pickup), a hung fetch cannot
    # masquerade as liveness. At most K workers drain at once, so at most K slots are ever
    # held by snapshots -- the fairness guarantee that keeps streaming readers from being
    # starved. A consumer subscribes a job and waits, holding no slot, for its result.

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.queue: deque = deque()
        # Job keys queued or running: dedups concurrent subscribers so one table drains
        # exactly once even when several flows (or retries) ask for it at the same time.
        self.pending: set = set()
        self.jobs: dict[tuple, dict[str, Any]] = {}
        # job key -> None on success, or the Exception to re-raise to the waiter.
        self.results: dict[tuple, Any] = {}
        # job key -> monotonic timestamp of the last forward progress (enqueue, worker
        # pickup, or a staged page). A *running* job's waiter is measured from this, so a
        # hung fetch on its own worker is caught even while other workers progress.
        self.progress: dict[tuple, float] = {}
        # Job keys a worker has picked up and is actively draining (vs. still queued).
        self.running: set = set()
        # Pool-wide last-progress clock, bumped by any worker's pickup or staged page. A
        # *queued* job's waiter is measured from max(its own, this), so it waits its turn
        # as long as the pool as a whole is draining something -- and only times out when
        # the whole pool goes quiet, not merely because a big drain is ahead of it.
        self.last_progress_any: float = 0.0
        self.reader_factory: Callable[[], Any] | None = None
        self.thread_count = _DEFAULT_SNAPSHOT_READER_THREADS

    def configure(self, *, reader_factory, thread_count):
        with self.condition:
            if self.reader_factory is None:
                self.reader_factory = reader_factory
            self.thread_count = thread_count

    def submit(
        self,
        job_key,
        *,
        table,
        options,
        pipeline_scope,
        allow_keyless,
        registration_scope=None,
        reader_factory=None,
    ):
        # ``pipeline_scope`` is where the snapshot is staged. ``registration_scope`` is the
        # update the consumer runs in, when that is another: a scan taken up from an
        # earlier update is staged under that update's scope by a worker of this one.
        with self.condition:
            # Drop a prior terminal result so a re-subscribe after a failed drain (or a
            # process restart that left no manifest) enqueues a fresh attempt.
            self.results.pop(job_key, None)
            if job_key not in self.pending:
                self.jobs[job_key] = {
                    "table": table,
                    "options": options,
                    "pipeline_scope": pipeline_scope,
                    "registration_scope": registration_scope or pipeline_scope,
                    "allow_keyless": allow_keyless,
                    # The submitting flow's own: its options (a flow's carry its table's
                    # configuration) are the worker's fallback for the job's.
                    "reader_factory": reader_factory,
                }
                self.queue.append(job_key)
                self.pending.add(job_key)
                # Start the liveness clock at enqueue; a worker pickup and each staged
                # page reset it, so a queued-but-not-yet-started job is not charged for
                # time it spent waiting behind other drains.
                self.progress[job_key] = time.monotonic()
                self.condition.notify_all()

    def mark_running(self, job_key) -> None:
        # Docstring: Record that a worker has picked this job up (it is no longer merely queued).

        with self.condition:
            self.running.add(job_key)
            now = time.monotonic()
            if job_key in self.pending:
                self.progress[job_key] = now
            # Picking a job up is pool progress: it proves a worker just freed and took
            # the next job, so every queued waiter's idle window resets.
            self.last_progress_any = now
            self.condition.notify_all()

    def report_progress(self, job_key) -> None:
        # Docstring: Record forward progress on a job so waiters' idle windows reset.
        #
        # A staged page advances both this job's own clock (for its running waiter) and the
        # pool-wide clock (for every queued waiter), so a queued job stays alive as long as
        # any worker is draining.

        with self.condition:
            now = time.monotonic()
            if job_key in self.pending:
                self.progress[job_key] = now
            self.last_progress_any = now
            self.condition.notify_all()

    def wait(self, job_key, idle_timeout):
        # Docstring: Block until the job finishes, fails, or goes quiet for ``idle_timeout``.
        #
        # Returns True on success, re-raises the worker's exception on failure, and returns
        # False only when the drain is *stalled*. "Stalled" means different things by state:
        # a **running** job must make its own progress (a hung fetch on its worker times
        # out even while other workers advance); a **queued** job only needs the pool to be
        # making progress (it waits its turn behind a big, healthy drain rather than failing
        # while it is still queued). Either way it fails once the whole pool goes quiet.

        with self.condition:
            while job_key in self.pending:
                own = self.progress.get(job_key, time.monotonic())
                if job_key in self.running:
                    last = own
                else:
                    last = max(own, self.last_progress_any)
                remaining = (last + idle_timeout) - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(timeout=min(remaining, _SNAPSHOT_DRAIN_IDLE_SECONDS))
            result = self.results.get(job_key)
        if isinstance(result, BaseException):
            raise result
        return True


# Serialization-safe registry (a bare module global becomes a function local in the
# merged deployment; a mutated dict survives, as with the shared-CDC registry).
_SNAPSHOT_DRAIN_POOLS: dict[tuple, _SnapshotDrainPool] = {}
_SNAPSHOT_DRAIN_THREADS: dict[tuple, list] = {}
_SNAPSHOT_DRAIN_REGISTRY_LOCK: dict[str, threading.Lock] = {}


def _snapshot_drain_registry_lock() -> threading.Lock:
    lock = _SNAPSHOT_DRAIN_REGISTRY_LOCK.get("lock")
    if lock is None:
        lock = _SNAPSHOT_DRAIN_REGISTRY_LOCK.setdefault("lock", threading.Lock())
    return lock


def _close_snapshot_drain_reader(reader) -> None:
    if reader is None:
        return
    try:
        reader.close()
    except Exception:  # noqa: BLE001 - best-effort slot release
        # 156: snapshot drain reader close failed
        logging.getLogger(__name__).debug(_m(156), exc_info=True)


def _retire_snapshot_drain_worker(key: tuple, pool: _SnapshotDrainPool) -> bool:
    # Docstring: Take this idle worker out of its pool's registry, and the pool with its
    # last one; False when a job has arrived. A consumer that asks for the pool
    # afterwards gets its workers started again.

    with _snapshot_drain_registry_lock():
        with pool.condition:
            if pool.queue or pool.pending:
                return False
        threads = _SNAPSHOT_DRAIN_THREADS.get(key)
        if threads is not None:
            threads[:] = [thread for thread in threads if thread is not threading.current_thread()]
            if not threads:
                _SNAPSHOT_DRAIN_THREADS.pop(key, None)
                if _SNAPSHOT_DRAIN_POOLS.get(key) is pool:
                    _SNAPSHOT_DRAIN_POOLS.pop(key, None)
    return True


def _drain_progressed(pool: _SnapshotDrainPool, job_key: tuple, reader: Any) -> None:
    # Docstring: A drain did work: its consumer's watchdog is reset, and its slot counts the
    # work as the pool's progress (note_progress), so readers waiting behind a long drain
    # for a slot see a busy pool, not a wedged one. Before the drain holds a slot it is
    # only the first.

    pool.report_progress(job_key)
    note = getattr(getattr(reader, "_bridge_instance", None), "note_progress", None)
    if callable(note):
        note()


def _run_snapshot_drain_worker(pool: _SnapshotDrainPool, key: tuple | None = None) -> None:
    # Docstring: Daemon loop: pull one drain job, run the inline snapshot drain under a private
    # reader (staging pages and publishing the manifest durably), then close the reader to
    # release its slot. A failure is recorded on the job so the waiting consumer re-raises
    # and its next microbatch re-subscribes, recovering by restart: with no manifest, the
    # table drains again.

    logger = logging.getLogger(__name__)
    while True:
        idle_since = time.monotonic()
        while True:
            with pool.condition:
                if not pool.queue:
                    pool.condition.wait(timeout=_SNAPSHOT_DRAIN_IDLE_SECONDS)
                if pool.queue:
                    break
            # No job for a long while: the worker ends, unless one arrives just now.
            if (
                key is not None
                and time.monotonic() - idle_since >= _SNAPSHOT_DRAIN_IDLE_EXIT_SECONDS
                and _retire_snapshot_drain_worker(key, pool)
            ):
                return
        with pool.condition:
            if not pool.queue:
                continue
            job_key = pool.queue.popleft()
            job = pool.jobs.get(job_key)
            factory = (job or {}).get("reader_factory") or pool.reader_factory
        # Mark the job running (so its waiter switches from pool-wide to own-progress
        # liveness) and reset the idle clock: startup (slot acquisition + first page) gets
        # a fresh window, and every still-queued job sees this pickup as pool progress.
        pool.mark_running(job_key)
        result: Any = None
        reader = None
        if job is not None and factory is not None:
            try:
                reader = factory()
                # The consumer's update, not whatever this process's registration context
                # holds: staging tokens are keyed by it, and one minted under another update
                # revokes this update's, for every table. The stage paths follow the scope
                # the read below is given, which is an earlier update's for a scan taken up.
                reader.set_registration_scope(job["registration_scope"])
                # Each staged page resets the consumer's liveness watchdog, so a big table
                # that keeps making progress never trips it while a wedged drain does.
                reader._drain_progress = functools.partial(_drain_progressed, pool, job_key, reader)
                # The reader is marked as a drain worker, so this drains inline and returns
                # the staged page-0 result; the daemon ignores the rows -- it only needs the
                # durable manifest the drain publishes as a side effect -- so page 0 is
                # returned unread.
                reader._defer_staged_rows = True
                reader._read_snapshot(
                    job["table"],
                    None,
                    job["options"],
                    pipeline_scope_override=job["pipeline_scope"],
                    allow_keyless=job["allow_keyless"],
                )
            except Exception as error:  # noqa: BLE001 - surfaced to the waiting consumer
                # 157: Informix snapshot drain job failed; the consumer will re-raise and retry on
                # its next microbatch
                logger.warning(
                    _m(157),
                    exc_info=True,
                )
                result = error
            finally:
                _close_snapshot_drain_reader(reader)
        with pool.condition:
            pool.jobs.pop(job_key, None)
            pool.pending.discard(job_key)
            pool.running.discard(job_key)
            pool.progress.pop(job_key, None)
            pool.results[job_key] = result
            pool.condition.notify_all()


def _snapshot_drain_pool(key, *, reader_factory, thread_count) -> _SnapshotDrainPool:
    with _snapshot_drain_registry_lock():
        pool = _SNAPSHOT_DRAIN_POOLS.get(key)
        if pool is None:
            pool = _SNAPSHOT_DRAIN_POOLS.setdefault(key, _SnapshotDrainPool())
        pool.configure(reader_factory=reader_factory, thread_count=thread_count)
        threads = _SNAPSHOT_DRAIN_THREADS.setdefault(key, [])
        threads[:] = [thread for thread in threads if thread.is_alive()]
        index = len(threads)
        while len(threads) < thread_count:
            thread = threading.Thread(
                target=_run_snapshot_drain_worker,
                args=(pool, key),
                daemon=True,
                name=f"informix-snapshot-drain-{key[-1]}-{index}",
            )
            threads.append(thread)
            index += 1
            thread.start()
        return pool


class _AsyncStagePipeline:
    # Docstring: Overlap a drain's next page fetch with the previous page's shape+upload.
    #
    # Wraps the ``page_consumer`` the consistent-snapshot loop calls. ``submit``
    # hands a page to a single background worker and returns, so the loop can fetch
    # the next page while the worker shapes, compresses, and uploads the previous one.
    # A single worker preserves page order; a depth-1 queue bounds resident pages to
    # roughly three (one being fetched, one queued, one uploading) and applies
    # backpressure by blocking ``submit`` when the worker falls behind.
    #
    # The wrapped stage callable owns nonlocal counters (page/row counts); it runs
    # only on the worker thread, and ``close`` joins that thread before the caller
    # reads them. A stage error is captured and re-raised at the next ``submit`` (so
    # the fetch loop stops early) and again at ``close``; ``shutdown`` joins without
    # raising, for the path where a primary error is already propagating, and stages
    # none of the pages still queued: the failed drain's pages are never served.

    def __init__(self, stage: Callable[[int, int, list], None], *, queue_size: int = 1) -> None:
        self._stage = stage
        self._queue: queue.Queue = queue.Queue(maxsize=max(1, queue_size))
        self._error: BaseException | None = None
        self._aborted = False
        self._started = False
        self._thread = threading.Thread(
            target=self._run, name="informix-snapshot-uploader", daemon=True
        )

    def submit(self, snapshot_lsn: int, page_index: int, page_rows: list) -> None:
        if not self._started:
            self._started = True
            self._thread.start()
        if self._error is not None:
            raise self._error
        self._queue.put((snapshot_lsn, page_index, page_rows))

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            if self._error is not None or self._aborted:
                # Drain the backlog without processing once a page has failed; the
                # error is surfaced to the caller through submit/close.
                continue
            try:
                self._stage(*item)
            except BaseException as error:  # noqa: BLE001 - re-raised on the caller's thread
                self._error = error

    def close(self) -> None:
        # Docstring: Finish all queued pages, join the worker, then re-raise any stage error.

        if self._started:
            self._queue.put(None)
            self._thread.join()
        if self._error is not None:
            raise self._error

    def shutdown(self) -> None:
        # Docstring: Join the worker without raising -- for use while another error propagates.
        #
        # A page queued then is not uploaded: the caller still holds the snapshot's
        # transaction and its connection slot while the worker runs.

        self._aborted = True
        if self._started:
            self._queue.put(None)
            self._thread.join()


class _DeferredStagedPage:
    # Docstring: A staged page's rows, read from the stage only once iterated.
    #
    # ``page_batch`` is the page-serve tuple (see _snapshot_page_batch) the partitioned
    # reader uses to serve the page from an executor, so the driver never reads it.

    def __init__(self, read: Callable[[], Iterator[dict]], page_batch: tuple) -> None:
        self.page_batch = page_batch
        self._read = read
        self._rows: Iterator[dict] | None = None

    def __iter__(self) -> "_DeferredStagedPage":
        return self

    def __next__(self) -> dict:
        if self._rows is None:
            self._rows = self._read()
        return next(self._rows)


def _options_set(options: Any) -> Any:
    # Docstring: A table's options as the connector reads them: without the ones left empty,
    # under lower-case names. An empty value is one not set: a form sends "" for a field
    # nobody filled in. Read as set, it shadowed the connection's value with the default,
    # or failed as an invalid one. Only "" counts: a value of blanks is something typed, and
    # is judged as given (a tab is a delimiter, and a column list of one blank is a mistake
    # to report).
    #
    # Lower case because Spark hands a read its options under lower-case names while the
    # pipeline hands read_table_metadata a table's configuration as written: a key such
    # as "Primary.Keys" keyed the stream but not the registration, which then had no
    # delete flow. Every option the connector reads has a lower-case name. The same object
    # when nothing changes.
    try:
        items = list(options.items())
    except (TypeError, AttributeError):
        return options
    empty = {name for name, value in items if isinstance(value, str) and value == ""}
    if not empty and all(not isinstance(name, str) or name == name.lower() for name, _ in items):
        return options
    return {
        (name.lower() if isinstance(name, str) else name): value
        for name, value in items
        if name not in empty
    }


def _refuse_code_loading_options(options: dict[str, str]) -> None:
    # Docstring: Fail on ``bridge.factory`` or ``transport.factory``.
    #
    # Both named a module to import and a callable to hand every connection option to, the
    # passwords among them, and nothing but a test's own wiring ever needed them: whoever
    # could set a connection option could run code of their choosing on the compute. A
    # test replaces the bridge with set_bridge_factory, or sets a bridge's transport.
    for name in ("bridge.factory", "transport.factory"):
        if options.get(name):
            # 448: Option '{}' is not accepted: a connection option cannot name code to
            # load
            raise ValueError(_m(448, name))


def _load_factory(path: str) -> Callable[[dict[str, str]], Any]:
    # Docstring: The callable ``authentication.provider.factory`` names, an option for an
    # administrator-reviewed PAM exchange (see the README).
    module_name, separator, attribute = path.partition(":")
    if not separator:
        module_name, separator, attribute = path.rpartition(".")
    if not module_name or not attribute:
        # 332: authentication.provider.factory must be 'module:callable'
        raise ValueError(_m(332))
    factory = getattr(importlib.import_module(module_name), attribute)
    if not callable(factory):
        # 333: Factory {!r} is not callable
        raise TypeError(_m(333, path))
    return factory


_CATALOG_TYPES = {
    0: "CHAR",
    1: "SMALLINT",
    2: "INTEGER",
    3: "FLOAT",
    4: "SMALLFLOAT",
    5: "DECIMAL",
    6: "SERIAL",
    7: "DATE",
    8: "MONEY",
    10: "DATETIME",
    11: "BYTE",
    12: "TEXT",
    13: "VARCHAR",
    14: "INTERVAL",
    15: "NCHAR",
    16: "NVARCHAR",
    17: "INT8",
    18: "SERIAL8",
    19: "SET",
    20: "MULTISET",
    21: "LIST",
    22: "ROW",
    23: "COLLECTION",
    40: "UDT_VAR",
    41: "UDT_FIXED",
    43: "LVARCHAR",
    45: "BOOLEAN",
    52: "BIGINT",
    53: "BIGSERIAL",
    101: "BLOB",
    102: "CLOB",
}


def _field(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        if name in row:
            return row[name]
        lowered = {str(key).lower(): value for key, value in row.items()}
        if name.lower() in lowered:
            return lowered[name.lower()]
        # Informix names unaliased routine results after the full expression.
        # Lifecycle calls return a single scalar, so retain the positional
        # fallback used for tuple rows when a stable label is unavailable.
        return tuple(row.values())[index]
    return row[index]


def _catalog_column(row: Any) -> dict[str, Any]:
    name = str(_field(row, "colname", 0))
    raw_type = int(_field(row, "coltype", 1))
    length = int(_field(row, "collength", 2))
    base_type = raw_type & 0xFF
    type_name = _CATALOG_TYPES.get(base_type)
    if type_name is None:
        # 334: Unsupported Informix catalog coltype {} for {}
        raise InformixError(_m(334, base_type, name))
    if type_name in {"UDT_VAR", "UDT_FIXED"}:
        if isinstance(row, dict):
            lowered = {str(key).lower(): value for key, value in row.items()}
            extended_name = str(lowered.get("extended_name") or "").strip().upper()
            extended_owner = str(lowered.get("extended_owner") or "").strip().upper()
        else:
            extended_name = str(row[6] or "").strip().upper() if len(row) > 6 else ""
            extended_owner = str(row[7] or "").strip().upper() if len(row) > 7 else ""
        # BLOB and CLOB are built-in opaque types too; naming them lets the warning for a
        # column left out say which type it was. Both stay unsupported.
        builtin_extended_types = {
            ("UDT_VAR", "INFORMIX", "LVARCHAR"): "LVARCHAR",
            ("UDT_FIXED", "INFORMIX", "BOOLEAN"): "BOOLEAN",
            ("UDT_FIXED", "INFORMIX", "BLOB"): "BLOB",
            ("UDT_FIXED", "INFORMIX", "CLOB"): "CLOB",
        }
        type_name = builtin_extended_types.get(
            (type_name, extended_owner, extended_name), type_name
        )
    if type_name == "DATETIME":
        # syscolumns.collength stores the packed width in its high byte and
        # start/end qualifier nibbles in its low byte.  CDC descriptors use
        # the native extended-id layout instead: start in bits 8..11 and end in
        # bits 0..3.  For example, live YEAR TO FRACTION(5) is 0x130f in the
        # catalog and must become 0x000f for the native row decoder.
        encoded_qualifier = length & 0xFF
        length = ((encoded_qualifier >> 4) << 8) | (encoded_qualifier & 0x0F)
    precision = scale = None
    if type_name in {"DECIMAL", "MONEY"}:
        precision, scale = (length >> 8) & 0xFF, length & 0xFF
    unsupported = {
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    cdc_supported = type_name not in unsupported
    if type_name == "DATETIME":
        cdc_supported = _datetime_qualifier_supported(length)
    if type_name == "INTERVAL":
        cdc_supported = interval_layout(length) is not None
    return {
        "name": name,
        "type_name": type_name,
        "nullable": not bool(raw_type & 0x100),
        "length": length,
        "precision": precision,
        "scale": scale,
        "cdc_supported": cdc_supported,
    }


def _column_descriptor(raw: dict[str, Any]) -> ColumnDescriptor:
    return ColumnDescriptor(
        name=str(raw["name"]),
        type_name=str(raw["type_name"]),
        length=int(raw.get("length") or 0),
        precision=_optional_int(raw.get("precision")),
        scale=_optional_int(raw.get("scale")),
        encoding=str(raw.get("encoding") or "utf-8"),
    )


def _expect_zero(rows: list[Any], operation: str) -> None:
    if not rows:
        raise InformixError(f"{operation} returned no status")
    status = int(_field(rows[0], "status", 0))
    if status != 0:
        raise InformixError(f"{operation} failed with status {status}")


def _sysmaster_database_name(database: str) -> str:
    # Docstring: A database's name as sysmaster holds it. Informix folds a database name to
    # lower case, so a connection's ``database: TESTDB`` opened testdb, and then found no
    # row for 'TESTDB': the snapshot could not tell its transaction mode, and the
    # DB_LOCALE check was skipped.

    return str(database).lower()


def _transport_closed(transport) -> bool:
    # Docstring: Whether the SQLI client has closed its connection, as it does when a call
    # fails or times out (state POISONED or CLOSED). A transport without a state counts
    # as open.

    state = getattr(getattr(transport, "state", None), "name", None)
    return state in ("POISONED", "CLOSED")


def _is_timeout_error(error: BaseException | None) -> bool:
    # Docstring: Report whether ``error`` (or anything it wraps) is a socket read timeout.
    #
    # A SQLI socket read timeout surfaces as a bare ``TimeoutError`` (``socket.timeout``
    # is an alias of it on Python 3.10+); it may also travel as the ``__cause__`` of a
    # wrapping connector error, so walk the cause chain rather than checking the top
    # exception alone.

    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        if isinstance(error, TimeoutError):
            return True
        seen.add(id(error))
        error = error.__cause__ or error.__context__
    return False


def _patterns(value: str | None) -> tuple[str, ...]:
    return tuple(part.strip() for part in (value or "").split(",") if part.strip())


def _cdc_streamable(table: Table) -> bool:
    # Docstring: Report whether Informix can capture this table's changes at all.
    #
    # Split from :func:`_cdc_capable` because the two questions are different and
    # conflating them made every key-less table snapshot-only. ``cdc_startcapture``
    # takes ``(session, table, columns, full_row_logging)`` -- no key -- so the
    # logical log records inserts, updates, and deletes for any table with full-row
    # logging enabled. A primary key is needed to *merge* a change into the
    # destination, not to read it.
    #
    # Missing/placeholder values are unsafe in Lakeflow table rows, especially for
    # binary and complex types, so a table containing uncaptured columns is still
    # excluded here rather than silently nulling those values.

    return all(column.cdc_supported for column in table.columns)


def _cdc_capable(table: Table) -> bool:
    # Docstring: Report whether this table can be merged into a keyed destination.
    #
    # Requires a primary key on top of :func:`_cdc_streamable`: both the upsert
    # channel's ``apply_changes`` target and the delete channel's row identification
    # need one, as does snapshot keyset pagination.

    return bool(table.primary_keys) and _cdc_streamable(table)


# Key types whose values Informix doesn't reliably parse back from a SQL literal, so a
# snapshot keyed on one pages by position within its transaction instead.
# Key types whose values can't be rendered back into SQL as exact keyset literals, so a
# snapshot pages them by position instead: partial DATETIME and INTERVAL literals don't
# all parse back, and a float has no exact decimal literal (nor any rendering at all).
# Every name a float type may have: one left out passed the keyset guards and failed in
# rendering instead.
_FLOAT_KEY_TYPES = _UNLOAD_FLOAT_TYPES
# A BOOLEAN has no order to page by: Informix compares it for equality only, and its
# literals are 't' and 'f', which a bound value is not rendered as.
_BOOLEAN_KEY_TYPES = frozenset({"BOOLEAN", "BOOL"})
_LITERAL_UNSAFE_KEY_TYPES = frozenset(
    {"DATETIME", "INTERVAL", *_FLOAT_KEY_TYPES, *_BOOLEAN_KEY_TYPES}
)

_VARIABLE_DECIMAL_DEFAULT_TARGET = ("decimal", 38, 18)


def _variable_decimal_text(target: tuple[str, int | None, int | None]) -> str:
    # Docstring: A variable-DECIMAL target as the option writes it.

    kind, precision, scale = target
    return f"decimal({precision},{scale})" if kind == "decimal" else kind


def _apply_decimal_targets(table: Table, options: dict[str, str]) -> Table:
    # Docstring: Record the types decimal.variable.type and decimal.variable.column.type give
    # the table's variable-scale DECIMAL columns (Table.decimal_targets, and the table-wide
    # one as Table.decimal_default), and refuse a decimal.variable.column.type entry for a
    # column that is not one.
    #
    # A misspelt name was ignored, and the column landed as the default type.

    variable = [column for column in table.columns if _is_variable_scale_decimal(column)]
    names = {column.name for column in variable}
    for name in _variable_decimal_column_overrides(options):
        if name not in names:
            # 458: Option 'decimal.variable.column.type' names {}, which is not a
            # variable-scale DECIMAL column '{}' has or selects
            raise InformixError(_m(458, name, table.exposed_name))
    targets = []
    for column in variable:
        target = _variable_decimal_target(column, options)
        if target != _VARIABLE_DECIMAL_DEFAULT_TARGET:
            targets.append((column.name, _variable_decimal_text(target)))
    table_wide = _parse_variable_decimal_type(
        options.get("decimal.variable.type", _VARIABLE_DECIMAL_DEFAULT),
        option="decimal.variable.type",
    )
    recorded = tuple(sorted(targets))
    default = (
        None
        if table_wide == _VARIABLE_DECIMAL_DEFAULT_TARGET
        else _variable_decimal_text(table_wide)
    )
    if table.decimal_targets == recorded and table.decimal_default == default:
        return table
    return replace(table, decimal_targets=recorded, decimal_default=default)


def _validate_variable_decimal_options(options: dict[str, str]) -> None:
    # Docstring: Eagerly validate the variable-DECIMAL options so bad specs fail fast.

    _parse_variable_decimal_type(
        options.get("decimal.variable.type", _VARIABLE_DECIMAL_DEFAULT),
        option="decimal.variable.type",
    )
    _variable_decimal_column_overrides(options)


def _ensure_materializable(table: Table, options: dict[str, str] | None = None) -> None:
    unsupported = _snapshot_unsupported_columns(table)
    if unsupported:
        details = _unsupported_column_list(unsupported)
        # 341: Table '{}' contains columns that the pure-Python SQLI snapshot decoder cannot
        # materialize: {}
        raise InformixError(_m(341, table.exposed_name, details))
    options = options or {}
    if (
        table.primary_keys
        and not table.key_only_from_option
        and _snapshot_scan_columns(options.get(_SNAPSHOT_SCAN_OPTION, ""))
    ):
        # 432: Option snapshot.scan=keyset(...) names columns to page '{}' by, but the
        # table has a key and pages by that; change the key with primary.keys instead
        raise InformixError(_m(432, table.exposed_name))
    _validate_variable_decimal_options(options)
    for column in table.columns:
        if column.type_name in {"DECIMAL", "NUMERIC", "MONEY"}:
            variable_scale = column.type_name in {"DECIMAL", "NUMERIC"} and (column.scale == 0xFF)
            valid_fixed = (
                column.precision is not None
                and column.scale is not None
                and 1 <= column.precision <= 38
                and 0 <= column.scale <= column.precision
            )
            # A variable-scale DECIMAL(p) always maps to a materializable target
            # (string, double, integer, or decimal(p,s)) chosen by the
            # decimal.variable.type / decimal.variable.column.type options.
            if valid_fixed or variable_scale:
                continue
            # 342: Table '{}' has invalid {} metadata for column {}: precision={}, scale={}
            raise InformixError(
                _m(
                    342,
                    table.exposed_name,
                    column.type_name,
                    column.name,
                    column.precision,
                    column.scale,
                )
            )


def _transaction_batch(
    records: Sequence[dict[str, Any]],
) -> tuple[list[CommittedTransaction], bool, int | None, int | None]:
    # Docstring: Assemble one poll's records: ``(committed, caught_up, open_begin, reached)``.
    #
    # ``reached`` is the LSN of the poll's TIMEOUT record -- how far Informix's log
    # reader had scanned when it went idle -- or None if the poll did not time out.
    # That can be short of the log's end (measured: idle at page 6141 while the log was
    # at 6157), so being caught up says nothing about a later position by itself.

    buffer = TransactionBuffer()
    result = []
    timed_out = False
    reached = None
    for record in records:
        if _operation(record) == "TIMEOUT":
            timed_out = True
            reached = _lsn(record)
        committed = buffer.feed(record)
        if committed is not None:
            result.append(committed)
    # Informix may interleave active transactions without returning their records
    # in one globally monotonic LSN sequence. Transaction.advance() still validates
    # monotonicity within each transaction. Order completed transactions by their
    # commit position before the caller advances its checkpoint, otherwise a higher
    # commit returned first could checkpoint past a lower commit returned later.
    result.sort(key=lambda tx: (tx.commit_lsn, tx.tx_id))
    # Open transactions are intentionally discarded.  The returned offset
    # remains before their BEGIN so a finite next call safely replays them.
    open_begin = min((tx.begin_lsn for tx in buffer.open.values()), default=None)
    return result, timed_out and not buffer.open, open_begin, reached


def _recover(
    transactions: Sequence[CommittedTransaction], checkpoint: dict[str, Any]
) -> list[CommittedTransaction]:
    commit = int(checkpoint["commit_lsn"])
    # Offsets are transaction-atomic. Replaying from the oldest open BEGIN can
    # reproduce transactions already checkpointed, but records from a newly
    # committed transaction must never be filtered using another transaction's
    # commit/change LSN.
    return [tx for tx in transactions if tx.commit_lsn > commit]


def _database_encoding(options: dict[str, str]) -> str:
    # Docstring: The database code set's Python encoding: what LENGTH counts bytes in, and
    # what the server sends every text in, query results and change records alike.

    locale = options.get("db_locale") or options.get("DB_LOCALE") or options.get("db.locale")
    return informix_locale_encoding(locale or "en_US.819")


def _client_encoding(options: dict[str, str]) -> str:
    # Docstring: The codeset of an extract file (.unl, HPL): CLIENT_LOCALE's, which the
    # dbaccess that wrote it converted the database's text to. Not the server's: it sends
    # text in the database codeset (_database_encoding).
    #
    # Spark lowercases option keys, so ``CLIENT_LOCALE`` arrives as
    # ``client_locale``; check that first, then the original-case / dotted forms.
    locale = (
        options.get("client_locale")
        or options.get("CLIENT_LOCALE")
        or options.get("client.locale")
        or "en_US.utf8"
    )
    return informix_locale_encoding(locale)


def _capture_descriptor(table: Table, encoding: str = "utf-8") -> dict[str, Any]:
    # ``encoding`` is the database codeset's (_database_encoding): a change record carries
    # text as the database holds it, whatever the session's CLIENT_LOCALE (probed on 14.10).
    return {
        "identity": table.native_identity,
        "logical_identity": table.identity,
        "columns": [column.name for column in table.columns if column.cdc_supported],
        "descriptors": [
            {
                "name": column.name,
                "type_name": column.type_name,
                "length": column.length or column.precision or 0,
                "precision": column.precision,
                "scale": column.scale,
                "encoding": encoding,
            }
            for column in table.columns
            if column.cdc_supported
        ],
    }


def _unl_char(value: object, name: str, *, allow_empty: bool) -> str:
    # Docstring: Validate a `.unl`/HPL delimiter or escape option to a single character.
    #
    # HPL and UNLOAD field delimiters and escape characters are each one character. A
    # multi-character value is a configuration error, caught eagerly rather than silently
    # truncated. The escape takes ``none`` (``allow_empty``), which disables escape
    # processing for an HPL extract that writes no escapes: given as "", which a form sends
    # for a field nobody filled in, it turned escaping off on every load of the connection.

    text = "" if value is None else str(value)
    if allow_empty and text.lower() == _UNL_NO_ESCAPE:
        return ""
    if text == "":
        # 359: Option '{}' must be a single character
        raise ValueError(_m(359, name))
    if len(text) != 1:
        # 360: Option '{}' must be a single character, got {!r}
        raise ValueError(_m(360, name, text))
    return text


def _validate_unl_field_format(options: dict[str, str]) -> None:
    # Docstring: Eagerly validate the connection-scope `.unl`/HPL delimiter and escape options.
    #
    # Table-scope overrides are validated the same way at resolve time by
    # ``_unl_field_format``; this catches a bad connection default at construction.

    delimiter = _unl_char(
        options.get(_UNL_DELIMITER_OPTION, _UNL_DEFAULT_DELIMITER),
        _UNL_DELIMITER_OPTION,
        allow_empty=False,
    )
    escape = _unl_char(
        options.get(_UNL_ESCAPE_OPTION, _UNL_DEFAULT_ESCAPE),
        _UNL_ESCAPE_OPTION,
        allow_empty=True,
    )
    if escape and escape == delimiter:
        # 259: Options '{}' and '{}' must differ
        raise ValueError(_m(259, _UNL_DELIMITER_OPTION, _UNL_ESCAPE_OPTION))


class _UnloadEmpty(str):
    # Docstring: An UNLOAD field that was one escaped space, which is how UNLOAD writes an
    # empty VARCHAR, NVARCHAR or LVARCHAR (probed on 14.10: ``|\ |`` for '', ``| |`` for
    # one blank, ``||`` for a NULL). It is the space it decodes to for every other use.
    pass


def _parse_unload_fields(
    line: str,
    *,
    delimiter: str = _UNL_DEFAULT_DELIMITER,
    escape: str = _UNL_DEFAULT_ESCAPE,
    fields: int | None = None,
) -> list[str]:
    # Docstring: Split one UNLOAD / HPL DELIMITED record into raw field strings.
    #
    # Delimited with a single-character field ``delimiter`` (``|`` for UNLOAD) and, when
    # ``escape`` is non-empty (backslash for UNLOAD), backslash-style escaping: ``<esc>n``/
    # ``<esc>r``/``<esc>t`` decode to the control chars and ``<esc><other>`` to the literal
    # char. UNLOAD terminates each record with a trailing delimiter and HPL DELIMITED does
    # not; both are handled -- a line ending in the delimiter yields no spurious trailing
    # field (the final ``value`` is empty and dropped) and a line without one still yields
    # its last field. An empty field is the empty string here -- ``_unload_value`` maps it
    # to NULL (both writers render NULL as an empty field). An empty ``escape`` disables
    # escape processing (raw split on the delimiter), for an HPL job that writes no escapes.

    values: list[str] = []
    value: list[str] = []
    escaped = False
    # Whether the field so far is one escaped space and nothing else: UNLOAD's form of
    # an empty VARCHAR or LVARCHAR (see _UnloadEmpty).
    empty = False

    def field() -> str:
        return _UnloadEmpty(" ") if empty else "".join(value)

    def escaped_at(index: int) -> bool:
        # Whether an odd run of escapes precedes line[index], making it data.
        run = 0
        while index - run > 0 and line[index - run - 1] == escape:
            run += 1
        return run % 2 == 1

    # The line's own terminator only, where the caller left it on. A newline or carriage
    # return after an escape is data: an HPL record has no trailing delimiter, so its last
    # value can end in one, and the record splitter has taken the terminator off already.
    for terminator in ("\n", "\r"):
        if line.endswith(terminator) and not (escape and escaped_at(len(line) - 1)):
            line = line[:-1]
    for character in line:
        if escape and escaped:
            empty = character == " " and not value
            value.append({"n": "\n", "r": "\r", "t": "\t"}.get(character, character))
            escaped = False
        elif escape and character == escape:
            escaped = True
        elif character == delimiter:
            values.append(field())
            value, empty = [], False
        else:
            empty = False
            value.append(character)
    if escape and escaped:
        # 452: An UNLOAD record ends in a lone escape character '{}': the extract is cut
        # short, or was written with another escape
        raise InformixError(_m(452, escape))
    # Short of ``fields``, an empty final field is real: HPL writes no trailing delimiter,
    # so a NULL last column leaves the record ending in a delimiter, as UNLOAD's does.
    if value or (fields is not None and len(values) < fields):
        values.append(field())
    return values


def _unload_float(text: str, type_name: str, single: bool) -> float:
    # Docstring: An UNLOAD float field; a SMALLFLOAT's in its 32 bits, as SQLI and CDC give it,
    # not the 9 digits UNLOAD writes (0.1 as 0.100000001): HASH read those digits, and a
    # SMALLFLOAT key hashed apart from its CDC changes. A finite one past 32 bits is no
    # SMALLFLOAT's.

    value = float(text)
    if math.isinf(value) and "inf" not in text.lower():
        # A number past a double's range, read as infinity: 1e400 passed for "inf".
        # 470: UNLOAD value {!r} in snapshot.source is not a finite number, which no
        # Informix {} column holds
        raise InformixError(_m(470, text, type_name))
    if not single or not math.isfinite(value):
        return value
    rounded = _float32(value) if abs(value) < 3.5e38 else math.inf
    if not math.isfinite(rounded):
        # 470: UNLOAD value {!r} in snapshot.source is not a finite number, which no
        # Informix {} column holds
        raise InformixError(_m(470, text, type_name))
    return rounded
