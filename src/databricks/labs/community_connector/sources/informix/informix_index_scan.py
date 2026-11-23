"""The index-ordered snapshot scan, the copy's change filters, and .unl snapshot sources."""

from __future__ import annotations

import bisect
import collections
import json
import logging
import os
import time
from dataclasses import replace
from typing import Callable

from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _capture_descriptor,
    _database_encoding,
    _transaction_batch,
    _unl_char,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _NULL_BYTE_OPTION,
    _coerce_variable_decimal_values,
    _framework_row,
    _index_scan_key,
    _index_scan_key_text,
    _index_scan_lookups,
    _index_scan_order,
    _index_scan_plain,
    _index_scan_row_key,
    _index_scan_staged_key,
    _is_variable_scale_decimal,
    _ScanReads,
    _snapshot_scan_columns,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import _validated_offset
from databricks.labs.community_connector.sources.informix.informix_options import (
    _DEFAULT_CDC_MAX_RECORDS,
    _DEFAULT_SNAPSHOT_ISOLATION,
    _INDEX_SCAN_FOLLOW_ROWS,
    _INDEX_SCAN_FOLLOW_SECONDS,
    _INDEX_SCAN_KEY_ROWS,
    _INDEX_SCAN_KEY_TYPES,
    _INDEX_SCAN_LOOKUP_CHARS,
    _INDEX_SCAN_LOOKUP_ROWS,
    _INDEX_SCAN_MOVED_ROWS,
    _INDEX_SCAN_POLL_RECORDS,
    _INDEX_SCAN_POLL_SECONDS,
    _INDEX_SCAN_PROOF_SECONDS,
    _INDEX_SCAN_ROW_READS,
    _INDEX_SCAN_SETTLE_ATTEMPTS,
    _INDEX_SCAN_TRACKED_ROWS,
    _SNAPSHOT_BOUNDARY_LSN_OPTION,
    _SNAPSHOT_ISOLATION_SQL,
    _SNAPSHOT_SCAN_OPTION,
    _SNAPSHOT_SOURCE_OPTION,
    _UNL_DEFAULT_DELIMITER,
    _UNL_DEFAULT_ESCAPE,
    _UNL_DELIMITER_OPTION,
    _UNL_ESCAPE_OPTION,
    _UNL_SNAPSHOT_MODES,
    COMMIT_LSN,
    COMMIT_TIME,
    CURSOR,
    OP,
    TX_ID,
    InformixError,
    _m,
    _sql_name,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    ConnectionCapacityUnavailable,
    Table,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _before_lsn,
    _dropped_mount_error,
    _is_path,
    _lsn,
    _operation,
    _record_matches,
    _refuse_altered_changes,
    _validated_volume_location,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _decode_snapshot_stage_value,
    _encode_snapshot_stage_value,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    WorkspaceHttpError,
)


class _IndexScanMixin:
    # Docstring: The index-ordered snapshot scan, the copy's change filters, and .unl snapshot
    # sources.

    # pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
    def _index_scan_snapshot(
        self,
        table,
        options,
        index,
        page_size,
        max_rows,
        stage_page,
        staged_rows,
        scan,
        resume=None,
        progress=None,
    ) -> int | None:
        # Docstring: Stage a keyless table by an index and record, in ``scan``, which of its
        # changes the stream must not replay; returns the snapshot LSN. Returns None, with
        # nothing staged, when the index was chosen for the table and too many of its rows
        # have a NULL in it: the snapshot then reads as it would without the index.
        #
        # The stream replays every transaction that committed after the snapshot LSN, and the
        # scan has already seen some of them: the snapshot LSN is rounded down to its log
        # page, and the scan takes no locks. The table is read a page at a time in the order
        # of an index, each key whole within one read, and staged as read, with the log
        # position sampled before the read (rounded down to its log page) and after it
        # (rounded up). A change that committed before the read of its key's page is in the
        # page, so the stream drops it (see _drop_scanned_changes); so is one the follower
        # held when the read began. One that committed after the read is not in the page.
        # One that committed between the two samples may be either: its key is read again
        # until a read has no change to that key between its own two samples, and the rows
        # staged for it, read back, show which of those changes the scan saw. Those are
        # dropped by their own log positions. ``scan`` goes into the snapshot's manifest.

        bridge = _ScanReads(self, table)
        timeout = self._table_int_option(options, "cdc.timeout", 5, minimum=1)
        keys, descending = index
        # A table keyed by primary.keys alone and merged by it needs the index for paging
        # only: the merge takes out what the snapshot and the stream both hold. On the
        # append path nothing does, as for a table with no key.
        follows = not table.primary_keys or self._append_only_table(table, options)
        names = [column.name for column in table.columns]
        isolation = _SNAPSHOT_ISOLATION_SQL[_DEFAULT_SNAPSHOT_ISOLATION]
        capture = _capture_descriptor(table, _database_encoding(self.options))

        def token(row):
            shaped = _framework_row(_coerce_variable_decimal_values(row, table, options))
            return json.dumps(shaped, sort_keys=True, default=str)

        # A scan taken up again keeps its snapshot LSN: the stream starts there, and the
        # follower reads the table's changes from there once more.
        boundary = resume["boundary"] if resume else bridge.boundary_lsn()
        start = last_commit = boundary
        begin, noted = progress or (None, None)
        if begin is not None and not resume:
            begin(boundary)  # pylint: disable=not-callable

        def note(bound, before, after, staged, count, at) -> None:
            # Record a page read, for a restart to go on after it.
            if noted is not None:
                noted(
                    boundary,
                    len(pages) - 1,
                    {
                        "after": after,
                        "before": before,
                        "bound": None
                        if bound is None
                        else _encode_snapshot_stage_value(_index_scan_plain(bound)),
                        "cursor": None if at is None else _encode_snapshot_stage_value(list(at)),
                        "index": staged,
                        "rows": count,
                    },
                )

        # Changes read from the log and not sorted yet, per key: (commit LSN, +1 for an
        # inserted image or -1 for a deleted one, the row's token, the change's own LSN,
        # its operation). ``events`` keeps those of the keys in doubt, whose index values
        # ``doubt`` holds for reading them again.
        fresh: dict[tuple, list[tuple]] = {}
        events: dict[tuple, list[tuple]] = {}
        doubt: dict[tuple, list] = {}
        # The keys settled, each with the position after its last read and the source's
        # clock then.
        settled: dict[tuple, tuple[int, int | None]] = {}
        clock = getattr(bridge, "current_utc_time", lambda: None)
        # The changes settled, by their own positions: those the scan saw, and all of them.
        suppress: list[int] = []
        decided: list[int] = []
        # The rows of a table keyed by primary.keys that changed index value during the
        # scan, by an UPDATE or by a DELETE and an INSERT of the same key. By key: the
        # row's last image (None once deleted), the commits that changed it since, and
        # the index values it has had, under which the pages may hold it. The pages can
        # hold such a row under both index values, or a later change to it under the new
        # one, so every copy is left out of them and the row is read once more at the
        # end, into a page of its own (``repair``).
        relocated: dict[str, list] = {}
        # The index values every changed row of such a table has had since the snapshot
        # LSN, by key. A second one makes the row one of those above.
        had: dict[str, set] = {}
        # The position after the last page read, once every page has been read. A row that
        # changes index value after it is in the pages as it was before, once, and the
        # stream brings the change: only a change the pages may hold makes a row one to
        # read again. Without this a table whose rows keep changing index value would
        # hand the scan new rows to read again for as long as it follows the changes
        # (seen live: 20 rounds of reads, each ending with rows that had just moved).
        scanned_to: int | None = None
        # Each page read, in scan order: its last key's order (None for the last page), the
        # positions before and after its read, and its staged page's index (None if empty).
        pages: list[tuple[tuple | None, int, int, int | None]] = []
        bounds: list[tuple] = []

        # pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
        def follow(target: int, proof: bool = False) -> None:
            # Read the table's changes until every commit at or below ``target`` is in, and
            # sort them. An idle poll is taken to mean that, as the stream takes it, but a
            # session can go idle before it has handed over what is there, and a server that
            # has stopped handing changes over only ever goes idle. With ``proof`` the poll
            # must report having read up to ``target``, or bring a later commit. An idle
            # session reports the last checkpoint's position, so on a quiet table that takes
            # until the next checkpoint. Waited for only 30 s, a change Informix CDC handed
            # over later than that (its triggered lag runs to minutes) reached the stream
            # alone, which failed for a full refresh (438). So it is waited for as long as a
            # poll may take, then taken as read; on a log nothing has written to since
            # ``target`` 30 s, as such a log may get no checkpoint to show it.
            nonlocal start, last_commit
            if not follows:
                return
            began = time.monotonic()
            idle_since = None
            # pylint: disable-next=too-many-nested-blocks
            while True:
                # Whether the log is no further than ``target``: asked before a poll, so that
                # a poll is still the last thing the wait does, and nothing that committed
                # after it can pass for having committed during the scan.
                quiet = (
                    idle_since is not None
                    and time.monotonic() - idle_since >= _INDEX_SCAN_PROOF_SECONDS
                    and bridge.current_lsn() <= target
                )
                records = bridge.read_changes(
                    [capture],
                    start,
                    timeout,
                    _DEFAULT_CDC_MAX_RECORDS,
                    budget=_INDEX_SCAN_POLL_RECORDS,
                    # A table that keeps changing never lets the session go idle: the poll
                    # ends at the first commit past the target, or to let the waits below
                    # be checked.
                    until=target,
                    seconds=_INDEX_SCAN_POLL_SECONDS,
                )
                _refuse_altered_changes(records, (*keys, *table.primary_keys), table, self.options)
                committed, _caught_up, open_begin, reached = _transaction_batch(records)
                if self._drain_progress is not None:
                    # A poll that returned shows the reader is not hung, whatever it
                    # brought: this wait has its own limit, below, and its own error.
                    self._drain_progress()
                # A poll that went idle has handed over every commit there is. A transaction
                # still open does not hold that up: the scan reads committed rows only, so
                # its changes are in no page, and it commits after the scan.
                idle = reached is not None
                for tx in committed:
                    if tx.commit_lsn <= last_commit:
                        continue
                    last_commit = tx.commit_lsn
                    for record in tx.records:
                        op = _operation(record)
                        if op == "TRUNCATE":
                            # 433: The index-ordered snapshot does not support {} on '{}'; set
                            # snapshot.scan=cursor to read the table without index order
                            raise InformixError(_m(433, "TRUNCATE", table.exposed_name))
                        before = record.get("before", record.get("row") if op == "DELETE" else None)
                        after = record.get("after", record.get("row") if op == "INSERT" else None)
                        # An UPDATE that changes an index value takes a row out of one key
                        # and puts one into another, and each key's page holds its half or
                        # does not: a delete under the old key, named by the before-image's
                        # own position, and an insert under the new one.
                        moved = None not in (before, after) and _index_scan_key(
                            before, keys
                        ) != _index_scan_key(after, keys)
                        named = [None, None]
                        # Paged by its own key, a row's index value is its key: an UPDATE of
                        # it already goes as the two halves above, and no row moves.
                        if table.primary_keys and set(keys) != set(table.primary_keys):
                            named = [
                                None if row is None else _index_scan_row_key(row, table, options)
                                for row in (before, after)
                            ]
                            for name, row, image in zip(named, (None, after), (before, after)):
                                if name is None:
                                    continue
                                if name not in had and len(had) >= _INDEX_SCAN_TRACKED_ROWS:
                                    # 433: The index-ordered snapshot does not support {} on '{}';
                                    # set snapshot.scan=cursor to read the table without index order
                                    raise InformixError(
                                        _m(
                                            433,
                                            f"more than {_INDEX_SCAN_TRACKED_ROWS} rows "
                                            "changing during it, for a table paged by an "
                                            "index other than its key,",
                                            table.exposed_name,
                                        )
                                    )
                                seen = had.setdefault(name, set())
                                seen.add(_index_scan_key(image, keys))
                                # Under a second index value, by an UPDATE or by a DELETE
                                # and an INSERT of the same key: read again at the end.
                                if (
                                    len(seen) > 1
                                    and name not in relocated
                                    and (scanned_to is None or tx.commit_lsn <= scanned_to)
                                ):
                                    if len(relocated) >= _INDEX_SCAN_MOVED_ROWS:
                                        # 442: The index-ordered snapshot of
                                        # '{}' found more than {} rows changing
                                        # index value during it; run the
                                        # snapshot when the table is quieter, or
                                        # set snapshot.scan=cursor
                                        raise InformixError(
                                            _m(442, table.exposed_name, _INDEX_SCAN_MOVED_ROWS)
                                        )
                                    relocated[name] = [None, [], seen]
                                if name in relocated:
                                    relocated[name][0] = row
                                    relocated[name][1].append(tx.commit_lsn)
                        for row, sign in ((before, -1), (after, 1)):
                            if row is not None:
                                key = _index_scan_key(row, keys)
                                fresh.setdefault(key, []).append(
                                    (
                                        tx.commit_lsn,
                                        sign,
                                        token(row),
                                        _before_lsn(record) if moved and sign < 0 else _lsn(record),
                                        ("DELETE" if sign < 0 else "INSERT") if moved else op,
                                        tx.commit_time,
                                        named[sign > 0],
                                    )
                                )
                                if key not in doubt and key not in raw:
                                    raw[key] = [row[name] for name in keys] if key else None
                # As the stream restarts: at the oldest open BEGIN, else the last commit.
                start = open_begin if open_begin is not None else max(start, last_commit)
                if last_commit > target or (idle and (not proof or reached >= target)):
                    break
                # The short wait is for proof alone, from the first idle poll: a backlog is
                # read to its end first.
                now = time.monotonic()
                if idle and (quiet or now - began > _INDEX_SCAN_FOLLOW_SECONDS):
                    break
                # From the first idle poll since one that brought changes (late ones, on a
                # log that hasn't moved, may have more behind them); the log moved, asked
                # again a proof interval on.
                idle_since = (
                    None
                    if not idle
                    else now
                    if idle_since is None or now - idle_since >= _INDEX_SCAN_PROOF_SECONDS
                    else idle_since
                )
                if now - began > _INDEX_SCAN_FOLLOW_SECONDS:
                    # 435: The index-ordered snapshot stopped waiting for
                    # Informix CDC to reach the snapshot's position for '{}'
                    # after {} seconds; a transaction left open on the table
                    # with more changes behind it than cdc.max.poll.records
                    # can keep it from getting there, as can a table that
                    # changes faster than its changes are read
                    raise InformixError(_m(435, table.exposed_name, _INDEX_SCAN_FOLLOW_SECONDS))
            for key, changed in fresh.items():
                # A row read again at the end is in no page: its changes settle no key.
                changed = [event for event in changed if event[6] not in relocated]
                if not changed:
                    continue
                if key in settled:
                    # A change the key's last read may have held, handed over only now:
                    # the key was settled without it. One the source's clock puts after
                    # that read is the stream's.
                    position, clock = settled[key]
                    if any(
                        event[0] <= position and not (clock and event[5] and event[5] > clock)
                        for event in changed
                    ):
                        # 436: The index-ordered snapshot cannot tell
                        # which changes to a key of '{}' its scan saw: the
                        # rows staged for the key do not fit the key's
                        # changes
                        raise InformixError(_m(436, table.exposed_name))
                    # Those it could have held by position are kept by name.
                    decided.extend(event[3] for event in changed if event[0] <= position)
                    continue
                if key in doubt:
                    events[key].extend(changed)
                    continue
                page = bisect.bisect_left(bounds, _index_scan_order(key, descending))
                # A key not read yet: its page will hold these changes, and its read will
                # start after the follower held them, so the stream drops them.
                if page < len(pages):
                    _bound, before, after, _index = pages[page]
                    if any(before < event[0] <= after for event in changed):
                        doubt[key] = raw[key]
                        events[key] = [event for event in changed if event[0] > before]
            fresh.clear()
            raw.clear()

        raw: dict[tuple, list] = {}
        cursor = None
        held = total = staged_pages = 0
        for entry in resume["pages"] if resume else ():
            bound = (
                None
                if entry["bound"] is None
                else _index_scan_order(
                    tuple(_decode_snapshot_stage_value(entry["bound"])), descending
                )
            )
            pages.append((bound, int(entry["before"]), int(entry["after"]), entry["index"]))
            if bound is not None:
                bounds.append(bound)
            if entry["cursor"] is not None:
                cursor = _decode_snapshot_stage_value(entry["cursor"])
            total += int(entry["rows"])
            staged_pages += entry["index"] is not None
        # The scan had read its last page: only the keys in doubt are left to settle.
        complete = bool(pages) and pages[-1][0] is None
        # Rows with a NULL in an index column are no part of the keyed order: one group,
        # read first, and left out of the pages that follow. The index finds the NULLs of
        # its leading column; those of its other columns take a pass over the table.
        optional = {column.name for column in table.columns if column.nullable}
        nullable = [name for name in keys if name in optional]
        lead = nullable[:1] if nullable[:1] == list(keys[:1]) else []
        rest = nullable[len(lead) :]
        present = " AND ".join(f"{_sql_name(name)} IS NOT NULL" for name in nullable)

        def null_rows(limit=None):
            # Them all, or one more than ``limit`` of them at most.
            found: list = []
            for part, others in ((lead, []), (rest, lead)):
                if part and (limit is None or len(found) <= limit):
                    found += bridge.key_rows(
                        table.identity,
                        names,
                        part,
                        None,
                        isolation,
                        limit and limit + 1 - len(found),
                        others,
                    )
            return found

        if nullable and not pages:
            before = bridge.boundary_lsn()
            # Too many to read together? The index answers that for its leading column
            # without their rows being read; for the others the read itself must.
            many = lead and bridge.key_rows(
                table.identity, lead, lead, None, isolation, 1, (), _INDEX_SCAN_KEY_ROWS
            )
            rows = [] if many else null_rows(_INDEX_SCAN_KEY_ROWS)
            if many or len(rows) > _INDEX_SCAN_KEY_ROWS:
                reason = f"more than {_INDEX_SCAN_KEY_ROWS} rows with a NULL index value"
                if _snapshot_scan_columns(
                    options.get(_SNAPSHOT_SCAN_OPTION, self.options.get(_SNAPSHOT_SCAN_OPTION, ""))
                ):
                    # 433: The index-ordered snapshot does not support {} on '{}'; set
                    # snapshot.scan=cursor to read the table without index order
                    raise InformixError(_m(433, reason, table.exposed_name))
                if follows:
                    # 437: Informix snapshot of '{}', which has no key of its
                    # own, reads without index order because it has {}; a row
                    # that changes during the snapshot can then be in its
                    # change log twice, or not at all
                    logging.getLogger(__name__).warning(_m(437, table.exposed_name, reason))
                return None
            after = bridge.current_lsn()
            total = len(rows)
            if rows:
                stage_page(boundary, 0, rows)
            pages.append(((), before, after, 0 if rows else None))
            staged_pages = int(bool(rows))
            bounds.append(())
            note((), before, after, 0 if rows else None, len(rows), None)
        while not complete:
            # A commit the follower already holds was made before this read, wherever it
            # sits in the log page the read's own position is rounded down to.
            known = last_commit
            before, rows = bridge.snapshot_chunk(
                table.identity,
                names,
                keys,
                cursor,
                page_size,
                snapshot_filter=present or None,
                key_descending=descending,
                isolation=isolation,
            )
            orders = [_index_scan_order(_index_scan_key(row, keys), descending) for row in rows]
            if () in orders:
                # No NULL is among these rows, so the connector made one of a value: paging
                # on from it would lose the rows after it.
                # 433: The index-ordered snapshot does not support {} on '{}'; set
                # snapshot.scan=cursor to read the table without index order
                raise InformixError(
                    _m(
                        433,
                        "an index value read as NULL, such as a string holding a NUL byte "
                        f"under {_NULL_BYTE_OPTION}=null,",
                        table.exposed_name,
                    )
                )
            if follows and (
                any(later < earlier for earlier, later in zip(orders, orders[1:]))
                or (orders and bounds and not bounds[-1] < orders[0])
            ):
                # The page lookup of a change's key relies on it.
                # 433: The index-ordered snapshot does not support {} on '{}'; set
                # snapshot.scan=cursor to read the table without index order
                raise InformixError(
                    _m(
                        433,
                        "index values that Informix does not sort by their characters",
                        table.exposed_name,
                    )
                )
            more = len(rows) >= page_size
            bound = None
            if more:
                # The last key may go on past the page: read it whole if it is the only
                # one, else leave it to the next page.
                last = _index_scan_key(rows[-1], keys)
                if _index_scan_key(rows[0], keys) == last:
                    cursor = [rows[-1][name] for name in keys]
                    rows = bridge.key_rows(
                        table.identity, names, keys, cursor, isolation, _INDEX_SCAN_KEY_ROWS + 1
                    )
                    if len(rows) > _INDEX_SCAN_KEY_ROWS:
                        # 433: The index-ordered snapshot does not support {} on '{}'; set
                        # snapshot.scan=cursor to read the table without index order
                        raise InformixError(
                            _m(
                                433,
                                f"more than {_INDEX_SCAN_KEY_ROWS} rows under one index value",
                                table.exposed_name,
                            )
                        )
                else:
                    while _index_scan_key(rows[-1], keys) == last:
                        rows.pop()
                    cursor = [rows[-1][name] for name in keys]
                    last = _index_scan_key(rows[-1], keys)
                bound = _index_scan_order(last, descending)
            after = bridge.current_lsn()
            total += len(rows)
            if max_rows and total > max_rows:
                # 52: Initial snapshot exceeds snapshot.max.rows={}
                raise InformixError(_m(52, max_rows))
            if rows:
                stage_page(boundary, staged_pages, rows)
            pages.append((bound, max(before, known), after, staged_pages if rows else None))
            note(
                bound, max(before, known), after, staged_pages if rows else None, len(rows), cursor
            )
            staged_pages += bool(rows)
            if bound is not None:
                bounds.append(bound)
            held += len(rows)
            if more and held >= _INDEX_SCAN_FOLLOW_ROWS:
                follow(after)
                held = 0
            if not more:
                break
        # Every change that committed during the scan should be in before any key is
        # settled: the follower waits to be shown to have read the log through the page the
        # scan ended on. A quiet table on a busy server shows it only at the next
        # checkpoint, so the wait is bounded; a change it misses then is one the stream
        # fails on.
        scanned_to = max((after for _bound, _before, after, _index in pages), default=boundary)
        follow(bridge.current_lsn(), proof=True)
        # The rows of ``relocated`` read again, by key: the follower's last commit before
        # the row's read, the position after it, the rows found, and whether the row was
        # there to find.
        repair: dict[str, tuple[int, int, list, bool]] = {}

        def unrepaired() -> list[str]:
            # The rows not read again yet, or changed between the positions of their read.
            return [
                name
                for name, (_image, commits, _had) in relocated.items()
                if name not in repair
                or any(repair[name][0] < commit <= repair[name][1] for commit in commits)
                # Not found where its last change left it, or found twice: a change the
                # follower has not been handed yet may explain it.
                or len(repair[name][2]) != repair[name][3]
            ]

        # The keys in doubt and the rows read again each get their own number of reads.
        key_reads = row_reads = 0
        while True:
            if not doubt:
                again = unrepaired()
                if not again or row_reads >= _INDEX_SCAN_ROW_READS:
                    break
                row_reads += 1
                # Each row is where its last change left it. A change the follower did
                # not hold yet shows in the poll after the read, and that row is read
                # once more; the others' reads stand.
                held_before = last_commit
                # By its index value and its key together, so that the index finds the
                # row and nothing else of that value is read; by the index value alone
                # when a key column is of a type the scan does not compare. Whatever a
                # read returns, a row counts only under its own key.
                # Nor a DECIMAL(p) with no scale: it is staged as decimal.variable.type
                # shapes it, so the row's key as read and as staged can differ in form.
                comparable = all(
                    column.type_name in _INDEX_SCAN_KEY_TYPES
                    and not _is_variable_scale_decimal(column)
                    for column in table.columns
                    if column.name in table.primary_keys
                )
                columns = list(dict.fromkeys([*keys, *(table.primary_keys if comparable else ())]))
                found: dict[str, list] = {name: [] for name in again}
                # Each set of values once: two rows can share an index value.
                lookups = _index_scan_lookups(
                    tuple(relocated[name][0][column] for column in columns)
                    for name in again
                    if relocated[name][0] is not None
                )
                # A query for many rows when each is found by its key; one at a time when
                # only its index value leads to it, which can hold many rows.
                size = _INDEX_SCAN_LOOKUP_ROWS if comparable else 1
                queries: list[tuple[list[str], list[tuple]]] = []
                room = 0
                # Each set of values is a SELECT of its own within the statement (see
                # rows_by_values), so it costs the column list and the table's name too:
                # on a wide table that is most of it.
                select = sum(len(name) + 3 for name in names) + len(table.identity) + 40
                for values in lookups:
                    where = sum(
                        len(str(value)) + len(column) + 8 for value, column in zip(values, columns)
                    )
                    if where > _INDEX_SCAN_LOOKUP_CHARS:
                        # Its values are too long to send: its index value alone finds
                        # the row. The column list is no reason to: it is sent either way.
                        queries.append((list(keys), [values[: len(keys)]]))
                        room = 0
                        continue
                    length = select + where
                    # A table so wide that one SELECT passes the budget still gets its
                    # rows one to a query, each by its key.
                    if room <= 0 or length > room or len(queries[-1][1]) >= size:
                        queries.append((columns, []))
                        room = _INDEX_SCAN_LOOKUP_CHARS
                    queries[-1][1].append(values)
                    room -= length
                filled: set[str] = set()
                for by, value_sets in queries:
                    batch: dict[str, list] = {}
                    rows_read = bridge.rows_by_values(
                        table.identity,
                        names,
                        by,
                        value_sets,
                        isolation,
                        _INDEX_SCAN_KEY_ROWS + 1,
                    )
                    if len(rows_read) > _INDEX_SCAN_KEY_ROWS:
                        # Cut at the limit, a copy of a row past it went unseen, and a row
                        # found once passed for found where it was.
                        # 433: The index-ordered snapshot does not support {} on '{}'; set
                        # snapshot.scan=cursor to read the table without index order
                        raise InformixError(
                            _m(
                                433,
                                f"more than {_INDEX_SCAN_KEY_ROWS} rows under one index value",
                                table.exposed_name,
                            )
                        )
                    for row in rows_read:
                        batch.setdefault(_index_scan_row_key(row, table, options), []).append(row)
                    for name, rows_found in batch.items():
                        # Informix compares character values without their trailing
                        # spaces, so a later query can return a row an earlier one did.
                        if name in found and name not in filled:
                            found[name] = rows_found
                    filled.update(batch)
                    if self._drain_progress is not None:
                        # The reader is not hung while these go on.
                        self._drain_progress()
                read_after = bridge.current_lsn()
                for name in again:
                    repair[name] = (
                        held_before,
                        read_after,
                        found[name],
                        relocated[name][0] is not None,
                    )
                follow(read_after, proof=True)
                continue
            if key_reads >= _INDEX_SCAN_SETTLE_ATTEMPTS:
                break
            key_reads += 1
            before = max(bridge.boundary_lsn(), last_commit)
            read = {
                key: [
                    (
                        token(row),
                        _index_scan_row_key(row, table, options) if table.primary_keys else None,
                    )
                    for row in (
                        null_rows()
                        if values is None
                        else bridge.key_rows(table.identity, names, keys, values, isolation)
                    )
                ]
                for key, values in doubt.items()
            }
            after = bridge.current_lsn()
            read_at = clock()
            # As at the scan's end: the wait for proof is bounded, and a change it misses
            # is one the scan or the stream fails on.
            follow(after, proof=True)
            for key, found in read.items():
                # The rows read again at the end take no part: they are left out of the
                # pages, and of what this key holds and how it changed.
                state = collections.Counter(
                    shaped for shaped, name in found if name not in relocated
                )
                changed = [event for event in events[key] if event[6] not in relocated]
                if any(before < event[0] <= after for event in changed):
                    continue
                _bound, read_before, read_after, staged_at = pages[
                    bisect.bisect_left(bounds, _index_scan_order(key, descending))
                ]
                unsure = [event for event in changed if event[0] <= read_after]
                # What the scan staged for the key, less what the key holds now, plus every
                # change since its read began: the net of the unsure changes the scan saw.
                saw = collections.Counter(
                    json.dumps({name: row[name] for name in names}, sort_keys=True, default=str)
                    for row in (() if staged_at is None else staged_rows(boundary, staged_at))
                    if _index_scan_key(row, keys) == key
                    and not (
                        relocated and _index_scan_staged_key(row, table.primary_keys) in relocated
                    )
                )
                saw.subtract(state)
                for event in changed:
                    if event[0] <= before:
                        saw[event[2]] += event[1]
                # The scan read the key at one moment as a rule, so it saw the unsure
                # transactions up to some point in commit order: the one where their net is
                # that. Their changes to the key are dropped, an UPDATE with both its images.
                seen: list[int] | None = [] if not +saw and not -saw else None
                net: collections.Counter = collections.Counter()
                for at, event in enumerate(unsure):
                    net[event[2]] += event[1]
                    last = at + 1 == len(unsure) or unsure[at + 1][0] != event[0]
                    if seen is None and last and not +(net - saw) and not +(saw - net):
                        seen = [event[3] for event in unsure[: at + 1]]
                if seen is None and not any(event[4] == "UPDATE" for event in unsure):
                    # It read the key while one of them committed. Identical rows cannot be
                    # told apart, so any inserts and deletes of the right rows will do.
                    seen = []
                    for event in unsure:
                        if saw[event[2]] * event[1] > 0:
                            saw[event[2]] -= event[1]
                            seen.append(event[3])
                    if any(saw.values()):
                        seen = None
                if seen is None:
                    # 436: The index-ordered snapshot cannot tell which
                    # changes to a key of '{}' its scan saw: the rows staged
                    # for the key do not fit the key's changes
                    raise InformixError(_m(436, table.exposed_name))
                suppress.extend(seen)
                # Every change up to this read is accounted for: the unsure ones dropped or
                # kept, the later ones kept.
                decided.extend(event[3] for event in changed)
                settled[key] = (after, read_at)
                del doubt[key], events[key]
        finished = clock()
        if doubt:
            # 434: The index-ordered snapshot could not settle {} key(s) of '{}' after
            # {} re-reads because they kept changing; run the snapshot when the table
            # is quieter, or set snapshot.scan=cursor
            raise InformixError(
                _m(434, len(doubt), table.exposed_name, _INDEX_SCAN_SETTLE_ATTEMPTS)
            )
        if unrepaired():
            amiss = [
                name
                for name, read in repair.items()
                if len(read[2]) != read[3]
                and not any(read[0] < commit <= read[1] for commit in relocated[name][1])
            ]
            if amiss:
                # Unchanged during its last read and still not found where its last
                # change left it, or found twice: its copies are left out of the pages,
                # so the row would be lost or doubled.
                # 444: The index-ordered snapshot of '{}' could not read again, each by
                # its index and key values, the {} row(s) that changed index value during
                # it; set snapshot.scan=cursor to read the table without index order
                raise InformixError(_m(444, table.exposed_name, len(amiss)))
            # 443: The index-ordered snapshot could not settle the {} row(s) of '{}'
            # that changed index value during it after {} re-reads because they kept
            # changing; run the snapshot when the table is quieter, or set
            # snapshot.scan=cursor
            raise InformixError(
                _m(443, len(unrepaired()), table.exposed_name, _INDEX_SCAN_ROW_READS)
            )
        moved_rows = None
        if relocated:
            # Not counted against snapshot.max.rows again: the pages counted these rows.
            found_rows = [row for _held, _after, rows, _there in repair.values() for row in rows]
            if found_rows:
                scan["moved_rows_page"] = True
                stage_page(boundary, staged_pages, found_rows)
            # Each row is left out of the pages read under an index value it has had.
            by_page: dict[str, list[str]] = {}
            for name, (_image, _commits, values_had) in sorted(relocated.items()):
                for staged_at in {
                    pages[bisect.bisect_left(bounds, _index_scan_order(key, descending))][3]
                    for key in values_had
                } - {None}:
                    by_page.setdefault(str(staged_at), []).append(name)
            # How many staged copies the pages leave out, for the snapshot's row count.
            scan["left_out_rows"] = sum(
                _index_scan_staged_key(row, table.primary_keys) in listed
                for at, listed in ((at, set(names_at)) for at, names_at in by_page.items())
                for row in staged_rows(boundary, int(at))
            )
            moved_rows = {
                "keys": list(table.primary_keys),
                # Each row with the positions of its read: a change to it is in the page
                # that holds the row now if the follower held it before the read, and none
                # committed between that and the position after it.
                "rows": {name: [repair[name][0], repair[name][1]] for name in sorted(repair)},
                # The pages each row is left out of, and the page that holds them now.
                "pages": by_page,
                "page": staged_pages if found_rows else None,
            }
        scan["index_scan"] = follows and {
            "keys": list(keys),
            "descending": list(descending),
            # Each page's last key, with the positions before and after its read. A change
            # to a key of the page that committed at or below the first is in the page. One
            # that committed between the two was settled, and is listed: dropped if the
            # scan saw it, kept if not.
            "pages": [
                [
                    None
                    if bound is None
                    else _encode_snapshot_stage_value(_index_scan_plain(bound)),
                    before,
                    after,
                ]
                for bound, before, after, _index in pages
            ],
            "end": max(
                [after for _bound, _before, after, _index in pages]
                + [position for position, _clock in settled.values()]
                + ([after for _held, after in moved_rows["rows"].values()] if moved_rows else [])
            ),
            "moved": moved_rows,
            "suppress": sorted(set(suppress)),
            "keep": sorted(set(decided) - set(suppress)),
            # A settled key's read went on until this position: its window ends there.
            "settled": [
                [_encode_snapshot_stage_value(list(key)), position]
                for key, (position, _clock) in settled.items()
            ],
            # The source's clock once every read was done. The second position is the end
            # of a log page, and a quiet log stays on that page long after the scan: a
            # change there that committed later by the clock is not one the scan missed.
            "time": finished,
        }
        return boundary

    # pylint: disable-next=too-many-locals,too-many-statements
    def _drop_scanned_changes(self, table, checkpoint, options, transactions, reached=None):
        # Docstring: ``transactions`` without the changes a keyless table's index-ordered
        # snapshot already holds (see _index_scan_snapshot).
        #
        # The snapshot's manifest names them: every change to a key that committed at or
        # below the position before that key's page was read, and the changes listed by
        # their own log positions. Nothing above the scan's end is dropped. A change that
        # committed between the two positions of its key's read, is not among those the scan
        # settled, and did not commit after the scan by the source's clock, reached the
        # scan's follower too late: it may be in the page or not, so the read fails for a
        # full refresh. The manifest is the snapshot's update's; the
        # stream's offsets name that update (scan_scope, see _read_stream) until the stream
        # is past the scan's end, since a later update's offsets carry its own scope.

        if (
            table.primary_keys
            and not table.key_only_from_option
            and not self._append_only_table(table, options)
        ) or (
            # snapshot.scan as it is now says nothing of a scan the snapshot took: set to
            # cursor before the stream was past its end, the offset lost the scan's name
            # (_read_stream) and the changes the pages hold were replayed.
            self._snapshot_cursor_scan(options) and not checkpoint.get("scan_scope")
        ):
            # No scan to be within: an offset stops naming one (see _read_stream).
            self.__dict__.setdefault("_index_scan_live", {})[table.identity] = None
            return transactions
        scope = checkpoint.get("scan_scope") or self._offset_scopes(checkpoint)[1]
        schema_id = str(checkpoint.get("scan_schema") or checkpoint.get("schema_id"))
        cache = self.__dict__.setdefault("_index_scans", {})
        cached = (table.identity, scope, schema_id)
        if cached not in cache:
            manifest = self._read_snapshot_stage_manifest(table, scope, schema_id) or {}
            recorded = manifest.get("index_scan")
            cache[cached] = recorded and (
                recorded["keys"],
                recorded["descending"],
                [
                    _index_scan_order(
                        tuple(_decode_snapshot_stage_value(page[0])), recorded["descending"]
                    )
                    for page in recorded["pages"]
                    if page[0] is not None
                ],
                [(int(page[1]), int(page[2])) for page in recorded["pages"]],
                int(recorded["end"]),
                frozenset(int(lsn) for lsn in recorded["suppress"]),
                frozenset(int(lsn) for lsn in recorded["keep"]),
                recorded.get("time"),
                {
                    tuple(_decode_snapshot_stage_value(key)): int(position)
                    for key, position in recorded.get("settled", ())
                },
                recorded.get("moved"),
                (recorded.get("moved") or {}).get("rows") or {},
            )
        live = self.__dict__.setdefault("_index_scan_live", {})
        if not cache[cached]:
            live[table.identity] = None
            return transactions
        (
            keys,
            descending,
            bounds,
            reads,
            end,
            suppress,
            keep,
            finished,
            settled,
            moved,
            relocated,
        ) = cache[cached]
        # A poll that went idle past the scan's end with nothing at or below it has seen the
        # last change the scan could hold: a quiet table's offset stays where it is, and
        # would name the snapshot's update for good.
        past = (reached or 0) > end and all(tx.commit_lsn > end for tx in transactions)
        live[table.identity] = None if past else (scope, schema_id, end)

        def held(tx, row, lsn) -> bool:
            # Whether the scan's pages hold the change to ``row`` named by ``lsn``.
            #
            # Informix returns a row logged before a column was dropped without it, so a
            # change replayed after the scan's index column went had no value to place
            # by (a KeyError). Nothing says then whether the pages hold it.
            missing = [
                name
                for name in dict.fromkeys((*keys, *(moved["keys"] if relocated else ())))
                if name not in row
            ]
            if missing:
                # 465: Informix table '{}' no longer has column(s) {}, which its
                # index-ordered snapshot paged by, while the stream has yet to pass the
                # snapshot's end; run a full refresh
                raise InformixError(_m(465, table.exposed_name, ", ".join(missing)))
            window = relocated and relocated.get(
                _index_scan_row_key(row, table, options, moved["keys"])
            )
            if window:
                # A row read again at the scan's end, into the page that holds it now:
                # that page has what the follower held before the row's read.
                if tx.commit_lsn <= window[0]:
                    return True
                if tx.commit_lsn <= window[1] and not (
                    finished and tx.commit_time and tx.commit_time > finished
                ):
                    # 438: A change to keyless table '{}' committed
                    # while its snapshot was reading the rows it
                    # applies to, and reached Informix CDC only after
                    # the snapshot was staged, so the change log may
                    # hold it twice or not at all; run a full refresh
                    raise InformixError(_m(438, table.exposed_name))
                return False
            key = _index_scan_key(row, keys)
            order = _index_scan_order(key, descending)
            before, after = reads[bisect.bisect_left(bounds, order)]
            after = max(after, settled.get(key, 0))
            if lsn in suppress or tx.commit_lsn <= before:
                return True
            # In the read's window and not settled: the scan's follower never got
            # it. By the clock it may still have committed after the scan, on the
            # log page the scan ended on.
            if (
                tx.commit_lsn <= after
                and lsn not in keep
                and not (finished and tx.commit_time and tx.commit_time > finished)
            ):
                # 438: A change to keyless table '{}' committed
                # while its snapshot was reading the rows it
                # applies to, and reached Informix CDC only after
                # the snapshot was staged, so the change log may
                # hold it twice or not at all; run a full refresh
                raise InformixError(_m(438, table.exposed_name))
            return False

        kept = []
        checked = (*keys, *(moved["keys"] if relocated else ()), *table.primary_keys)
        # pylint: disable-next=too-many-nested-blocks
        for tx in transactions:
            if tx.commit_lsn > end:
                kept.append(tx)
                continue
            # The follower refuses such a change too, but one it never read (after an
            # unproven end, or through a shared shard) was placed by the value read.
            _refuse_altered_changes(tx.records, checked, table, self.options)
            # A shared shard hands each table the whole transaction, other tables' changes
            # included: those are the projection's to leave out. Read as this table's, a
            # row without its index columns failed, and one with them named a change the
            # manifest did not, for a full refresh.
            if any(_record_matches(part, table) for part in tx.spilled):
                # 433: The index-ordered snapshot does not support {} on '{}'; set
                # snapshot.scan=cursor to read the table without index order
                raise InformixError(
                    _m(433, "a transaction over cdc.spill.records during it", table.exposed_name)
                )

            records, halved = [], False
            for record in tx.records:
                if not _record_matches(record, table):
                    records.append(record)
                    continue
                old, new = record.get("before"), record.get("after")
                if None not in (old, new):
                    # An UPDATE that changed an index value took its row out of one key
                    # and put it into another, and the scan can hold either half without
                    # the other: the before-image is then named by its own position. The
                    # half the pages do not hold goes on alone, as the DELETE or the
                    # INSERT it is to that key.
                    same = _index_scan_key(old, keys) == _index_scan_key(new, keys)
                    gone = held(tx, old, _lsn(record) if same else _before_lsn(record))
                    come = held(tx, new, _lsn(record))
                    if (
                        gone != come
                        and table.primary_keys
                        and _index_scan_row_key(old, table, options)
                        == _index_scan_row_key(new, table, options)
                    ):
                        # One row by primary.keys cannot take a half. The scan reads
                        # such a row again, so this is an UPDATE its follower never got.
                        # 438: A change to keyless table '{}' committed
                        # while its snapshot was reading the rows it
                        # applies to, and reached Informix CDC only after
                        # the snapshot was staged, so the change log may
                        # hold it twice or not at all; run a full refresh
                        raise InformixError(_m(438, table.exposed_name))
                    if gone or come:
                        if not (gone and come):
                            half = {
                                name: value
                                for name, value in record.items()
                                if name not in {"before", "after", "before_lsn"}
                            }
                            if gone:
                                half.update(op="INSERT", row=new)
                            else:
                                half.update(op="DELETE", row=old, lsn=_before_lsn(record))
                            records.append(half)
                            halved = True
                        continue
                else:
                    row = new or record.get("row") or old
                    if row is not None and held(tx, row, _lsn(record)):
                        continue
                records.append(record)
            records = tuple(records)
            whole = len(records) == len(tx.records) and not halved
            kept.append(tx if whole else replace(tx, records=records))
        return kept

    # pylint: disable-next=too-many-locals,too-many-statements
    def _drop_copied_changes(self, table, copy, options, transactions):
        # Docstring: ``transactions`` without the changes an incremental copy that follows the
        # stream holds or will hold (see _copy_chunk), and what each window of its chunks has
        # judged before each of them and after the last.
        #
        # On the append path nothing merges what the copy and the stream both hold. A change
        # to a key the copy has yet to read is the chunk's that will read it, and one to a key
        # above the copy's bound the stream's alone. A chunk holds each change to its keys
        # that committed before its window and none that committed after it. Of those that
        # committed during it, it read each key once, so it holds the key's first changes in
        # commit order, up to the first whose image before or after is the key's staged row;
        # the rest are the stream's. What a window has judged rides in the offset, so a batch
        # can end inside one; a key the staged row fits none of fails it once it closes.

        keys = list(table.primary_keys)
        descending = (
            list(table.key_descending)
            if len(table.key_descending) == len(keys)
            else [False] * len(keys)
        )
        scope, schema_id = copy["exact"]

        def order(row):
            return _index_scan_order(_index_scan_key(row, keys), descending)

        def cursor(value):
            return order(dict(zip(keys, _decode_snapshot_stage_value(value))))

        top, at = cursor(copy["max_pk"]), copy["last_pk"] and cursor(copy["last_pk"])
        windows = [
            (
                window[0],
                window[1] and cursor(window[1]),
                window[2] and cursor(window[2]),
                *map(int, window[3:5]),
            )
            for window in copy["windows"]
        ]
        pages: dict[int, dict] = {}
        states = {window[0]: dict(window[5] if window[5:] else {}) for window in copy["windows"]}
        marks = [states]
        connector_columns = {CURSOR, COMMIT_LSN, TX_ID, OP, COMMIT_TIME}

        def same(image, row) -> bool:
            # Whether a change's image is a staged row, shaped alike, by the columns of both.
            if image is None or row is None:
                return image is row
            shaped = _framework_row(_coerce_variable_decimal_values(image, table, options))
            return all(
                shaped[name] == value
                for name, value in row.items()
                if name in shaped and name not in connector_columns
            )

        def dropped(tx, row, old, new) -> bool:
            # Whether the copy holds the change of ``row``'s key from ``old`` to ``new``.
            nonlocal states
            point = order(row)
            if point <= top and not copy["done"] and (at is None or point > at):
                return True
            for number, low, high, before, after in windows:
                if (low is None or point > low) and (high is None or point <= high):
                    break
            else:
                return False
            if not before < tx.commit_lsn <= after:
                return tx.commit_lsn <= before
            if number not in pages:
                # The bound's window (-1) holds no row.
                pages[number] = {
                    _index_scan_key(item, keys): item
                    for item in _decode_snapshot_stage_value(
                        self._read_snapshot_stage_page_record(
                            table, scope, schema_id, int(copy["boundary_lsn"]), number
                        )["rows"]
                        if number >= 0
                        else []
                    )
                }
            held = pages[number].get(_index_scan_key(row, keys))
            key = _index_scan_key_text(_index_scan_key(row, keys))
            if states is marks[-1]:
                states = {number: dict(state) for number, state in states.items()}
            state = states[number]
            if state.get(key) or same(old, held):
                state[key] = 1
                return False
            state[key] = int(same(new, held))
            return True

        kept = []
        for tx in transactions:
            if any(_record_matches(part, table) for part in tx.spilled) and (
                not copy["done"] or any(w[3] < tx.commit_lsn <= w[4] for w in windows)
            ):
                # 494: Informix incremental copy of '{}', which lands as a change log, cannot
                # judge a transaction over cdc.spill.records while the copy runs; raise
                # cdc.spill.records, or set snapshot.mode=initial and run a full refresh
                raise InformixError(_m(494, table.exposed_name))
            _refuse_altered_changes(tx.records, keys, table, self.options)
            records, changed = [], False
            for record in tx.records:
                op = _operation(record)
                old = record.get("before", record.get("row") if op == "DELETE" else None)
                new = record.get("after", record.get("row") if op == "INSERT" else None)
                if not _record_matches(record, table) or old is new is None:
                    records.append(record)
                    continue
                if None in (old, new) or _index_scan_key(old, keys) == _index_scan_key(new, keys):
                    if dropped(tx, old or new, old, new):
                        changed = True
                    else:
                        records.append(record)
                    continue
                # A key-changing UPDATE: each half by its own key, as the DELETE or the INSERT
                # it is to that key.
                gone, come = dropped(tx, old, old, None), dropped(tx, new, None, new)
                if gone or come:
                    changed = True
                    if not (gone and come):
                        half = {
                            name: value
                            for name, value in record.items()
                            if name not in {"before", "after", "before_lsn"}
                        }
                        if gone:
                            half.update(op="INSERT", row=new)
                        else:
                            half.update(op="DELETE", row=old, lsn=_before_lsn(record))
                        records.append(half)
                    continue
                records.append(record)
            kept.append(replace(tx, records=tuple(records)) if changed else tx)
            marks.append(states)
        return kept, marks

    @staticmethod
    def _validate_unl_source_options(options: dict[str, str]) -> None:
        # Docstring: Eagerly validate the connection-level `.unl` source options (per-table too).

        if _SNAPSHOT_SOURCE_OPTION in options:
            if not str(options[_SNAPSHOT_SOURCE_OPTION]).strip():
                # 257: Option '{}' must be a non-empty path
                raise ValueError(_m(257, _SNAPSHOT_SOURCE_OPTION))
            _validated_volume_location(
                str(options[_SNAPSHOT_SOURCE_OPTION]).strip(), _SNAPSHOT_SOURCE_OPTION, options
            )
        if _SNAPSHOT_BOUNDARY_LSN_OPTION in options:
            raw = str(options[_SNAPSHOT_BOUNDARY_LSN_OPTION]).strip()
            if not raw or not raw.isdigit():
                # 258: Option '{}' must be a non-negative integer
                raise ValueError(_m(258, _SNAPSHOT_BOUNDARY_LSN_OPTION))

    def _table_snapshot_source(self, table_options: dict[str, str]) -> str | None:
        # Docstring: The table's ``snapshot.source`` Volume path (table option, then connection).

        value = table_options.get(_SNAPSHOT_SOURCE_OPTION) or self.options.get(
            _SNAPSHOT_SOURCE_OPTION
        )
        value = str(value).strip() if value is not None else ""
        if not value:
            return None
        # A pipeline author sets this per table. Held to the staging location's rule (an
        # absolute Volume path with no traversal), so that it cannot name a file of the
        # compute's own, or step out of the Files API's path into another workspace API.
        return _validated_volume_location(value, _SNAPSHOT_SOURCE_OPTION, self.options)

    def _source_utc_time(self) -> int | None:
        # Docstring: Best-effort source-server UTC epoch seconds for a snapshot row's commit time.
        #
        # Captured at the snapshot's boundary/LSN capture so ``r`` rows carry the snapshot's
        # as-of time (``<=`` every later CDC commit time, keeping the column monotonic across
        # the snapshot->CDC handoff). Returns None -- a NULL commit time -- if the bridge does
        # not implement it or the source cannot answer, so a snapshot never fails over a time.

        getter = getattr(self._bridge, "current_utc_time", None)
        if getter is None:
            return None
        try:
            value = getter()  # pylint: disable=not-callable
        except ConnectionCapacityUnavailable:
            # Not a time the source could not tell: the read has no slot, or lost its lease.
            # Taken for None, the loss was cleared unseen, or the read then waited the whole
            # connection wait a second time.
            raise
        except Exception:
            return None
        return int(value) if value is not None else None

    def _unl_field_format(self, table_options: dict[str, str]) -> tuple[str, str]:
        # Docstring: The ``(delimiter, escape)`` for a `.unl`/HPL DELIMITED serve.
        #
        # Resolved table option, then connection, then the UNLOAD defaults (``|``/backslash),
        # so a plain UNLOAD extract needs no options and an HPL extract sets only what differs.
        # Both are validated to a single character (the escape ``none`` = escaping disabled),
        # and the two must differ.

        delimiter = _unl_char(
            table_options.get(
                _UNL_DELIMITER_OPTION,
                self.options.get(_UNL_DELIMITER_OPTION, _UNL_DEFAULT_DELIMITER),
            ),
            _UNL_DELIMITER_OPTION,
            allow_empty=False,
        )
        escape = _unl_char(
            table_options.get(
                _UNL_ESCAPE_OPTION,
                self.options.get(_UNL_ESCAPE_OPTION, _UNL_DEFAULT_ESCAPE),
            ),
            _UNL_ESCAPE_OPTION,
            allow_empty=True,
        )
        if escape and escape == delimiter:
            # 259: Options '{}' and '{}' must differ
            raise ValueError(_m(259, _UNL_DELIMITER_OPTION, _UNL_ESCAPE_OPTION))
        return delimiter, escape

    def _unl_snapshot_serve(
        self,
        table_options: dict[str, str],
        effective_start: dict,
        table_name: str | None = None,
        table: Table | None = None,
    ) -> bool:
        # Docstring: Whether this read belongs to a file-source initial snapshot's split serve.
        #
        # The single gate the partitioned ``latest_offset``/``get_partitions`` branches
        # share: a non-delete flow, ``snapshot.source`` set, and ``snapshot.mode`` initial or
        # initial_only (the modes that route to the file serve; initial_only, a keyed table's
        # alone, which registration and _pin_unl_boundary refuse an append-only table, stops
        # at the boundary as its live snapshot does). It fires
        # on the first read (no committed offset) and on every continuation microbatch (a
        # snapshot-phase offset), so the serve fans out ``stream.partitioned.snapshot.pages.
        # per.batch`` splits at a time until the boundary offset transitions to the stream
        # phase; after that ``effective_start`` is a stream offset and every later read
        # takes the ordinary CDC path. A ``.unl`` table never stages pages, so a
        # snapshot-phase offset here is always a ``.unl`` split window (never a staged page).

        if self._is_delete_flow() or self._table_migration(table_options):
            # A cutover seeds from the parked token, never from the extract.
            return False
        if self._table_snapshot_source(table_options) is None:
            return False
        if not effective_start:
            if (
                table is None
                and table_name is not None
                and not self._snapshot_mode_set(table_options)
            ):
                # With no mode set, an append-only table takes initial: the table says which.
                with self._metadata_access():
                    table = self._table(table_name, table_options)
            if table is not None:
                return self._table_snapshot_mode(table, table_options) in _UNL_SNAPSHOT_MODES
            return self._snapshot_mode(table_options) in _UNL_SNAPSHOT_MODES
        # A serve part-way through finishes as a staged one does, whatever snapshot.mode
        # says now: the mode chooses how a snapshot starts. A staged snapshot's offset is
        # no split window: one a connection-scope extract was set or added beside (another
        # mode's, or a fallback's) took its next page for a split, and lost the pages left
        # or served the extract's rows into the table.
        return effective_start.get("phase") == "snapshot" and not (
            table_name is not None
            and self._staged_snapshot_offset(table_name, table_options, effective_start)
        )

    def _staged_snapshot_offset(
        self, table_name: str, table_options: dict[str, str], offset: dict
    ) -> bool:
        # Docstring: Whether a snapshot-phase offset is a staged snapshot's, told by the page
        # manifest the snapshot publishes before it serves its first page; read without
        # opening Informix (see _delete_floor_recorded).

        identity = self._native_table_state_identity(
            table_options.get("qualified_source_table", table_name)
        )
        if identity is None:
            return False
        checkpoint = _validated_offset(offset)
        key = "/".join(
            (
                identity[1],
                identity[2],
                "snapshot-page-manifest",
                self._offset_scopes(checkpoint)[1],
                str(checkpoint["schema_id"]),
            )
        )
        cache = self.__dict__.setdefault("_staged_snapshot_offsets", {})
        if key not in cache:
            cache[key] = self._read_immutable_head(key) is not None
        return cache[key]

    def _list_unl_files(self, source: str, exposed_name: str) -> list[str]:
        # Docstring: Sorted `.unl`/`.unl.gz` files under ``source`` (listing/stat only -- no content
        # read).
        #
        # Both plain and gzip-compressed extracts are listed; whether a given file is gzip is
        # decided per file by its magic bytes at read time (see :func:`_unl_file_is_gzip`), not
        # by its name, so a mislabelled file is still handled correctly.

        try:
            names = sorted(
                name for name in os.listdir(source) if name.endswith((".unl", ".unl.gz"))
            )
        except OSError as error:
            if _dropped_mount_error(error) is not None:
                raise  # unwrapped, so a read's dropped-mount retry sees it
            # 260: snapshot.source '{}' for '{}' is not readable
            raise InformixError(_m(260, source, exposed_name)) from error
        files = [
            os.path.join(source, name) for name in names if _is_path(os.path.join(source, name))
        ]
        if not files:
            # 261: snapshot.source '{}' for '{}' contains no .unl files
            raise InformixError(_m(261, source, exposed_name))
        return files

    def _unl_read_chunk(self, path: str) -> Callable[[int, int], bytes]:
        # Docstring: A ``(offset, length) -> bytes`` range reader for one `.unl` file.
        #
        # Prefers the Files REST API (ranged GET, 206) when a staging token can be
        # minted -- the whole point, since serverless FUSE caches an entire file to
        # local disk on first access, fatal for a large `.unl`. Falls back to a local
        # seek/read when REST is unavailable (offline tests, or a workspace that forbids
        # token minting), mirroring ``snapshot.staging.transport=auto`` for writes, and
        # after a 401 nothing could recover. The transport is consulted on every read, so
        # once it has moved to FUSE the reads still to come go there directly.

        def local(offset: int, length: int) -> bytes:
            with open(path, "rb") as handle:
                handle.seek(offset)
                return handle.read(length)

        def read(offset: int, length: int) -> bytes:
            files_api = self._staging_transport()
            if files_api is not None:
                try:
                    return files_api.get_range(path, offset, offset + length)
                except WorkspaceHttpError as error:
                    if not self._staging_fell_back(error):
                        raise
            return local(offset, length)

        return read
