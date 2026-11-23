"""Snapshots from .unl extracts (boundaries, layouts, gzip windows, splits), and append-only and
snapshot-only reads.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import logging
import os
import re
import time
from typing import Any, Iterable, Iterator

from databricks.labs.community_connector.sources.informix.informix_filters import (
    _parse_row_filter,
    _row_filter_kind,
    _snapshot_filter,
    _snapshot_predicate,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _capture_descriptor,
    _client_encoding,
    _database_encoding,
    _ensure_materializable,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _STRING_KINDS,
    _null_byte_mode,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _handoff_position,
    _unl_position,
    _validated_offset,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CDC_RECONNECT_BASE_SECONDS,
    _CDC_RECONNECT_MAX_RETRIES,
    _CDC_RECONNECT_MAX_SECONDS,
    _HANDOFF_CHANNEL_DELETE,
    _HANDOFF_KEYED,
    _REPLAY_INITIAL_END_OPTION,
    _SNAPSHOT_BOUNDARY_LSN_OPTION,
    _SNAPSHOT_MANIFEST_NAME,
    _SNAPSHOT_SOURCE_OPTION,
    _UNL_DEFAULT_DELIMITER,
    _UNL_DEFAULT_ESCAPE,
    _UNL_DELIMITER_OPTION,
    _UNL_ESCAPE_OPTION,
    _UNL_FINGERPRINT_SAMPLE_BYTES,
    _UNL_GZIP_RECORDS_OPTION,
    _UNL_MTIME_TOLERANCE_SECONDS,
    _UNL_SPLIT_BYTES_OPTION,
    _UNL_SPLIT_TARGET_BYTES,
    InformixError,
    _m,
    _split_identity,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    Column,
    ConnectionCapacityUnavailable,
    Table,
    _replay_keeps_floor,
)
from databricks.labs.community_connector.sources.informix.informix_shards import _schema_fingerprint
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _filter_column_names,
    _row_filter_matches,
    _row_filter_predicate,
    _strict_lsn,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _iter_gzip_unl_records,
    _iter_unl_records,
    _offset,
    _partition_column,
    _schema_state,
    _shape_snapshot,
    _shape_unl_record,
    _table_from_schema_state,
    _unl_byte_splits,
    _unl_escape_byte,
    _unl_file_is_gzip,
    _unl_split_read_chunk,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    WorkspaceHttpError,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    SqliProtocolError,
    reconnectable_error,
)


class _UnlMixin:
    # Docstring: Snapshots from .unl extracts (boundaries, layouts, gzip windows, splits), and
    # append-only and snapshot-only reads.

    def _resolve_unl_boundary_lsn(self, table_options: dict[str, str], source: str) -> int | None:
        # Docstring: Resolve the operator-supplied boundary LSN, or None to self-pin.
        #
        # Order (first hit wins): the ``snapshot.boundary.lsn`` table option (table
        # options first, connection fallback -- same shape as :meth:`_table_snapshot_source`),
        # then ``_manifest.json`` in the source directory (``{"boundary_lsn": "<X>"}``).
        # Fails closed on a non-integer value in either place.

        raw = table_options.get(_SNAPSHOT_BOUNDARY_LSN_OPTION) or self.options.get(
            _SNAPSHOT_BOUNDARY_LSN_OPTION
        )
        if raw is not None and str(raw).strip():
            try:
                return _strict_lsn(str(raw).strip(), _SNAPSHOT_BOUNDARY_LSN_OPTION)
            except ValueError as error:
                raise InformixError(str(error)) from error
        manifest = self._read_unl_manifest(source)
        if manifest is None or manifest.get("boundary_lsn") is None:
            return None
        try:
            return _strict_lsn(manifest["boundary_lsn"], f"{_SNAPSHOT_MANIFEST_NAME} boundary_lsn")
        except ValueError as error:
            raise InformixError(str(error)) from error

    def _resolve_unl_commit_time(self, source: str) -> int | None:
        # Docstring: The `.unl` extract's snapshot commit time (UTC epoch seconds) for its ``r``
        # rows.
        #
        # Read from the optional ``_manifest.json`` ``boundary_time`` the extract records
        # beside ``boundary_lsn`` -- the wall-clock instant the extract reflects. A file-source
        # snapshot has no live boundary to query, so absent a manifest time these rows carry
        # NULL rather than a fabricated time (which would break the monotonic-handoff
        # guarantee that snapshot commit time is ``<=`` every later CDC commit time). Fails
        # closed on a present-but-invalid value.

        manifest = self._read_unl_manifest(source)
        if manifest is None or manifest.get("boundary_time") is None:
            return None
        try:
            value = int(manifest["boundary_time"])
        except (TypeError, ValueError) as error:
            # 262: snapshot.source '{}' has a non-integer {} boundary_time
            raise InformixError(_m(262, source, _SNAPSHOT_MANIFEST_NAME)) from error
        if value < 0:
            # 263: snapshot.source '{}' has a negative {} boundary_time
            raise InformixError(_m(263, source, _SNAPSHOT_MANIFEST_NAME))
        return value

    def _read_unl_manifest(self, source: str) -> dict | None:
        # Docstring: Read ``<source>/_manifest.json`` (small file, whole), or None if absent.
        #
        # Uses the Files REST API when a staging token is available (a 404 means absent),
        # else, or after a 401 nothing could recover, a local read (a missing file means
        # absent). A present-but-malformed manifest fails closed.

        path = os.path.join(source, _SNAPSHOT_MANIFEST_NAME)
        files_api = self._staging_transport()
        data = None
        try:
            if files_api is not None:
                try:
                    data = files_api.get(path)
                except WorkspaceHttpError as error:
                    if not self._staging_fell_back(error):
                        raise
            if data is None:
                with open(path, "rb") as handle:
                    data = handle.read()
        except FileNotFoundError:
            return None
        except WorkspaceHttpError as error:
            if getattr(error, "status_code", None) == 404:
                return None
            raise
        try:
            parsed = json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as error:
            # 264: snapshot.source '{}' has an unreadable {}
            raise InformixError(_m(264, source, _SNAPSHOT_MANIFEST_NAME)) from error
        return parsed if isinstance(parsed, dict) else None

    def _publish_unl_initialization(self, table: Table, scope: str, boundary: int) -> str:
        # Docstring: Publish the durable initialization record at an operator-supplied ``boundary``.
        #
        # A variant of the owner path in :meth:`_shared_table_lsn` that pins ``initial_lsn``
        # to the operator's LSN (X) rather than the connector's freshly-captured
        # registration LSN -- X is authoritative, so the schema node's ``start_lsn`` and
        # the init record's ``initial_lsn`` are both X. Full-row logging is still enabled
        # (idempotent) and the boundary validated at X. Election makes it idempotent: a
        # re-call with the same X adopts the same winner; a table already initialized at a
        # different LSN loses the election and fails closed (run a full refresh).

        capture = _capture_descriptor(table, _database_encoding(self.options))
        self._bridge.prepare_initial_capture([table.native_identity])
        self._bridge.validate_initial_lsn(capture, boundary)
        node = _schema_state(table, boundary)
        winner = self._publish_immutable_head(
            self._immutable_namespace(table, "initialization", scope),
            {
                "created_at": time.time(),
                "initial_lsn": str(boundary),
                "schema": node,
                "scope": scope,
                "table": table.native_identity,
            },
            record_type="initialization",
        )
        self._validate_immutable_record_header(winner, "initialization", table.exposed_name)
        if (
            self._immutable_lsn(winner, "initial_lsn", table.exposed_name) != boundary
            or winner.get("scope") != scope
            or winner.get("table") != table.native_identity
        ):
            # 265: Conflicting Informix initialization boundary for '{}': the table was already
            # initialized at a different LSN; run a full refresh
            raise InformixError(_m(265, table.exposed_name))
        schema = winner.get("schema")
        if not isinstance(schema, dict) or not re.fullmatch(r"[0-9a-f]{32}", str(schema.get("id"))):
            # 316: Invalid Informix immutable initialization schema
            raise InformixError(_m(316))
        if (
            schema.get("fingerprint") != _schema_fingerprint(table)
            or schema.get("predecessor") is not None
            or self._immutable_lsn(schema, "start_lsn", table.exposed_name) != boundary
            or _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
        ):
            # 316: Invalid Informix immutable initialization schema
            raise InformixError(_m(316))
        # The node every later schema step and delete seed starts from, as a self-pinned
        # initialization records it.
        schema_winner = self._publish_immutable_head(
            self._immutable_namespace(table, "schema-nodes", str(schema["id"])),
            {"created_at": winner.get("created_at", time.time()), "schema": schema},
            record_type="schema-node",
        )
        self._validate_schema_node_winner(schema_winner, schema, table)
        return str(schema["id"])

    def _landed_predicate(self, table: Table) -> tuple | None:
        # Docstring: row_filter as the stream and a file snapshot apply it, to the value that
        # lands, for an SQL snapshot's rows; None where Informix's own filter is that.
        #
        # Informix filters a string by what it holds, and under string.null.byte=null or
        # empty one holding a NUL lands as NULL or without it: a row the snapshot took that
        # its filter then leaves out, whose changes the stream dropped (an orphan in the
        # target). Informix SQL cannot find a NUL (CHR(0) ends a string there,
        # live-checked), so a row it left out stays out until the stream's next change.

        if table.row_filter is None or _null_byte_mode(self.options) == "keep":
            return None
        strings = {c.name for c in table.columns if _row_filter_kind(c) in _STRING_KINDS}
        if not strings.intersection(_filter_column_names(_parse_row_filter(table.row_filter))):
            return None
        return _row_filter_predicate(table, _database_encoding(self.options))

    def _landed_rows(self, table: Table, rows: list) -> list:
        # Docstring: ``rows`` of an SQL snapshot read that pass _landed_predicate.

        predicate = self._landed_predicate(table)
        if predicate is None:
            return rows
        return [row for row in rows if _row_filter_matches(predicate, row)]

    def _pin_unl_boundary(
        self, table: Table, options: dict[str, str], pipeline_scope: str
    ) -> tuple[Table, str, int]:
        # Docstring: Pin and durably publish the file-source snapshot boundary; idempotent.
        #
        # With an operator-supplied boundary (``snapshot.boundary.lsn`` or ``_manifest.json``,
        # see :meth:`_resolve_unl_boundary_lsn`) the LSN X is authoritative: guard
        # ``minimum_lsn() <= X <= current_lsn()`` (below min = the logical log recycled past
        # X -> gap; above current = bogus), then publish the initialization + snapshot
        # boundary at ``initial_lsn == snapshot_lsn == X`` so the CDC stream and the
        # delete-channel bootstrap both start at X. Otherwise self-pin via ``_initial_lsn``
        # (the connector's registration LSN, cached -> stable across re-calls). Either way
        # the durable records are elected/immutable, so a speculative ``latest_offset``
        # re-call is idempotent.

        if self._snapshot_mode(options) == "initial_only" and self._append_only_table(
            table, options
        ):
            # As registration refuses it: read directly, the partitioned reader served the
            # extract and then streamed the log.
            # 274: Append-only table '{}' supports only snapshot.mode=initial or
            # snapshot.mode=cdc_only, or snapshot.mode=incremental when it has a key
            raise ValueError(_m(274, table.exposed_name))
        boundary = self._resolve_unl_boundary_lsn(options, self._table_snapshot_source(options))
        if boundary is None:
            high_water = self._initial_lsn(table, scope=pipeline_scope)
            schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
            table = self._refresh_table_schema(table, _schema_fingerprint(table))
            self._publish_snapshot_boundary(
                table,
                schema_id,
                high_water,
                high_water,
                pipeline_scope,
                floor=not _replay_keeps_floor(options),
            )
            self._pin_unl_extract(table, options, pipeline_scope, schema_id, high_water)
            return table, schema_id, high_water
        minimum = self._bridge.minimum_lsn()
        current = self._bridge.current_lsn()
        # initial_only reads no log after the extract: a boundary the log has recycled past
        # leaves it no gap.
        if boundary > current or (
            boundary < minimum and self._snapshot_mode(options) != "initial_only"
        ):
            # 266: {}={} for '{}' is outside the retained/current log range [{}, {}]: below the
            # minimum means the logical log recycled past it (gap); above current is invalid.
            # Re-extract at a retained position.
            raise InformixError(
                _m(
                    266,
                    # Named where it was given: the manifest's, with no option set.
                    _SNAPSHOT_BOUNDARY_LSN_OPTION
                    if options.get(_SNAPSHOT_BOUNDARY_LSN_OPTION)
                    or self.options.get(_SNAPSHOT_BOUNDARY_LSN_OPTION)
                    else f"{_SNAPSHOT_MANIFEST_NAME} boundary_lsn",
                    boundary,
                    table.exposed_name,
                    minimum,
                    current,
                )
            )
        table = self._refresh_table_schema(table, _schema_fingerprint(table))
        schema_id = self._publish_unl_initialization(table, pipeline_scope, boundary)
        self._publish_snapshot_boundary(
            table,
            schema_id,
            boundary,
            boundary,
            pipeline_scope,
            floor=not _replay_keeps_floor(options),
        )
        self._pin_unl_extract(table, options, pipeline_scope, schema_id, boundary)
        return table, schema_id, boundary

    def _replayed_unl_boundary(
        self, table: Table, options: dict[str, str]
    ) -> tuple[Table, str, int, str] | None:
        # Docstring: Where a replayed first batch of a file snapshot was served, as (table,
        # schema id, LSN, scope), else None.
        #
        # A self-pinned boundary pinned again by the update replaying the batch was a newer
        # LSN than the end Spark goes on from: the extract's rows were stamped with it, so
        # the changes up to it lost to them on the sequence column, and the pipeline's
        # delete floor moved past deletes the target lacks. The partitioned reader serves
        # such a batch again from the pin it logged (_served_partitions); so does this,
        # from the manifest pinned at the end's LSN, against which the extract is checked.

        raw = options.get(_REPLAY_INITIAL_END_OPTION)
        if raw is None:
            return None
        try:
            end = _validated_offset(json.loads(raw))
        except (TypeError, ValueError):
            return None
        lsn = int(end["commit_lsn"])
        if (
            end["phase"] != "stream"
            or {int(end["begin_lsn"]), int(end["change_lsn"])} != {lsn}
            or end.get("tx_id") is not None
            or "incremental" in end
        ):
            return None
        scope, schema_id = str(end["pipeline_scope"]), str(end["schema_id"])
        record = self._read_immutable_head(self._unl_extract_namespace(table, scope, schema_id))
        if record is None or str(record.get("snapshot_lsn")) != str(lsn):
            return None
        self._validate_immutable_record_header(record, "unl-extract", table.exposed_name)
        table = self._refresh_table_schema(table, end.get("schema_fingerprint"))
        paths = [entry["path"] for entry in self._unl_extract_files(table, options)]
        self._verify_unl_extract(table, options, scope, schema_id, lsn, paths)
        return table, schema_id, lsn, scope

    def _unl_extract_namespace(self, table: Table, pipeline_scope: str, schema_id: str) -> str:
        return self._immutable_namespace(table, "unl-extract", pipeline_scope, schema_id)

    def _unl_extract_files(self, table: Table, table_options: dict[str, str]) -> list[dict]:
        # Docstring: The extract's files as they are now: name, path, size and modification time.

        source = self._table_snapshot_source(table_options)
        entries = []
        for path in self._list_unl_files(source, table.exposed_name):
            info = os.stat(path)
            entries.append(
                {
                    "name": os.path.basename(path),
                    "path": path,
                    "size": int(info.st_size),
                    "mtime": float(info.st_mtime),
                    "mtime_ns": int(info.st_mtime_ns),
                }
            )
        return entries

    def _unl_fingerprint(self, entry: dict) -> str:
        # Docstring: SHA-256 of a file's size with its first and last sample bytes; cached per
        # version.

        key = (entry["path"], entry["size"], entry["mtime_ns"])
        cached = self._unl_fingerprint_cache.get(key)
        if cached is not None:
            return cached
        size = entry["size"]
        sample = _UNL_FINGERPRINT_SAMPLE_BYTES
        digest = hashlib.sha256(f"{size}\0".encode())
        if size:
            read_chunk = self._unl_read_chunk(entry["path"])
            if size <= 2 * sample:
                digest.update(read_chunk(0, size))
            else:
                digest.update(read_chunk(0, sample))
                digest.update(read_chunk(size - sample, sample))
        value = digest.hexdigest()
        self._unl_fingerprint_cache[key] = value
        return value

    def _pin_unl_extract(
        self,
        table: Table,
        table_options: dict[str, str],
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
    ) -> None:
        # Docstring: Pin the extract's manifest with the snapshot boundary; idempotent.
        #
        # The record is elected, so a retried first read compares the directory with the
        # manifest the snapshot actually started from rather than re-pinning it.

        entries = self._unl_extract_files(table, table_options)
        winner = self._publish_immutable_head(
            self._unl_extract_namespace(table, pipeline_scope, schema_id),
            {
                "created_at": time.time(),
                "scope": pipeline_scope,
                "schema_id": schema_id,
                "snapshot_lsn": str(snapshot_lsn),
                "layout": self._unl_layout(table_options),
                "files": [
                    {
                        "name": entry["name"],
                        "size": entry["size"],
                        "mtime": entry["mtime"],
                        "fingerprint": self._unl_fingerprint(entry),
                    }
                    for entry in entries
                ],
            },
            record_type="unl-extract",
        )
        self._validate_immutable_record_header(winner, "unl-extract", table.exposed_name)
        self._compare_unl_extract(table, winner, entries, fingerprint_paths=None)
        self._compare_unl_layout(table, winner, table_options)

    def _unl_layout(self, table_options: dict[str, str]) -> dict[str, Any]:
        # Docstring: The options a file snapshot's position depends on, as the serve takes them.
        #
        # A position is a split index into the plan snapshot.split.bytes and the escape give,
        # and a record index counted by the delimiter and escape. Pinned with the manifest.

        delimiter, escape = self._unl_field_format(table_options)
        split_bytes = self._table_int_option(
            table_options, _UNL_SPLIT_BYTES_OPTION, _UNL_SPLIT_TARGET_BYTES, minimum=1
        )
        return {
            _UNL_SPLIT_BYTES_OPTION: split_bytes,
            _UNL_DELIMITER_OPTION: delimiter,
            _UNL_ESCAPE_OPTION: escape,
        }

    def _compare_unl_layout(self, table: Table, record: dict, table_options: dict[str, str]):
        # Docstring: Raise if an option the snapshot's position depends on changed since it was
        # pinned. A manifest pinned before these were recorded names none.

        pinned = record.get("layout")
        if not isinstance(pinned, dict):
            return
        for name, value in self._unl_layout(table_options).items():
            if name in pinned and pinned[name] != value:
                # 490: Option '{}' for '{}' changed from {!r} to {!r} after its file snapshot
                # started, and the snapshot's position is a place in the split plan or the
                # records the old value gave: resuming would skip or repeat rows. Set it back
                # until the snapshot is done, or run a full refresh
                raise InformixError(_m(490, name, table.exposed_name, pinned[name], value))

    def _verify_unl_extract(
        self,
        table: Table,
        table_options: dict[str, str],
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        read_paths: Iterable[str],
    ) -> None:
        # Docstring: Fail closed when the extract no longer matches the manifest its snapshot
        # pinned.
        #
        # Checks every file's name, size and modification time, and the fingerprint of each
        # file in ``read_paths`` -- the ones this read will serve. A snapshot that began
        # before manifests existed pins one now, which guards the rest of its serve.

        record = self._read_immutable_head(
            self._unl_extract_namespace(table, pipeline_scope, schema_id)
        )
        if record is None:
            # 267: Informix file snapshot for '{}' has no pinned extract manifest; pinning the
            # extract as it is now
            logging.getLogger(__name__).info(
                _m(267, table.exposed_name),
            )
            self._pin_unl_extract(table, table_options, pipeline_scope, schema_id, snapshot_lsn)
            return
        self._validate_immutable_record_header(record, "unl-extract", table.exposed_name)
        self._compare_unl_layout(table, record, table_options)
        entries = self._unl_extract_files(table, table_options)
        self._compare_unl_extract(table, record, entries, fingerprint_paths=set(read_paths))

    def _compare_unl_extract(
        self,
        table: Table,
        record: dict,
        entries: list[dict],
        *,
        fingerprint_paths: set[str] | None,
    ) -> None:
        # Docstring: Raise if ``entries`` differ from the pinned manifest ``record``.
        #
        # ``fingerprint_paths`` limits the content check to those files; None checks all.

        pinned = record.get("files")
        if not isinstance(pinned, list) or not all(isinstance(item, dict) for item in pinned):
            # 268: Invalid pinned extract manifest for '{}'; run a full refresh
            raise InformixError(_m(268, table.exposed_name))
        pinned_names = [str(item.get("name")) for item in pinned]
        current_names = [entry["name"] for entry in entries]
        problem = None
        if pinned_names != current_names:
            added = sorted(set(current_names) - set(pinned_names))
            removed = sorted(set(pinned_names) - set(current_names))
            problem = "; ".join(
                part
                for part in (
                    f"added {', '.join(added)}" if added else "",
                    f"removed {', '.join(removed)}" if removed else "",
                )
                if part
            )
        else:
            for item, entry in zip(pinned, entries):
                name = entry["name"]
                if int(item.get("size", -1)) != entry["size"]:
                    problem = (
                        f"{name} changed size from {item.get('size')} to {entry['size']} bytes"
                    )
                elif (
                    abs(float(item.get("mtime", 0.0)) - entry["mtime"])
                    > _UNL_MTIME_TOLERANCE_SECONDS
                ):
                    problem = f"{name} has a different modification time"
                elif (
                    fingerprint_paths is None or entry["path"] in fingerprint_paths
                ) and self._unl_fingerprint(entry) != item.get("fingerprint"):
                    problem = f"{name} has different content"
                if problem:
                    break
        if problem:
            # 269: The {} extract for '{}' changed after its snapshot started ({}), so resuming
            # would skip or repeat rows. Run a full refresh to ingest the current extract, or
            # restore the original files with their modification times.
            raise InformixError(_m(269, _SNAPSHOT_SOURCE_OPTION, table.exposed_name, problem))

    def _unl_snapshot_offset(
        self, table: Table, table_options: dict[str, str], effective_start: dict
    ) -> tuple[dict, list[dict[str, Any]] | None]:
        # Docstring: End offset for one windowed ``.unl`` snapshot microbatch, plus any driver rows.
        #
        # On the first read (empty start) the boundary is pinned durably and idempotently
        # (see :meth:`_pin_unl_boundary`) and the serve begins at split 0; on a continuation
        # the boundary/schema come from the checkpoint and the serve resumes at the recorded
        # split index (and, inside a gzip file served in record windows, its record index).
        # An executor window advances by up to ``stream.partitioned.snapshot.pages.per.batch``
        # splits, stopping early before a gzip split that ``snapshot.gzip.records.per.batch``
        # serves in record windows. Such a window is read here on the driver -- its rows are
        # returned so latest_offset can cache them -- and advances by up to that many records,
        # moving to the next split once the file is exhausted. A non-terminal microbatch
        # returns a snapshot-phase offset (so the serve is resumable mid-snapshot), and the
        # microbatch that finishes the last split transitions to the stream phase at the
        # boundary -- where the CDC stream resumes and ``_recover`` discards everything at or
        # below it. The schema fingerprint is carried forward from the checkpoint on a
        # continuation (the extract is frozen as of the boundary; drift is still caught at the
        # snapshot->CDC transition by ``_read_stream``), matching the staged-page serve. An
        # empty extract transitions to the stream phase immediately.

        if not effective_start:
            pipeline_scope = pin_scope = self._pipeline_scope()
            table, schema_id, snapshot_lsn = self._pin_unl_boundary(
                table, table_options, pipeline_scope
            )
            start_split, start_record = 0, 0
            expected_fingerprint = None
        else:
            checkpoint = _validated_offset(effective_start)
            # The serve keeps the scopes it started under, as a staged one does: its extract
            # pin lives there, and in a later update the registration scope has none, which
            # would re-pin whatever files are there now instead of failing on a change.
            pipeline_scope, pin_scope = self._offset_scopes(checkpoint)
            schema_id = str(checkpoint["schema_id"])
            snapshot_lsn = int(checkpoint["snapshot_lsn"])
            start_split, start_record = _unl_position(checkpoint)
            expected_fingerprint = checkpoint.get("schema_fingerprint")
        splits = self._unl_all_splits(table, table_options)
        total = len(splits)
        window_rows: list[dict[str, Any]] | None = None
        driver_window = self._unl_gzip_driver_window(
            table_options, splits, start_split, start_record
        )
        if effective_start:
            # The first read just pinned the manifest; a continuation checks the extract
            # still matches it before serving anything more.
            self._verify_unl_extract(
                table,
                table_options,
                pin_scope,
                schema_id,
                snapshot_lsn,
                self._unl_window_paths(table_options, splits, start_split, driver_window),
            )
        if driver_window:
            limit = self._table_int_option(table_options, _UNL_GZIP_RECORDS_OPTION, 0, minimum=0)
            window_rows, exhausted, read = self._read_unl_gzip_window(
                table,
                table_options,
                snapshot_lsn,
                splits[start_split],
                start_record,
                limit or None,
            )
            if exhausted:
                end_split, end_record = start_split + 1, 0
            else:
                end_split, end_record = start_split, start_record + read
        else:
            end_split = self._unl_executor_window_end(table_options, splits, start_split)
            end_record = 0
        if end_split >= total:
            # Last window (or empty extract): the snapshot is complete -> transition to the
            # stream phase at the boundary, an ordinary stream offset from here on.
            end = _offset(
                snapshot_lsn,
                snapshot_lsn,
                snapshot_lsn,
                None,
                "stream",
                table,
                schema_id,
                pipeline_scope,
            )
            if expected_fingerprint is not None:
                end["schema_fingerprint"] = expected_fingerprint
            return end, window_rows
        end = _offset(
            snapshot_lsn,
            snapshot_lsn,
            snapshot_lsn,
            None,
            "snapshot",
            table,
            schema_id,
            pipeline_scope,
        )
        if expected_fingerprint is not None:
            end["schema_fingerprint"] = expected_fingerprint
        # No PK cursor for byte splits: page_index is the split index; last_pk is an unused
        # placeholder that satisfies the snapshot-offset schema. record_index, present only
        # inside a gzip file served in record windows, counts that file's records done.
        snapshot: dict[str, Any] = {"last_pk": [], "page_index": end_split}
        if end_record:
            snapshot["record_index"] = end_record
        end.update({"snapshot_lsn": str(snapshot_lsn), "snapshot": snapshot})
        if pin_scope != pipeline_scope:
            end["stage_scope"] = pin_scope
        return end, window_rows

    def _unl_executor_window_end(
        self,
        table_options: dict[str, str],
        splits: list[tuple[str, int, int, bool]],
        start_split: int,
    ) -> int:
        # Docstring: End split of an executor window: up to pages-per-batch splits, stopping before
        # a
        # gzip split that ``snapshot.gzip.records.per.batch`` serves on the driver.

        end_split = min(start_split + self._partitioned_pages_per_batch(), len(splits))
        if self._table_int_option(table_options, _UNL_GZIP_RECORDS_OPTION, 0, minimum=0):
            end_split = next(
                (index for index in range(start_split + 1, end_split) if splits[index][3]),
                end_split,
            )
        return end_split

    def _unl_window_paths(
        self,
        table_options: dict[str, str],
        splits: list[tuple[str, int, int, bool]],
        start_split: int,
        driver_window: bool,
    ) -> set[str]:
        # Docstring: The files the window starting at ``start_split`` will read.

        if start_split >= len(splits):
            return set()
        if driver_window:
            return {splits[start_split][0]}
        end_split = self._unl_executor_window_end(table_options, splits, start_split)
        return {split[0] for split in splits[start_split:end_split]}

    def _unl_gzip_driver_window(
        self,
        table_options: dict[str, str],
        splits: list[tuple[str, int, int, bool]],
        start_split: int,
        start_record: int,
    ) -> bool:
        # Docstring: Whether the window starting at ``start_split`` is a driver-read gzip record
        # window.
        #
        # True for a gzip split when ``snapshot.gzip.records.per.batch`` is set, and also for
        # one already part-served (``start_record`` > 0) after the option was cleared, so that
        # file finishes from its recorded position instead of being re-read whole.

        if start_split >= len(splits) or not splits[start_split][3]:
            if start_record:
                # 270: File snapshot offset records a record index for a split that is not a gzip
                # file; the extract changed under a running snapshot -- run a full refresh
                raise InformixError(_m(270))
            return False
        if start_record:
            return True
        return bool(self._table_int_option(table_options, _UNL_GZIP_RECORDS_OPTION, 0, minimum=0))

    def _read_unl_gzip_window(
        self,
        table: Table,
        table_options: dict[str, str],
        snapshot_lsn: int,
        split: tuple[str, int, int, bool],
        start_record: int,
        limit: int | None,
    ) -> tuple[list[dict[str, Any]], bool, int]:
        # Docstring: Read up to ``limit`` records (None: the rest) of a gzip split from
        # ``start_record``.
        #
        # Returns the shaped rows, whether the file is now exhausted -- a short window, or
        # an empty one when the previous window ended exactly at the last record -- and how
        # many records the window read. That count, not the rows, positions the next window:
        # a row_filter can drop records, so a window may hold fewer rows than it read. The open
        # record iterator is kept between microbatches, so consecutive windows decompress the
        # file once; a window that does not continue it (a restart, or a different file)
        # reopens the file and skips ``start_record`` records. A repeated request for the
        # window just read returns the same rows without touching the iterator.

        path, _start_byte, size, _is_gzip = split
        key = (path, size, snapshot_lsn)
        memo_key = (key, start_record, limit)
        memo = self._unl_gzip_window_memo
        if memo is not None and memo[0] == memo_key:
            return memo[1]
        reader = self._unl_gzip_reader
        if reader is None or reader[0] != key or reader[1] != start_record:
            self._unl_gzip_reader = None
            records = self._open_unl_gzip_records(path, size, table_options)
            skipped = sum(1 for _ in itertools.islice(records, start_record))
            if skipped != start_record:
                # 271: gzip snapshot file {} for '{}' holds {} records, fewer than the {} the
                # checkpoint has served; the extract changed under a running snapshot -- run a full
                # refresh
                raise InformixError(_m(271, path, table.exposed_name, skipped, start_record))
            reader = [key, start_record, records]
        try:
            raw_records = list(itertools.islice(reader[2], limit) if limit else reader[2])
        except BaseException:
            self._unl_gzip_reader = None
            raise
        exhausted = limit is None or len(raw_records) < limit
        rows = self._shape_unl_records(table, table_options, snapshot_lsn, raw_records)
        reader[1] = start_record + len(raw_records)
        self._unl_gzip_reader = None if exhausted else reader
        result = (rows, exhausted, len(raw_records))
        self._unl_gzip_window_memo = (memo_key, result)
        return result

    def _replay_unl_gzip_window(
        self,
        table: Table,
        table_options: dict[str, str],
        snapshot_lsn: int,
        split: tuple[str, int, int, bool],
        start_record: int,
        *,
        stop_record: int | None,
    ) -> list[dict[str, Any]]:
        # Docstring: Recompute a logged gzip record window: records ``[start_record, stop_record)``.
        #
        # ``stop_record`` None means the window ran to the end of the file. The logged end is
        # what Spark commits, so a shortfall (the file holds fewer records than the window
        # claimed) fails closed rather than silently dropping rows.

        limit = None if stop_record is None else stop_record - start_record
        if limit is not None and limit <= 0:
            return []
        rows, _exhausted, read = self._read_unl_gzip_window(
            table, table_options, snapshot_lsn, split, start_record, limit
        )
        if limit is not None and read != limit:
            # 272: gzip snapshot file {} for '{}' ended after {} records, short of the {} the logged
            # microbatch covers; the extract changed under a running snapshot -- run a full refresh
            raise InformixError(
                _m(272, split[0], table.exposed_name, start_record + read, stop_record)
            )
        return rows

    def _open_unl_gzip_records(
        self, path: str, size: int, table_options: dict[str, str]
    ) -> Iterator[bytes]:
        # Docstring: A fresh raw-record iterator over a whole gzip `.unl.gz` file, from its start.

        _delimiter, escape = self._unl_field_format(table_options)
        escape_byte = _unl_escape_byte(escape, _client_encoding(self.options))
        return _iter_gzip_unl_records(
            self._unl_read_chunk(path), size, escape=escape_byte, path=path
        )

    def _shape_unl_records(
        self,
        table: Table,
        table_options: dict[str, str],
        snapshot_lsn: int,
        raw_records: list[bytes],
    ) -> list[dict[str, Any]]:
        # Docstring: Shape raw `.unl` records exactly as the serial and executor serves do.

        encoding = _client_encoding(self.options)
        delimiter, escape = self._unl_field_format(table_options)
        commit_time = self._resolve_unl_commit_time(
            self._table_snapshot_source(table_options) or ""
        )
        predicate = _row_filter_predicate(table, self._source_encoding(table_options))
        shaped = (
            _shape_unl_record(
                raw,
                table,
                snapshot_lsn,
                table_options,
                encoding,
                delimiter=delimiter,
                escape=escape,
                commit_time=commit_time,
                predicate=predicate,
                null_byte=_null_byte_mode(self.options),
            )
            for raw in raw_records
        )
        return [row for row in shaped if row is not None]

    def _serve_unl_snapshot(
        self, table: Table, options: dict[str, str], snapshot_lsn: int
    ) -> Iterator[dict[str, Any]]:
        # Docstring: Lazily yield shaped rows read from the table's `.unl` files (serial driver
        # path).
        #
        # The partitioned executor path (``read_partition`` ``unl_split``) shapes each
        # record through the same :func:`_shape_unl_record`, so both produce identical
        # rows. Streams each file record by record (constant memory); the serial path
        # reads the whole file, the executor path reads one byte split. A gzip file (magic
        # bytes ``1f 8b``) is decompressed as a single non-splittable stream.

        source = self._table_snapshot_source(options)
        files = self._list_unl_files(source, table.exposed_name)
        encoding = _client_encoding(self.options)
        delimiter, escape = self._unl_field_format(options)
        escape_byte = _unl_escape_byte(escape, encoding)
        commit_time = self._resolve_unl_commit_time(source)
        predicate = _row_filter_predicate(table, self._source_encoding(options))
        for path in files:
            size = os.path.getsize(path)
            if not size:
                # An empty table unloads to an empty file; a ranged read of it fails (416).
                continue
            read_chunk = self._unl_read_chunk(path)
            if _unl_file_is_gzip(read_chunk):
                records = _iter_gzip_unl_records(read_chunk, size, escape=escape_byte, path=path)
            else:
                records = _iter_unl_records(read_chunk, 0, size, escape=escape_byte, path=path)
            for raw in records:
                row = _shape_unl_record(
                    raw,
                    table,
                    snapshot_lsn,
                    options,
                    encoding,
                    delimiter=delimiter,
                    escape=escape,
                    commit_time=commit_time,
                    predicate=predicate,
                    null_byte=_null_byte_mode(self.options),
                )
                if row is not None:
                    yield row

    def _read_unl_split(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict[str, Any]]:
        # Docstring: Executor side: shape the rows of one `.unl` byte split, without a live Table.
        #
        # The descriptor carries the column metadata, so a Table is rebuilt from it
        # purely for shaping (variable-scale DECIMAL coercion needs the column list); no
        # catalog connection and no Informix session run here. The range is read via a
        # Files-API ranged GET when the descriptor carries a token/host, else a local
        # seek -- the same fallback shape as the driver's ``_unl_read_chunk``. A gzip split
        # (``gzip=True``) spans the whole file and is decompressed as one stream.

        # ``columns`` is the extract's full catalog layout; ``selected`` (absent when every
        # column is ingested) names the columns to keep, and ``row_filter`` the filter.
        catalog = tuple(Column.parse(raw) for raw in partition["columns"])
        selected = partition.get("selected")
        columns = catalog
        if selected is not None:
            wanted = set(selected)
            columns = tuple(column for column in catalog if column.name in wanted)
        # The key goes into nothing but the NULL key check: without it, a NULL in a NOT
        # NULL key column the driver's paths refuse landed from an executor.
        # Owner and name apart, or the stub's exposed_name read '""."owner.table"' in errors.
        exposed_name = str(partition.get("exposed_name", ""))
        owner_and_name = _split_identity(exposed_name)
        if len(owner_and_name) != 2:
            owner_and_name = ["", exposed_name]
        table = Table(
            database="",
            owner=owner_and_name[0],
            name=owner_and_name[1],
            columns=columns,
            primary_keys=tuple(str(key) for key in partition.get("primary_keys") or ()),
            catalog_columns=catalog if selected is not None else (),
            row_filter=partition.get("row_filter"),
        )
        snapshot_lsn = int(partition["snapshot_lsn"])
        encoding = str(partition.get("encoding") or "utf-8")
        # Delimiter/escape ride the descriptor so an HPL DELIMITED extract parses on the
        # executor exactly as on the driver; absent (older descriptor) they fall back to
        # the UNLOAD defaults. An escape stored as "" means escaping is disabled.
        delimiter = str(partition.get("delimiter") or _UNL_DEFAULT_DELIMITER)
        escape = partition.get("escape", _UNL_DEFAULT_ESCAPE)
        escape = "" if escape is None else str(escape)
        # Manifest boundary_time (UTC epoch seconds) rides the descriptor so every split's
        # `r` rows share the extract's one commit time; None -> NULL commit time.
        raw_commit_time = partition.get("commit_time")
        commit_time = None if raw_commit_time is None else int(raw_commit_time)
        # The filter counts LENGTH in the connection's DB_LOCALE here as on the driver; the
        # connection options travel with the pickled connector, the catalog does not.
        predicate = _row_filter_predicate(table, self._source_encoding(table_options))
        read_chunk = _unl_split_read_chunk(partition)
        start = int(partition["start_byte"])
        end = int(partition["end_byte"])
        escape_byte = _unl_escape_byte(escape, encoding)
        path = partition["path"]
        if partition.get("gzip"):
            records = _iter_gzip_unl_records(read_chunk, end, escape=escape_byte, path=path)
        else:
            records = _iter_unl_records(read_chunk, start, end, escape=escape_byte, path=path)
        for raw in records:
            row = _shape_unl_record(
                raw,
                table,
                snapshot_lsn,
                table_options,
                encoding,
                exposed_name=exposed_name,
                delimiter=delimiter,
                escape=escape,
                commit_time=commit_time,
                predicate=predicate,
                null_byte=_null_byte_mode(self.options),
            )
            if row is not None:
                yield row

    def _unl_all_splits(
        self, table: Table, table_options: dict[str, str]
    ) -> list[tuple[str, int, int, bool]]:
        # Docstring: The full ordered ``(path, start_byte, end_byte, gzip)`` split plan for the
        # extract.
        #
        # Files are listed sorted; a plain file is split into ~``snapshot.split.bytes`` byte
        # pieces (per-table, connection fallback, then :data:`_UNL_SPLIT_TARGET_BYTES`), while
        # a gzip file (magic bytes ``1f 8b``) is non-splittable and yields a single whole-file
        # split flagged ``gzip=True``. The plan is deterministic given the immutable extract
        # and target size, so ``latest_offset`` and ``get_partitions`` -- separate calls --
        # agree on the same global split ordering without sharing state, and a windowed serve
        # tiles ``[0, total)`` across microbatches with no gap, overlap, or duplication.

        target_bytes = self._table_int_option(
            table_options, _UNL_SPLIT_BYTES_OPTION, _UNL_SPLIT_TARGET_BYTES, minimum=1
        )
        source = self._table_snapshot_source(table_options)
        # The split planner must skip escaped newlines exactly as the record readers
        # do, so it takes the same escape byte (in the extract's encoding).
        _delimiter, escape = self._unl_field_format(table_options)
        escape_byte = _unl_escape_byte(escape, _client_encoding(self.options))
        splits: list[tuple[str, int, int, bool]] = []
        # A file's plan, by its size and change time: re-planned on every read, an extract's
        # boundaries were probed again in every window served from it, ranged reads of each.
        plans = self.__dict__.setdefault("_unl_split_plans", {})
        for path in self._list_unl_files(source, table.exposed_name):
            info = os.stat(path)
            key = (path, info.st_size, info.st_mtime_ns, target_bytes, escape_byte)
            if key not in plans:
                plans[key] = self._unl_file_splits(path, info.st_size, target_bytes, escape_byte)
            splits += plans[key]
        return splits

    def _unl_file_splits(self, path, size, target_bytes, escape_byte):
        if not size:
            # No records to split, and a ranged read of an empty file fails (416).
            return []
        read_chunk = self._unl_read_chunk(path)
        if _unl_file_is_gzip(read_chunk):
            # Gzip is not seekable, so the whole compressed file is one split.
            return [(path, 0, size, True)]
        return [
            (path, start_byte, end_byte, False)
            for start_byte, end_byte in _unl_byte_splits(
                read_chunk, size, target_bytes=target_bytes, escape=escape_byte
            )
        ]

    def _unl_split_descriptors(
        self,
        table: Table,
        table_options: dict[str, str],
        snapshot_lsn: int,
        *,
        start_split: int,
        window: int,
        all_splits: list | None = None,
    ) -> list[dict]:
        # Docstring: ``unl_split`` partition descriptors for the split window ``[start_split,
        # +window)``.
        #
        # The window (``stream.partitioned.snapshot.pages.per.batch`` splits) is sliced from
        # the deterministic :meth:`_unl_all_splits` plan, so this microbatch serves only its
        # slice and the offset advances by ``window`` splits per read. Mints a per-batch
        # staging token when REST is available so executors read via ranged GET; omitting it
        # makes executors fall back to a local seek. The token rides only this batch's
        # transient descriptors, never the checkpoint offset.

        if all_splits is None:
            all_splits = self._unl_all_splits(table, table_options)
        end_split = min(start_split + window, len(all_splits))
        columns = [_partition_column(column) for column in table.catalog_columns or table.columns]
        encoding = _client_encoding(self.options)
        delimiter, escape = self._unl_field_format(table_options)
        commit_time = self._resolve_unl_commit_time(
            self._table_snapshot_source(table_options) or ""
        )
        # Resolved after the manifest read, so a token that read found rejected (moving
        # ``auto`` to the mount) isn't handed to the executors.
        files_api = self._staging_transport()
        token = files_api._credential.bearer() if files_api is not None else None
        host = files_api._host if files_api is not None else None
        descriptors: list[dict] = []
        for path, start_byte, end_byte, is_gzip in all_splits[start_split:end_split]:
            descriptor = {
                "kind": "unl_split",
                "path": path,
                "start_byte": start_byte,
                "end_byte": end_byte,
                "gzip": is_gzip,
                "columns": columns,
                # Set whenever the table ingests fewer columns than the extract holds: under
                # a column selection, or with columns of a type the connector can't read.
                **(
                    {}
                    if not table.catalog_columns
                    else {"selected": [column.name for column in table.columns]}
                ),
                **({} if table.row_filter is None else {"row_filter": table.row_filter}),
                # For the NULL key check the driver's paths make (_refuse_null_key).
                **({"primary_keys": list(table.primary_keys)} if table.primary_keys else {}),
                "snapshot_lsn": snapshot_lsn,
                "exposed_name": table.exposed_name,
                "encoding": encoding,
                "delimiter": delimiter,
                "escape": escape,
                "commit_time": commit_time,
            }
            if token and host:
                descriptor["token"] = token
                descriptor["host"] = host
                # An executor can't replace a rejected token, so under ``auto`` it reads
                # the split through the mount instead, as the driver would.
                descriptor["fuse_fallback"] = not self._staging_rest_required
            descriptors.append(descriptor)
        return descriptors

    def _read_append_only(self, table: Table, start: dict | None, options: dict[str, str]):
        # Docstring: Begin an append-only flow, then hand off to the ordinary CDC stream.
        #
        # Only the *first* read reaches here. It establishes a stream-phase offset and
        # every later read is routed straight to :meth:`_read_stream`, which needs no
        # changes: it derives its restart position from ``begin_lsn``/``commit_lsn``
        # and never consults ``table.primary_keys``.
        #
        # Two startup behaviours, selected by ``snapshot.mode``:
        #
        # ``incremental`` (the default for a table with a key, as for a merged one)
        #     Copy the rows the table holds in chunks while the stream runs, the copy
        #     following the stream so that each change is in the log once (see
        #     _begin_incremental); a table whose key it cannot follow by takes initial's
        #     snapshot instead.
        #
        # ``initial`` (the default for a table without a key)
        #     Copy the rows the table holds, then continue from the snapshot's LSN, so
        #     history is captured and the handoff is gapless. The snapshot pages by an
        #     index, or by its key, and follows the table's changes while it runs (see
        #     the README's exact snapshots of keyless tables); without either it reads
        #     through one forward-only cursor. Fetched rows are written to immutable
        #     staged pages and become visible only after the completed snapshot
        #     manifest is published. Either default starts a change log with the rows the
        #     table holds: one that started from today's log alone could not be rebuilt
        #     from it.
        #
        # ``cdc_only``
        #     Start at the server's current log position and emit nothing now. Rows
        #     that predate the flow are not captured. No size ceiling and no snapshot,
        #     for a table whose arriving rows are all that matter.
        #
        # When ``table.migration=true`` (checked before either mode), the flow instead
        # records the checkpoint a prior flow parked in Lakebase (see :meth:`_read_handoff`),
        # emits no rows, and stops -- skipping the snapshot entirely. Used to switch a table
        # SCD1->append-only, or relocate its destination, without a re-snapshot. The table
        # keeps its real ``snapshot.mode``; migration never streams, and clearing
        # table.migration (Restore) resumes streaming from the recorded offset, with
        # _recover's strict ``commit_lsn > C`` filter keeping the seam gapless and
        # duplicate-free.

        _ensure_materializable(table, self._with_connection_scan(options))
        table = self._refresh_table_schema(table, None)
        pipeline_scope = self._pipeline_scope()
        if self._table_migration(options):
            token = self._read_handoff_token(table)
            if token is None:
                # 273: table.migration=true for '{}' found no parked checkpoint in Lakebase. Before
                # switching a table to append-only (or relocating it), run its source flow once in
                # triggered mode with table.migration=true so it parks its stream offset, then
                # switch. To start fresh instead, clear table.migration and use
                # snapshot.mode=cdc_only (no history) or snapshot.mode=initial (full snapshot).
                raise InformixError(_m(273, table.exposed_name))
            # A keyed table's two flows park their own positions, and the append stream
            # goes on from the upsert flow's alone: the deletes between the two were lost
            # (the delete flow behind) or sent again (ahead). So too for a copy parked
            # mid-way (incremental): its upsert flow went on past deletes it skipped.
            deletes = self._read_handoff_token(table, _HANDOFF_CHANNEL_DELETE)
            parked = [json.loads(token["offset_json"])]
            if deletes is None and parked[0].get(_HANDOFF_KEYED) and parked[0]["phase"] == "stream":
                # 489: table.migration for '{}' parked its upsert flow at LSN {} but no position
                # for its delete flow, which had no checkpoint at capture, and an append stream
                # goes on from the upsert position: the deletes after the delete flow's start
                # would be lost. Put the keyed configuration back without table.migration, run
                # one triggered update with no writes to the table while it runs, and capture
                # again
                raise InformixError(_m(489, table.exposed_name, parked[0].get("commit_lsn")))
            if deletes is not None and parked[0].get("phase") == "snapshot":
                # Parked mid-snapshot: the append stream goes on from the snapshot's LSN once
                # the pages are served, and the delete flow, which streams from there while
                # they are, already applied what came since to the rows served before.
                snapshot_at = str(parked[0].get("snapshot_lsn"))
                delete_at = _handoff_position(json.loads(deletes["offset_json"]))
                if delete_at != snapshot_at:
                    # 491: table.migration for '{}' parked its upsert flow in the middle of its
                    # snapshot, taken at LSN {}, but its delete flow at {}, and an append
                    # stream goes on from the snapshot's LSN: the deletes the delete flow
                    # applied since would be sent again. Run the keyed configuration without
                    # table.migration until the snapshot is done and the table streams, then
                    # capture again
                    raise InformixError(_m(491, table.exposed_name, snapshot_at, delete_at))
            if deletes is not None and parked[0].get("phase") == "stream":
                parked.append(json.loads(deletes["offset_json"]))
                # The change position too where it is not the commit's: stopped inside one
                # commit, the two named the same LSN.
                upsert_at, delete_at = (_handoff_position(p) for p in parked)
                if upsert_at != delete_at:
                    # 488: table.migration for '{}' parked its upsert flow at LSN {} but its
                    # delete flow at {}, and an append stream goes on from one position: the
                    # deletes between them would be lost or sent again. Put the keyed
                    # configuration back without table.migration, run one triggered update with
                    # no writes to the table while it runs, and capture again
                    raise InformixError(_m(488, table.exposed_name, upsert_at, delete_at))
            # Seed from the parked position and stop (no rows); migration never streams.
            # Adopts this flow's scope -- the seam is carried by the scope-independent LSNs,
            # and the first stream read (after Restore clears table.migration) re-publishes
            # the scoped channel-start record under the new scope. Schema drift since capture
            # fails closed inside the helper. The token is not deleted here -- it is dropped
            # only once Spark has committed this seed (see the append+stream branch in
            # _read_table_attempt), so a crash before commit re-seeds rather than failing
            # closed.
            return iter(()), self._handoff_seed_from_token(table, token)

        snapshot_mode = self._table_snapshot_mode(table, options)
        if snapshot_mode == "incremental" and table.primary_keys:
            return self._read_incremental(table, start, options)
        if snapshot_mode not in {"initial", "cdc_only"}:
            # 274: Append-only table '{}' supports only snapshot.mode=initial or
            # snapshot.mode=cdc_only, or snapshot.mode=incremental when it has a key
            raise ValueError(_m(274, table.exposed_name))

        if snapshot_mode == "initial":
            return self._read_snapshot(table, start, options, allow_keyless=True)

        # The boundary is durable and shared, exactly as for a keyed table, so a
        # restart before the first checkpoint commits resumes from the same position
        # instead of skipping or re-reading whatever the log had reached by then.
        boundary = self._initial_lsn(table, scope=pipeline_scope)
        if boundary is None:
            # 275: Could not establish an append-only start boundary for '{}'
            raise InformixError(_m(275, table.exposed_name))
        schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]

        rows: Iterator[dict] = iter(())

        # phase="stream" so the next read goes straight to _read_stream. tx_id is None
        # and change_lsn equals commit_lsn because no transaction has been read yet --
        # the same shape the keyed snapshot-to-stream transition produces.
        return rows, _offset(
            boundary,
            boundary,
            boundary,
            None,
            "stream",
            table,
            schema_id,
            pipeline_scope,
        )

    def _read_snapshot_only(self, table: Table, options: dict[str, str]):
        _ensure_materializable(table, self._with_connection_scan(options))
        table = self._refresh_table_schema(table, None)
        fingerprint = _schema_fingerprint(table)
        # PK-less tables cannot be seek-paginated safely.  Read exactly once;
        # returning None signals non-checkpointable full refresh semantics.
        limit = self._table_int_option(options, "snapshot.max.rows", 0, minimum=0)
        if limit == 0:
            limit = 100000

        def read() -> list[dict[str, Any]]:
            # In one transaction at the table's snapshot.isolation, as an initial snapshot
            # reads: bare, it took the session's isolation and timeout and, in an ANSI
            # database, left its locks held. The limit counts the rows that land, which a
            # NUL byte can take out of the row_filter (see _landed_rows).
            landed: list[dict[str, Any]] = []

            def consume(_lsn, _index, page) -> None:
                landed.extend(self._landed_rows(table, page))
                if len(landed) > limit:
                    # 276: Snapshot-only table {} exceeds snapshot.max.rows={}
                    raise InformixError(_m(276, table.exposed_name, limit))

            self._bridge.consistent_snapshot(
                table.identity,
                [c.name for c in table.columns],
                (),
                limit + 1,
                0,
                self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0),
                page_consumer=consume,
                snapshot_filter=_snapshot_predicate(_snapshot_filter(options), table),
                isolation=self._snapshot_isolation(options),
            )
            return landed

        rows = self._snapshot_read_with_reconnect(read, table=table, operation="snapshot read")
        table = self._refresh_table_schema(table, fingerprint)
        lsn = self._snapshot_read_with_reconnect(
            self._bridge.current_lsn, table=table, operation="end"
        )
        commit_time = self._source_utc_time()
        return (
            iter(
                _shape_snapshot(row, lsn, table, options, commit_time=commit_time) for row in rows
            ),
            None,
        )

    def _read_changes_with_reconnect(
        self, captures, restart, timeout_seconds, max_records, *, table: Table
    ):
        # Docstring: Run one bounded CDC poll, retrying in place on a mid-read drop.
        #
        # A connection dropped during ``read_changes`` raises ``SqliProtocolError``
        # (e.g. ``SQ_LODATA server/ISAM error -1`` when the socket dies mid-poll).
        # The read has not advanced any offset, so it is safe to reset the dead
        # transport and reissue the identical poll under the connection slot the
        # reader already holds. Bounded attempts with short backoff absorb a
        # transient blip; exhausting them re-raises so a genuinely-down source
        # still fails honestly.
        #
        # This retries synchronously inside the single ``latestOffset`` call rather
        # than yielding an empty batch, so it is correct for a triggered
        # (AvailableNow) flow too: an empty batch below the frozen high-water mark
        # would let AvailableNow complete the batch and skip the un-read changes,
        # whereas retrying in place either returns the real records or raises.
        #
        # A lost connection-capacity lease (``ConnectionCapacityUnavailable``) is
        # not a transport drop and is never retried here; it propagates to the
        # capacity-aware handler.

        attempt = 0
        while True:
            # A fresh spill each attempt: a retried read writes its transactions' pages again.
            self._bridge.cdc_spill = self._cdc_spill(captures)
            try:
                return self._bridge.read_changes(captures, restart, timeout_seconds, max_records)
            except ConnectionCapacityUnavailable:
                raise
            # A reconnect that failed while the network or the listener recovers, too.
            except (SqliProtocolError, OSError) as error:
                if not reconnectable_error(error):
                    raise
                lease_lost = getattr(self._bridge, "_connection_lease_lost", None)
                if lease_lost is not None and lease_lost.is_set():
                    # The drop is a deliberate lease-loss shutdown, not a blip.
                    raise
                if attempt >= _CDC_RECONNECT_MAX_RETRIES:
                    raise
                delay = min(
                    _CDC_RECONNECT_BASE_SECONDS * (2**attempt),
                    _CDC_RECONNECT_MAX_SECONDS,
                )
                # 277: Informix CDC poll for '{}' lost its connection ({}); resetting the transport
                # and retrying the same read (attempt {}/{}) after {:.1f}s
                logging.getLogger(__name__).warning(
                    _m(
                        277,
                        table.exposed_name,
                        error,
                        attempt + 1,
                        _CDC_RECONNECT_MAX_RETRIES,
                        delay,
                    ),
                    exc_info=True,
                )
                reset = getattr(self._bridge, "reset_transport", None)
                if reset is not None:
                    reset()
                time.sleep(delay)
                attempt += 1
            finally:
                self._bridge.cdc_spill = None
