"""Snapshot modes, resnapshots, the delete channel's floors and handoffs, and the staging
transport.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import time
from typing import Any, Iterator

from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _ROW_FILTER_OPTION,
    _TRANSFORM_HASH_KEY_OPTION,
    _apply_column_renames,
    _apply_column_transforms,
    _column_renames,
    _column_transforms,
    _ensure_selected_keys,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _apply_decimal_targets,
    _cdc_capable,
    _ensure_materializable,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _key_override,
    _option_bool,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _schema_node_fallback_retry_count,
    _validated_offset,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _APPEND_INGESTION_OPTION,
    _DEFAULT_SNAPSHOT_MODE,
    _DEFAULT_SNAPSHOT_STAGING_TRANSPORT,
    _FULL_REFRESH_REQUESTS,
    _HANDOFF_CHANNEL_DELETE,
    _HANDOFF_CHANNEL_UPSERT,
    _HANDOFF_KEYED,
    _OFFSET_ADVISORY_FIELDS,
    _OWN_START_SNAPSHOT_MODES,
    _PIPELINE_SCOPE,
    _REPLAY_END_SEED_OPTION,
    _REPLAY_REESTABLISHED,
    _SCHEMA_NODE_FALLBACK_RETRIES,
    _SEED_FLOOR_FIELD,
    _SEED_MARKS,
    _SNAPSHOT_MODES,
    _SNAPSHOT_STAGE_CLEANUP_ATTEMPTS,
    _SNAPSHOT_STAGING_TRANSPORT_OPTION,
    _TABLE_MIGRATION_OPTION,
    _UNL_SNAPSHOT_MODES,
    _UNSUPPORTED_SNAPSHOT_MODES,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    Table,
    _replay_keeps_floor,
)
from databricks.labs.community_connector.sources.informix.informix_shards import _schema_fingerprint
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _apply_row_filter,
    _path_present,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _offset,
    _table_from_schema_state,
)
from databricks.labs.community_connector.sources.informix.lakebase_state import read_state_record
from databricks.labs.community_connector.sources.informix.lakebase_state_store import (
    delete_cdc_handoff,
    get_cdc_handoff,
    put_cdc_handoff,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    StagingTokenUnavailable,
    WorkspaceFilesApi,
    WorkspaceHttpError,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    add_informix_exception_note,
)


class _DeleteChannelMixin:
    # Docstring: Snapshot modes, resnapshots, the delete channel's floors and handoffs, and the
    # staging transport.

    def _apply_table_options(self, table: Table, table_options: dict[str, str]) -> Table:
        # Docstring: Apply every per-table option that reshapes the catalog table.
        #
        # The column selection runs first, then the columns of a type the connector can't
        # read are left out, so the key options see only the columns ingested (a wildcard
        # key covers just those), then row_filter, since it may only use selected columns,
        # then the transforms and the renames, which may only change them.

        override = _key_override(table_options)
        table = self._ingested_columns(table, table_options, override)
        table = self._apply_key_options(table, table_options, override)
        _ensure_selected_keys(table)
        table = _apply_decimal_targets(table, table_options)
        table = _apply_row_filter(table, table_options.get(_ROW_FILTER_OPTION))
        table = _apply_column_transforms(
            table,
            _column_transforms(table_options),
            self.options.get(_TRANSFORM_HASH_KEY_OPTION)
            or table_options.get(_TRANSFORM_HASH_KEY_OPTION),
            options=table_options,
        )
        return _apply_column_renames(table, _column_renames(table_options))

    def _snapshot_mode(self, table_options: dict[str, str]) -> str:
        mode = (
            str(
                table_options.get(
                    "snapshot.mode",
                    self.options.get("snapshot.mode", _DEFAULT_SNAPSHOT_MODE),
                )
            )
            .strip()
            .lower()
        )
        if mode in _UNSUPPORTED_SNAPSHOT_MODES:
            # 194: snapshot.mode={} is an external extension mechanism and is not supported by the
            # Python Informix connector
            raise ValueError(_m(194, mode))
        if mode == "handoff":
            # 195: snapshot.mode=handoff has been replaced by the 'table.migration' option; keep the
            # table's real snapshot.mode and set table.migration=true to migrate it (SCD1->append
            # switch or destination relocation), then clear table.migration to resume.
            raise ValueError(_m(195))
        if mode not in _SNAPSHOT_MODES:
            allowed = ", ".join(sorted(_SNAPSHOT_MODES))
            # 196: Unsupported snapshot.mode={!r}; supported values are {}
            raise ValueError(_m(196, mode, allowed))
        return mode

    def _snapshot_mode_set(self, table_options: dict[str, str]) -> bool:
        # Docstring: Whether the table's configuration or the connection sets snapshot.mode.
        return "snapshot.mode" in table_options or "snapshot.mode" in self.options

    def _table_snapshot_mode(self, table: Table, table_options: dict[str, str]) -> str:
        # Docstring: snapshot.mode as this table's reads take it. An append-only table with
        # none set, on the table or on the connection, takes incremental as any table with
        # a key does, its copy following the stream (see _begin_incremental), and initial
        # without one: either copies the rows the table holds, where cdc_only copied none.

        if (
            not table.primary_keys
            and not self._snapshot_mode_set(table_options)
            and self._append_only_table(table, table_options)
        ):
            return "initial"
        return self._snapshot_mode(table_options)

    def _table_migration(self, table_options: dict[str, str]) -> bool:
        # Docstring: Whether this table is being migrated (``table.migration=true``).
        #
        # Per-table first, falling back to the connection default; orthogonal to
        # snapshot.mode, which is left unchanged while migrating. When true the read runs
        # the record-and-stop handoff (see :meth:`_read_handoff`) instead of its normal
        # snapshot/stream behaviour.

        if _TABLE_MIGRATION_OPTION in table_options:
            return _option_bool(table_options, _TABLE_MIGRATION_OPTION, False)
        return _option_bool(self.options, _TABLE_MIGRATION_OPTION, False)

    @staticmethod
    def _schema_node_fallback_offset(start_offset: dict[str, Any]) -> dict[str, Any]:
        # Docstring: Carry the bootstrap deferral count on an advisory (non-positional) offset.
        #
        # A delete reader with no offset waits for the upsert reader to publish this
        # update's channel-start and yields an empty advisory batch meanwhile. The count
        # saturates at ``_SCHEMA_NODE_FALLBACK_RETRIES``: past it the returned offset
        # stops changing, which is how a replay (``_replay_between_offsets``) recognises
        # "still waiting for the upsert publication" and waits in place rather than
        # treating the unchanged offset as a failed replay. It rides an offset that has no
        # position at all, so it can never move a checkpoint. The field name is historical
        # -- it once gated a scope-independent schema-node fallback that never became part
        # of the bootstrap -- and is kept for checkpoint compatibility.

        count = _schema_node_fallback_retry_count(start_offset)
        if count >= _SCHEMA_NODE_FALLBACK_RETRIES:
            return dict(start_offset)
        result = dict(start_offset)
        result["schema_node_fallback_retry_count"] = count + 1
        return result

    def _checkpoint_requires_snapshot(self, checkpoint: dict[str, Any]) -> bool:
        validated = _validated_offset(checkpoint)
        restart = int(validated.get("begin_lsn") or validated["commit_lsn"])
        return restart < self._bridge.minimum_lsn()

    def _resnapshot_scope(self, table: Table, checkpoint: dict[str, Any]) -> str:
        del table
        _validated_offset(checkpoint)
        return self._pipeline_scope()

    def _bootstrap_seed_table(self, table: Table, schema_id: str, fingerprint: str) -> Table:
        # Docstring: The layout a delete bootstrap seeds under: the live one, or a recorded node.
        #
        # The upsert reader publishes the update's channel start at its checkpoint before it
        # steps, and a snapshot floor keeps the schema it was captured under, so the seed's
        # schema can be one the live layout has moved past (a column changed while the
        # pipeline was stopped). The seed names that recorded node; the next poll steps from
        # it as any lagging reader does.

        if fingerprint == _schema_fingerprint(table):
            return table
        record = self._read_immutable_head(
            self._immutable_namespace(table, "schema-nodes", schema_id)
        )
        node = record.get("schema") if isinstance(record, dict) else None
        if not isinstance(node, dict) or node.get("id") != schema_id:
            # 199: Informix upsert/delete bootstrap schema mismatch for '{}'
            raise InformixError(_m(199, table.exposed_name))
        self._validate_immutable_record_header(record, "schema-node", table.exposed_name)
        seed_table = _table_from_schema_state(node, table.database)
        if (
            seed_table.native_identity != table.native_identity
            or _schema_fingerprint(seed_table) != fingerprint
        ):
            # 199: Informix upsert/delete bootstrap schema mismatch for '{}'
            raise InformixError(_m(199, table.exposed_name))
        return seed_table

    def _resnapshot_delete_seed(self, table: Table, scope: str) -> dict | None:
        # Docstring: The delete seed at the update's resnapshot boundary, once it is published.
        #
        # The resnapshot ran under the live layout, which this reader's cached table may
        # predate, and the boundary lookup checks the two match.

        table = self._refresh_table_schema(table, None)
        high_water = self._initial_lsn(table, owner=False, wait=False, scope=scope)
        # The pipeline's floor, where it is below this update's boundary or this update has
        # none: a replay of a resnapshot batch took it again here, while Spark goes on from
        # the batch's end at the floor; or the upsert reader committed its resnapshot in an
        # earlier update and resumes from it, and this one never publishes a boundary (427
        # on every update). Started above the floor, the reader skipped the deletes between.
        # Whatever the update was started for: a resnapshot's destination keeps its rows, and
        # the floor it kept (_publish_delete_snapshot_floor), which in a refresh of other
        # tables was passed over and the deletes after it skipped.
        floor = self._delete_floor_seed(table, kept_destination=True)
        if floor is None or (high_water is not None and int(floor["commit_lsn"]) >= high_water):
            if high_water is None:
                return None
            floor = None
        self._schema_transition_waits.pop((table.identity, "resnapshot"), None)
        if floor is not None:
            return floor
        schema_id = self._snapshot_schema_ids[(scope, table.identity)]
        return {
            **_offset(high_water, high_water, high_water, None, "stream", table, schema_id, scope),
            # The seed is this update's first offset for the flow: see _read_stream.
            "update_fingerprint": _schema_fingerprint(table),
        }

    def _read_resnapshotted_deletes(
        self, table: Table, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: Move a delete checkpoint the log no longer retains to its resnapshot.
        #
        # auto_snapshot replaces it with a snapshot whose boundary the upsert reader
        # publishes on its read. Until it has, this yields a retry offset at the same
        # position: an unchanged offset reads as a drained source, which would end a
        # triggered update with no seed, and a later update's scope never gets one. A
        # replayed wait stays where it was; a replayed move to the boundary lands on it.
        # A wait's logged end is the checkpoint's own position, so any other end is a move,
        # one below the checkpoint's commit too: the floor, or a seed whose page-start
        # boundary is below it, as an open transaction's begin can expire first.

        checkpoint = _validated_offset(start_offset)
        replay_stop = self._replay_stop_lsn(table_options)
        if replay_stop is not None and replay_stop == int(checkpoint["commit_lsn"]):
            return iter(()), dict(start_offset)
        if replay_stop is not None:
            # From an expired checkpoint this reader only waits in place or moves to the
            # resnapshot boundary, a batch with no rows. Replayed in a later update, that
            # boundary is under the earlier update's scope, so a lookup here never finds it:
            # land on the logged end instead. A batch read before the log expired under a
            # stopped pipeline held deletes that are gone from the log either way.
            # 426: Informix delete channel for '{}' replays a batch from expired checkpoint LSN
            # {} by landing on its logged end {} without reading: such a batch moved the channel
            # to the resnapshot boundary and held no rows, unless the log expired while the
            # pipeline was stopped, in which case the deletes it read are no longer in the log
            logging.getLogger(__name__).warning(
                _m(426, table.exposed_name, checkpoint["commit_lsn"], replay_stop)
            )
            position = str(replay_stop)
            return iter(()), {
                **checkpoint,
                "commit_lsn": position,
                "change_lsn": position,
                "begin_lsn": position,
                "tx_id": None,
                # No transaction to match: the batch's deletes, if any, are gone.
                _REPLAY_REESTABLISHED: True,
            }
        seed = self._resnapshot_delete_seed(table, self._resnapshot_scope(table, checkpoint))
        if seed is not None:
            return iter(()), seed
        return iter(()), self._resnapshot_retry_offset(table, start_offset)

    def _read_table_deletes(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        *,
        snapshot_mode: str = "initial",
        bootstrap_offset: dict | None = None,
    ) -> tuple[Iterator[dict], dict]:
        # A position seeded at the snapshot floor (_delete_floor_seed) carries the floor's
        # update until the upsert reader's start shows which snapshot the target holds.
        seeded = (start_offset or {}).get("floor_seed")
        seed_floor = (start_offset or {}).get(_SEED_FLOOR_FIELD)
        restart_from = table = stamp = None
        if seeded is not None:
            # Validated first: the check takes the seed apart (469 on an invalid one).
            _validated_offset(start_offset)
            table = self._table(table_name, table_options)
            confirmed, seeded = self._confirmed_floor_seed(table, start_offset, table_options)
            if not confirmed:
                # One with its own seed is sent back from a full refresh's own start (477);
                # one with another floor's (291) starts again marked with that floor, which
                # the next batch confirms. Unmarked, a seed above the start read as a read
                # on in a replay, which then read from the position the floor replaced.
                restart_from = start_offset if seeded == start_offset["floor_seed"] else None
                stamp, seeded = (None if restart_from else seeded), None
                # Started again, as a reader with no checkpoint, whose waits count on an
                # offset of advisory fields alone: on the old offset they brought back the
                # position the floor replaced, and a replay stopped at it.
                bootstrap_offset = {
                    k: v
                    for k, v in (bootstrap_offset or start_offset).items()
                    if k in _OFFSET_ADVISORY_FIELDS
                }
            start_offset = confirmed
        rows, end = self._read_table_deletes_from(
            table_name,
            start_offset,
            table_options,
            snapshot_mode=snapshot_mode,
            bootstrap_offset=bootstrap_offset,
            restart_from=restart_from,
        )
        lsn = restart_from and restart_from["commit_lsn"]
        if lsn and int(end.get("commit_lsn") or lsn) < int(lsn):
            # 477: Informix delete channel for '{}' started at full refresh {}'s own start,
            # and its upsert reader went on from a checkpoint instead (the refresh left
            # the table); it starts again below that start, at {}
            logging.getLogger(__name__).warning(
                _m(477, table.exposed_name, restart_from["floor_seed"], end["commit_lsn"])
            )
        if seeded is not None and end.get("commit_lsn") is not None:
            end = {**end, "floor_seed": seeded}
            if seed_floor is not None:
                end[_SEED_FLOOR_FIELD] = seed_floor
        elif stamp and end.get("commit_lsn") is not None and "floor_seed" not in end:
            end = {**end, "floor_seed": stamp}
        return rows, end

    def _delete_floor_recorded(self, table_name: str, table_options: dict[str, str]) -> bool:
        # Docstring: Whether this pipeline has a snapshot floor a delete reader with no
        # checkpoint may start at, told without opening Informix (see
        # _upsert_channel_start_exists).

        if self._update_request in _FULL_REFRESH_REQUESTS:
            return False
        identity = self._native_table_state_identity(
            table_options.get("qualified_source_table", table_name)
        )
        if identity is None:
            return False
        _, connection_key, table_key = identity
        record_key = "/".join(
            (connection_key, table_key, "delete-floor", self._pipeline_id(self._pipeline_scope()))
        )
        record = self._state(
            read_state_record,
            connection_key,
            record_key,
            operation="delete-floor probe",
        )
        if not isinstance(record, dict) or not isinstance(record.get("snapshot_scope"), str):
            return False
        if self._floor_predates_reset(record):
            return False
        # A floor _delete_floor_seed turned down is not one to wait in a connection slot for.
        rejected = self.__dict__.get("_rejected_delete_floors", {})
        return rejected.get(record_key) != record["snapshot_scope"]

    def _delete_floor_seed(self, table: Table, *, kept_destination: bool = False) -> dict | None:
        # Docstring: The offset a delete reader with no checkpoint starts at before the
        # upsert reader has published this update's start: this pipeline's snapshot floor.
        #
        # A triggered update runs at most so many flows at once, and a delete flow whose
        # upsert flow is still queued found no start to wait for: it gave up after
        # _SCHEMA_NODE_FALLBACK_RETRIES empty batches, the update ended it with no
        # checkpoint, and every later triggered update ended it at once. Its table's
        # deletes, and the old rows of key-changing updates, were then never applied (seen
        # live: half the tables of a triggered run). The floor is the LSN the target's rows
        # were snapshotted at, so the deletes after it are the ones the target lacks, and an
        # earlier start only reads again what the sequence column makes harmless.
        #
        # Not in a full refresh, whose snapshot is still to come, nor where the floor's
        # layout is not today's (the reader would have steps to take that the upsert reader
        # records): those wait as before. An update the pipelines API can't say the purpose
        # of starts there too: the floor of a target a full refresh has since emptied is
        # told later, by its update (_confirmed_floor_seed), and the deletes read before
        # that are older than the rows the new snapshot lands, which they leave alone.

        if self._update_request in _FULL_REFRESH_REQUESTS and not kept_destination:
            return None
        record = self._floor_record(table)
        if record is None or not isinstance(record.get("snapshot_scope"), str):
            return None
        self._validate_immutable_record_header(record, "delete-floor", table.exposed_name)
        schema_id, fingerprint = record.get("schema_id"), _schema_fingerprint(table)
        floor = self._immutable_lsn(record, "start_lsn", table.exposed_name)
        unusable = (
            record.get("table") != table.native_identity
            or record.get("fingerprint") != fingerprint
            or not isinstance(schema_id, str)
            or not re.fullmatch(r"[0-9a-f]{32}", schema_id)
            or self._floor_predates_reset(record)
            or not self._bridge.minimum_lsn() <= floor <= self._bridge.current_lsn()
        )
        if unusable:
            # Remembered by its update, so the reader's later tries wait for its upsert
            # reader without a connection slot (_delete_floor_recorded); another floor is
            # judged afresh.
            namespace = self._immutable_namespace(
                table, "delete-floor", self._pipeline_id(self._pipeline_scope())
            )
            self.__dict__.setdefault("_rejected_delete_floors", {})[namespace] = record[
                "snapshot_scope"
            ]
            return None
        # 297: Informix delete channel for '{}' starts at the snapshot floor {} without
        # waiting for the upsert reader: it has no checkpoint, and the floor is this
        # pipeline's last snapshot of the table
        logging.getLogger(__name__).info(_m(297, table.exposed_name, floor))
        return {
            **_offset(
                floor, floor, floor, None, "stream", table, schema_id, self._pipeline_scope()
            ),
            "update_fingerprint": fingerprint,
            "floor_seed": record["snapshot_scope"],
        }

    def _floor_predates_reset(self, record: dict) -> bool:
        # Docstring: Whether a snapshot floor was written before the pipeline's latest full
        # refresh, which emptied the target the floor's snapshot filled.
        #
        # One stopped before a snapshot of its own replaced the floor left it standing, and
        # the next update's delete readers, with no checkpoint, started there: at deletes of
        # rows no target held, read through whatever the source did since (seen live: a
        # database rebuilt twice failed those reads with SQ_LODATA -1). They wait for this
        # update's start instead, as with no floor, and the next floor is this one's.

        reset_at = self.__dict__.get("_update_reset_at")
        if reset_at is None:
            return False
        try:
            return float(record.get("created_at")) < float(reset_at)
        except (TypeError, ValueError):
            return False

    def _full_refresh_delete_start(self, table_options: dict[str, str], snapshot_mode: str) -> bool:
        # Docstring: Whether a delete reader with no checkpoint takes this update's start
        # itself when its upsert reader has published none (_full_refresh_delete_seed).
        #
        # In a full refresh of the whole pipeline, where every table's flows start over, and
        # where the upsert reader starts at the update's initialization: not from a
        # snapshot.source extract (its boundary is the operator's), nor under another mode.

        scope = self._registration_scope
        return (
            self._update_request == "full_refresh"
            and snapshot_mode in _OWN_START_SNAPSHOT_MODES
            and isinstance(scope, str)
            and _PIPELINE_SCOPE.fullmatch(scope) is not None
            # A cutover seeds from its parked position (_read_handoff_deletes).
            and not self._table_migration(table_options)
            # An extract is read only under the modes that take a snapshot from it.
            and not (
                snapshot_mode in _UNL_SNAPSHOT_MODES
                and self._table_snapshot_source(table_options) is not None
            )
        )

    def _full_refresh_delete_seed(
        self, table: Table, table_options: dict[str, str], snapshot_mode: str
    ) -> dict | None:
        # Docstring: The offset a delete reader with no checkpoint starts at in a whole full
        # refresh before its upsert reader has published a start: this update's
        # initialization, which it takes itself if nobody has.
        #
        # A full refresh has no snapshot floor to start at, so the reader waited for its
        # upsert reader, and gave up after _SCHEMA_NODE_FALLBACK_RETRIES empty batches when
        # that flow was queued behind others, or taking a blocking snapshot: the update ended
        # it with nothing, and the deletes committed while the refresh ran, for hours
        # perhaps, waited for the next update, replayed from the floor only while the log
        # still held it. Either reader takes the initialization, elected once, and the
        # upsert reader's snapshot starts there or later, so the deletes from it on are
        # those the emptied target needs: earlier ones remove rows the snapshot does not
        # hold. The mark names this update (floor_seed) and the floor there was beside it
        # (seed_floor), until a snapshot publishes another floor; a start published with the
        # floor unchanged is an upsert reader's that went on from a checkpoint the refresh
        # left (_confirmed_own_start).

        if not self._full_refresh_delete_start(table_options, snapshot_mode):
            return None
        scope = self._pipeline_scope()
        table = self._refresh_table_schema(table, None)
        floor = self._floor_record(table, scope)
        start = self._initial_lsn(table, scope=scope)
        schema_id = self._snapshot_schema_ids[(scope, table.identity)]
        # Announced, then this update's start looked for again: an upsert reader publishes
        # its start, then looks for the announcement, so one of the two sees the other.
        own = self._immutable_namespace(table, "own-start", self._pipeline_id(scope))
        self._publish_own_start(own, table.native_identity, {"own_start": scope})
        published = self._read_upsert_channel_start(table, scope)
        if published is not None:
            self._noted_upsert_starts.pop(own, None)
            self._note_upsert_start(own, table.native_identity, published)
        # 476: Informix delete channel for '{}' starts at this full refresh's start {}, which it
        # took itself: its upsert reader had not published one yet
        logging.getLogger(__name__).info(_m(476, table.exposed_name, start))
        return {
            **_offset(start, start, start, None, "stream", table, schema_id, scope),
            "update_fingerprint": _schema_fingerprint(table),
            "floor_seed": scope,
            # The floor beside which it was taken, "" for none: a floor other than this one
            # is a snapshot's since, at or above this start (_confirmed_own_start).
            _SEED_FLOOR_FIELD: str((floor or {}).get("snapshot_scope") or ""),
        }

    def _confirmed_floor_seed(
        self, table: Table, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[dict, str | None]:
        # Docstring: A floor-seeded delete position checked against the pipeline's floor:
        # ``(offset to read from, floor update still to check)``.
        #
        # The upsert reader publishes a new snapshot's floor before its start
        # (_publish_snapshot_boundary), so a start with the floor unchanged means the
        # target still holds the snapshot the position was seeded from: the mark is
        # dropped. A floor another update has written means the target was emptied and
        # snapshotted again (a full refresh that ended before this table's flows ran, then
        # this update): the position belongs to the old snapshot, on a rebuilt source
        # perhaps to another log, and the reader starts again as one with no checkpoint.
        # That is told from the floor alone, before this update's start: read on, the old
        # position could fail on a log that no longer holds it. An auto_snapshot resnapshot's
        # floor leaves the destination's rows, and stands for the snapshot before it too
        # (_floor_scopes).
        #
        # A replay repeats the batch Spark logged, told by the end it logged: one that read
        # on from the seed carries it, or, the floor confirmed, ends on a transaction or below
        # a floor replaced since without it; one that started again carries another floor's
        # seed or none, on its seed.
        # A seed from another pipeline (a table.migration checkpoint) is one that pipeline's
        # floor confirmed: this pipeline's floor says nothing of it, and the position stands.

        seeded = start_offset["floor_seed"]
        unmarked = {k: v for k, v in start_offset.items() if k not in _SEED_MARKS}
        pipeline = self._pipeline_id(self._pipeline_scope(start_offset))
        if self._pipeline_id(seeded) != pipeline:
            return unmarked, None
        record = self._floor_record(table, seeded)
        current = None if record is None else record.get("snapshot_scope")
        if _SEED_FLOOR_FIELD in start_offset:
            return self._confirmed_own_start(table, start_offset, unmarked, record, table_options)
        if current is None:
            return start_offset, seeded
        if seeded not in self._floor_scopes(record):
            return self._replaced_floor_seed(
                table, start_offset, unmarked, record, table_options
            )
        if self._read_upsert_channel_start(table, self._pipeline_scope(start_offset)) is None:
            return start_offset, seeded
        # The floor again: a snapshot publishes its floor, then its start, the order read here.
        if self._floor_scopes(self._floor_record(table, seeded)) != self._floor_scopes(record):
            return self._confirmed_floor_seed(table, start_offset, table_options)
        return unmarked, None

    def _replaced_floor_seed(
        self,
        table: Table,
        start_offset: dict,
        unmarked: dict,
        record: dict,
        table_options: dict[str, str],
    ) -> tuple[dict, str | None]:
        # Docstring: A seeded delete position whose floor another update's snapshot has
        # replaced: it starts again, at the new one, but a replay does what its batch did.

        seeded = start_offset["floor_seed"]
        stop = self._replay_stop_lsn(table_options)
        logged, on_transaction = json.loads(
            table_options.get(_REPLAY_END_SEED_OPTION) or "[null, false]"
        )
        if stop is not None and logged == seeded:
            return start_offset, seeded
        if (
            stop is not None
            and logged is None
            # A batch that started again ends on its seed, which holds no transaction;
            # the new floor's page-start boundary can lie at or below its end.
            and (
                on_transaction
                or stop < self._immutable_lsn(record, "start_lsn", table.exposed_name)
            )
        ):
            return unmarked, None
        # 291: Informix delete channel for '{}' started at a snapshot floor that update {}
        # has since replaced; it starts again at the new one
        logging.getLogger(__name__).warning(
            _m(291, table.exposed_name, record.get("snapshot_scope"))
        )
        # The new floor's update, to mark where it starts again with (_read_table_deletes).
        return {}, record.get("snapshot_scope")

    def _confirmed_own_start(
        self,
        table: Table,
        start_offset: dict,
        unmarked: dict,
        record: dict | None,
        table_options: dict[str, str],
    ) -> tuple[dict, str | None]:
        # Docstring: _confirmed_floor_seed for a position seeded at a full refresh's own start
        # (_full_refresh_delete_seed), whose seed_floor names the floor beside it.
        #
        # The refresh's floor means the target holds its snapshot, taken at or above the
        # start: the position stands, and the mark goes (also where that floor was the one
        # beside the seed, published just before it). The floor unchanged: a start published
        # by the refreshing update or this one is an upsert reader's that went on from a
        # checkpoint the refresh left (a table set not to reset, one a migration brought),
        # and the reader starts again as one with no checkpoint, at the lower of the floor
        # and that start, or it skipped the deletes between; neither yet, it reads on. A
        # floor of a third update is a snapshot of a target the refresh emptied and then
        # failed to fill: the position may be past the log it kept, on a rebuilt source, or
        # under a layout since changed, and it starts again at that floor, as a floor seed
        # does. A start again here returns the seeded update, for the bootstrap: its start
        # lowers the restart, and one not below the position reads on from it instead
        # (_read_table_deletes_from). A replay does what its batch did: one that kept the
        # mark keeps it; one that started again ended at or below its start, or seeded, and
        # is not read (_range_emits_nothing), so with the floor unchanged the batch read on.

        seeded = start_offset["floor_seed"]
        scopes = self._floor_scopes(record)
        replaying = self._replay_stop_lsn(table_options) is not None
        logged, _ = json.loads(table_options.get(_REPLAY_END_SEED_OPTION) or "[null, false]")
        if replaying and logged == seeded:
            return start_offset, seeded
        if seeded in scopes:
            return unmarked, None
        # A resnapshot floor that replaced none kept the target as a seed beside none found it,
        # so its None stands for that seed's "": the position goes on, as one marked with a
        # kept snapshot does, and doesn't skip the deletes the kept rows need.
        if scopes[0] and start_offset[_SEED_FLOOR_FIELD] not in [scope or "" for scope in scopes]:
            return self._replaced_floor_seed(table, start_offset, unmarked, record, table_options)
        if replaying:
            return unmarked, None
        if self._own_start_upsert_start(table, seeded) is None:
            return start_offset, seeded
        if self._floor_scopes(self._floor_record(table, seeded)) != scopes:
            return self._confirmed_floor_seed(table, start_offset, table_options)
        return {}, seeded

    def _read_table_deletes_from(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        *,
        snapshot_mode: str = "initial",
        bootstrap_offset: dict | None = None,
        restart_from: dict | None = None,
    ) -> tuple[Iterator[dict], dict]:
        # ``start_offset`` has already been through _effective_start_offset, which
        # collapses an all-advisory offset to {} so a retry looks like a fresh
        # bootstrap positionally. The bootstrap deferral counter lives in exactly such
        # an offset, so it has to be read from the raw one instead or it would reset to
        # zero on every retry and the fallback would never become eligible.
        bootstrap_offset = start_offset if bootstrap_offset is None else bootstrap_offset
        table = self._table(table_name, table_options)
        self._touch_table_state(table)
        _ensure_materializable(table, self._with_connection_scan(table_options))
        if self._append_only_table(table, table_options):
            # The framework does not open a delete flow for ingestion_type="append",
            # so reaching here means the two sides disagree about this table. Returning
            # an empty batch would look like "no deletes" forever; fail loudly instead.
            # 197: Table '{}' is configured for append-only ingestion ({}), which has no delete
            # channel
            raise ValueError(_m(197, table_name, _APPEND_INGESTION_OPTION))
        if not _cdc_capable(table):
            # 198: Table '{}' lacks a primary key or has columns unsupported by Informix CDC and is
            # snapshot-only
            raise ValueError(_m(198, table_name))
        if (
            snapshot_mode == "auto_snapshot"
            and start_offset.get("phase") == "stream"
            and self._checkpoint_requires_snapshot(start_offset)
        ):
            channel_start = (
                None
                if self._replay_stop_lsn(table_options) is not None
                else self._read_upsert_channel_start(table, self._pipeline_scope())
            )
            if channel_start is None or channel_start[0] < self._bridge.minimum_lsn():
                return self._read_resnapshotted_deletes(table, start_offset, table_options)
            # The upsert reader goes on from the retained log this update, so no boundary
            # replaces the expired checkpoint (a resnapshot, if any, committed earlier): seed
            # as a delete reader with no checkpoint does, at the floor under any update and
            # under the floor's own layout, or at the upsert reader's start (200) once the
            # floor has aged out. The pipeline's floor alone failed those three (427).
            self._schema_transition_waits.pop((table.identity, "resnapshot"), None)
            start_offset = {}
        # Lakeflow checkpoints this independently from read_table(). The
        # upsert reader owns initialization and publishes the table boundary
        # through durable shared state; delete readers only consume it.
        if not start_offset:
            channel_start = (
                self._own_start_upsert_start(table, restart_from["floor_seed"])
                if restart_from
                else self._read_upsert_channel_start(table, self._pipeline_scope())
            )
            if channel_start is None:
                # The two Lakeflow flows have independent Spark checkpoints.  A
                # delete flow with no checkpoint must therefore wait until the
                # upsert reader publishes the position it is *actually* using in
                # this update.  Inferring from current_lsn or a scope-independent
                # schema node can start ahead of a resumed upsert checkpoint and
                # permanently skip old-key deletes after a cancelled refresh. The
                # pipeline's snapshot floor is never ahead of it: with one, start there.
                seed = self._delete_floor_seed(table) or self._full_refresh_delete_seed(
                    table, table_options, snapshot_mode
                )
                if seed is not None:
                    return iter(()), seed
                return iter(()), self._schema_node_fallback_offset(bootstrap_offset)
            high_water, schema_id, fingerprint = channel_start
            # The upsert channel-start records the upsert reader's *resume* position
            # for the current scope, which the restart/replay cycles push forward past
            # the snapshot boundary. Bootstrapping a delete flow there skips before-image
            # deletes for rows the snapshot already wrote -- notably a mutated-primary-key
            # row's old-key delete, which orphans the pre-update version permanently.
            # Anchor instead at the durable snapshot floor for this pipeline when one is
            # recorded: it is the LSN the current destination rows were captured at, is
            # never above the upsert resume LSN, and -- because apply_as_deletes is
            # idempotent (ordered by sequence_by) -- a lower start only ever re-reads, it
            # cannot remove a newer row or apply a delete twice. This is a bootstrap-only
            # cost; once the delete flow checkpoints, it resumes from its own cursor.
            minimum = self._bridge.minimum_lsn()
            # A start again (477) at a start not below its position reads on from it instead:
            # starting there skipped the deletes between, and a replay could not tell the
            # batch from a read on. One that waits stays where it is, marked: an offset of
            # advisory fields lost the own start, and the next batch started again above it.
            # Below its commit_lsn, not its begin_lsn: reading on drops every transaction
            # committed at or below that (_recover), the deletes from a start between too.
            position = {k: v for k, v in (restart_from or {}).items() if k not in _SEED_MARKS}
            below = int(position.get("commit_lsn") or 0)
            if snapshot_mode == "auto_snapshot" and high_water < minimum:
                # auto_snapshot replaces the expired checkpoint the channel start names (it was
                # published before the resnapshot, and the lowest start wins) with a snapshot
                # whose boundary the upsert reader publishes: seed there once it has, and
                # until then yield a retry that does not run out, as a checkpointed reader of
                # an expired position does (_read_resnapshotted_deletes).
                seed = self._resnapshot_delete_seed(table, self._pipeline_scope())
                if restart_from and not seed:
                    # Marked, and for a triggered update a retry that keeps it waiting (427
                    # bounds it): the position back unchanged ended the flow at once.
                    return iter(()), self._resnapshot_retry_offset(table, restart_from)
                if restart_from and int(seed["commit_lsn"]) >= below:
                    return self._read_table_deletes_from(
                        table_name, position, table_options, snapshot_mode=snapshot_mode
                    )
                return iter(()), seed or self._resnapshot_retry_offset(table, bootstrap_offset)
            floor_state = self._read_delete_snapshot_floor(table, schema_id, fingerprint)
            floor = floor_state[0] if floor_state else None
            bootstrap_lsn = high_water
            if floor is not None and floor < minimum:
                # The floor has aged below the retained log, so the upsert channel-start
                # is the only boundary left. Deletes committed between the two are gone
                # from the source and cannot be replicated: say so, loudly, rather than
                # bootstrapping ahead of them in silence.
                # 200: Informix delete snapshot floor {} for '{}' precedes the minimum retained LSN
                # {}; bootstrapping the delete channel at the upsert resume position {} instead, so
                # deletes committed between the floor and it can no longer be replicated. Run a full
                # refresh if the destination must match the source exactly
                logging.getLogger(__name__).warning(
                    _m(200, floor, table.exposed_name, minimum, high_water),
                )
            elif floor is not None and floor < high_water:
                # The range between them was never consumed by a delete checkpoint, so it
                # is re-read from the floor. Visible in the event log because it can be
                # long (days of log after a stalled bootstrap) and lowers the delete
                # shard's shared restart under the sharded daemon.
                # Under the floor's own schema, which the channel start's can descend from.
                bootstrap_lsn, schema_id, fingerprint = floor_state
                # 201: Informix delete channel for '{}' bootstraps at the snapshot floor {} rather
                # than the upsert resume position {}: no delete checkpoint has consumed that range
                # yet, so it is re-read (idempotent for the target)
                logging.getLogger(__name__).info(
                    _m(201, table.exposed_name, floor, high_water),
                )
            if restart_from and bootstrap_lsn >= below:
                return self._read_table_deletes_from(
                    table_name, position, table_options, snapshot_mode=snapshot_mode
                )
            if bootstrap_lsn < minimum:
                # 202: Informix delete bootstrap boundary {} for '{}' precedes the minimum retained
                # LSN {}; the delete channel is waiting for a resnapshot
                logging.getLogger(__name__).warning(
                    _m(202, bootstrap_lsn, table.exposed_name, minimum),
                )
                return iter(()), restart_from or self._schema_node_fallback_offset(bootstrap_offset)
            current = self._bridge.current_lsn()
            if (bootstrap_lsn >> 32) > (current >> 32):
                # 203: Informix delete bootstrap boundary {} for '{}' is ahead of the source's
                # current logical log {}; the delete channel is waiting for a new snapshot
                # generation
                logging.getLogger(__name__).warning(
                    _m(203, bootstrap_lsn, table.exposed_name, current),
                )
                return iter(()), restart_from or self._schema_node_fallback_offset(bootstrap_offset)
            # The seed is this update's first offset for the flow: see _read_stream.
            return iter(()), {
                **_offset(
                    bootstrap_lsn,
                    bootstrap_lsn,
                    bootstrap_lsn,
                    None,
                    "stream",
                    self._bootstrap_seed_table(table, schema_id, fingerprint),
                    schema_id,
                    self._pipeline_scope(),
                ),
                "update_fingerprint": _schema_fingerprint(table),
            }
        if start_offset.get("phase") == "snapshot":
            snapshot_checkpoint = _validated_offset(start_offset)
            expected = snapshot_checkpoint.get("schema_fingerprint")
            if expected is None:
                # 204: Informix delete checkpoint for '{}' predates schema-safe offsets; run a full
                # refresh
                raise InformixError(_m(204, table.exposed_name))
            table = self._refresh_table_schema(table, expected)
            high_water = int(start_offset.get("snapshot_lsn", start_offset.get("commit_lsn", 0)))
            start_offset = _offset(
                high_water,
                high_water,
                high_water,
                None,
                "stream",
                table,
                str(snapshot_checkpoint["schema_id"]),
                self._pipeline_scope(snapshot_checkpoint),
            )
        return self._read_stream(table, start_offset, table_options, deletes=True)

    def _release_worker_connection(self) -> None:
        # sys.exception() is Python 3.11+, while the connector supports 3.10.
        primary_error = sys.exc_info()[1]
        if self._bridge_instance is None:
            return
        release = getattr(self._bridge_instance, "release_connection", None)
        if not callable(release):
            # 205: Configured Informix bridge does not implement release_connection()
            contract_error = InformixError(_m(205))
            if primary_error is None:
                raise contract_error
            add_informix_exception_note(primary_error, str(contract_error))
            return
        try:
            release()  # pylint: disable=not-callable
        except Exception as cleanup_error:
            if primary_error is None:
                raise
            add_informix_exception_note(
                primary_error,
                f"Releasing the Informix worker connection also failed: {cleanup_error}",
            )

    def _reset_lakebase_connection(self, owner: Any = None) -> None:
        # Docstring: Drop the cached Lakebase state connection so the next use reopens fresh.
        #
        # ``owner`` is whose connection to drop: this connector's by default, or a
        # discarded bridge's.
        #
        # A psycopg connection left open across a long, idle wait -- worse, left
        # idle inside the implicit read transaction that a bare SELECT starts --
        # is reaped by Lakebase's ``idle_in_transaction_session_timeout``, and
        # ``.closed`` stays False until the next query fails, so
        # ``_lakebase_connection`` cannot tell it is dead and hands it back. The
        # snapshot-drain consumer blocks for the whole drain without touching
        # state, so it closes the connection here and lets the post-wait manifest
        # read reopen a guaranteed-live one.

        owner = self if owner is None else owner
        connection = getattr(owner, "_lakebase_conn", None)
        owner._lakebase_conn = None
        if connection is not None:
            try:
                connection.close()
            except Exception:  # pragma: no cover - best-effort teardown
                pass

    def _write_handoff_token(
        self,
        table: Table,
        offset: dict,
        channel: int = _HANDOFF_CHANNEL_UPSERT,
        *,
        keyed: bool = False,
    ) -> None:
        # Docstring: Park a stream-phase offset so a later flow can resume from it.

        _, table_key = self._table_state_keys(table)
        namespace = self._lakebase_state_namespace()
        # The position alone: a busy flow's retry counters went on into the new flow's seed.
        position = {k: v for k, v in offset.items() if k not in _OFFSET_ADVISORY_FIELDS}
        # A keyed flow's token says so: a delete flow with no checkpoint at capture parks no
        # token, which the append cutover would otherwise take for a table with none.
        if keyed:
            position[_HANDOFF_KEYED] = True
        offset_json = json.dumps(_validated_offset(position))
        schema_fp = str(offset.get("schema_fingerprint") or _schema_fingerprint(table))
        self._state(
            put_cdc_handoff,
            namespace,
            table_key,
            channel,
            offset_json,
            schema_fp,
            operation="handoff token write",
        )

    def _read_handoff_token(
        self, table: Table, channel: int = _HANDOFF_CHANNEL_UPSERT
    ) -> dict[str, str] | None:
        # Docstring: Return the parked handoff token for this table, or None when absent.

        _, table_key = self._table_state_keys(table)
        namespace = self._lakebase_state_namespace()
        return self._state(
            get_cdc_handoff,
            namespace,
            table_key,
            channel,
            operation="handoff token read",
        )

    def _delete_handoff_token(self, table: Table, channel: int = _HANDOFF_CHANNEL_UPSERT) -> None:
        # Docstring: Drop a consumed handoff token. Best-effort and idempotent.

        _, table_key = self._table_state_keys(table)
        namespace = self._lakebase_state_namespace()
        self._state(
            delete_cdc_handoff,
            namespace,
            table_key,
            channel,
            operation="handoff token delete",
        )

    def _append_migration_record_and_stop(
        self, table: Table, effective_start: dict
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: Record-and-stop for an append flow at stream phase under table.migration.
        #
        # Migration never streams, so emit no rows and return the same offset. Capture and
        # the committed cutover seed are told apart by ``handoff_seeded`` exactly as on the
        # keyed path in :meth:`_read_handoff`:
        #
        # * **Capture** (no marker) -- this append flow is the *source* of a relocation (an
        #   already-append table moving destination), so park its current stream offset for
        #   the new flow to seed from. Append has no delete channel, so only the upsert
        #   offset is parked.
        # * **Cutover, seed committed** (marker present) -- drop the now-consumed token(s).
        #   Crash-safe: a crash before the seed committed re-seeds from the still-present
        #   token, and a later full refresh under table.migration then finds no token and
        #   fails closed rather than silently re-seeding. The delete-channel token is dropped
        #   too: an SCD1->append switch parked one while the table was still keyed, and an
        #   append flow has no delete reader to consume it. The marker is kept on the
        #   outgoing offset so a later steady-state read stays a stop.

        if effective_start.get("handoff_seeded"):
            self._delete_handoff_token(table)
            self._delete_handoff_token(table, _HANDOFF_CHANNEL_DELETE)
        else:
            self._write_handoff_token(table, effective_start)
        return iter(()), dict(effective_start)

    def _drop_orphaned_handoff_tokens(self, table: Table, channels: list[int]) -> None:
        # Docstring: Best-effort, one-shot drop of a handoff token left behind after Restore.
        #
        # The committed seed drops the token from inside ``_read_handoff`` /
        # ``_read_handoff_deletes`` -- but only while ``table.migration`` is still set.
        # If a cutover run committed the seed without a later same-mode read to consume it
        # (and Restore then cleared table.migration, so reads no longer enter those methods),
        # the
        # token would linger: it has no TTL, and a *future* cutover of the same table could
        # adopt it. So when a post-Restore read still carries the ``handoff_seeded`` marker,
        # drop the token here. This is one-shot -- the marker is not carried onto this read's
        # output offset -- and best-effort: a lingering token never justifies failing a read.

        for channel in channels:
            try:
                self._delete_handoff_token(table, channel)
            except Exception:  # pylint: disable=broad-except
                # 206: Best-effort cleanup of a lingering handoff token for '{}' (channel {})
                # failed; it will be retried on a later read
                logging.getLogger(__name__).warning(
                    _m(206, table.exposed_name, channel),
                    exc_info=True,
                )

    def _handoff_seed_from_token(self, table: Table, token: dict[str, str]) -> dict:
        # Docstring: Build the seed offset from a parked token, failing closed on schema drift.
        #
        # The parked ``schema_fp`` is the source schema at capture time. If the source
        # schema changed before cutover the parked LSN position no longer aligns with the
        # current layout, so resuming would misread the stream -- fail closed and tell the
        # operator to re-capture or start fresh instead of silently corrupting the seam.

        current = self._refresh_table_schema(table, None)
        parked_fp = token.get("schema_fp")
        if parked_fp and parked_fp != _schema_fingerprint(current):
            # 207: table.migration for '{}' cannot resume: the source schema changed between capture
            # and cutover, so the parked position no longer aligns with the current schema. Re-run
            # the capture step against the current schema, or start fresh (clear table.migration and
            # use snapshot.mode=cdc_only for no history, or snapshot.mode=initial for a full
            # snapshot).
            raise InformixError(_m(207, table.exposed_name))
        # Adopt this flow's scope, and stamp a one-shot marker so the *next* read on this
        # flow (once Spark has committed the seed) recognises it as a cutover continuation
        # rather than a fresh capture -- the two are otherwise indistinguishable on the
        # keyed path, where capture and cutover share _read_handoff. The token is dropped
        # only when that marked offset arrives, so a crash before commit re-seeds.
        seeded = _validated_offset(json.loads(token["offset_json"]))
        seeded.pop(_HANDOFF_KEYED, None)
        # Where the pages live: a serve relocated once already keeps them under its first
        # capture's scope (stage_scope), not under the scope that parked it again.
        origin_scope = seeded.get("stage_scope") or seeded.get("pipeline_scope")
        seeded["pipeline_scope"] = self._pipeline_scope()
        if seeded.get("phase") == "snapshot" and origin_scope:
            # A blocking initial snapshot parked mid-serve. The new flow owns a fresh
            # scope for its boundary and stream, but the staged pages physically live
            # under the capturing flow's scope. stage_scope redirects *only* the
            # page/manifest reads and consumed-page cleanup there; it is dropped at the
            # snapshot->stream transition, after which the offset is an ordinary
            # new-scope stream offset. See _snapshot_page_batch / _staged_snapshot_result.
            seeded["stage_scope"] = origin_scope
        seeded["handoff_seeded"] = True
        return seeded

    def _read_handoff(self, table: Table, effective_start: dict) -> tuple[Iterator[dict], dict]:
        # Docstring: Both sides of ``table.migration`` for a **keyed** (merge) CDC flow.
        #
        # A keyed table keeps its ``scd_type`` across a destination relocation, so both
        # the capture (old flow) and the cutover (new flow) reach here -- unlike the
        # SCD1->append switch, whose cutover changes ingestion type and lands in
        # :meth:`_read_append_only`. Migration **never streams and never snapshots**: every
        # read records a checkpoint, emits no rows, and stops, or fails closed. Streaming
        # (and finishing an in-flight snapshot) resumes only once the operator clears
        # ``table.migration`` (the "Restore" step), after which the flow reads
        # normally from the checkpoint recorded here. The states:
        #
        # * **Capture** (old flow) -- a stream-phase offset (steady-state CDC, or an
        #   in-flight incremental copy whose ``incremental`` block fully describes a
        #   resumable position), or a blocking ``initial`` snapshot parked mid-serve
        #   (``phase=="snapshot"``, whose staged pages are portable *by reference* -- a new
        #   flow reads them under the capturing scope via ``stage_scope``). Park it and
        #   stop: an empty batch at the same offset is the AvailableNow "caught up, stop"
        #   signal, so the parked offset equals the flow's committed position exactly.
        # * **Cutover seed** (new flow) -- an empty checkpoint with a parked token. Adopt
        #   the parked offset as this flow's checkpoint, emit no rows, and stop. The token
        #   is kept (not dropped) so a crash before the seed commits re-seeds rather than
        #   failing closed; a ``handoff_seeded`` marker on the seed lets the next read drop
        #   it (see :meth:`_handoff_seed_from_token`).
        # * **Seed committed** (new flow) -- the marker is present, so the seed committed.
        #   Drop the now-consumed token and stop, still emitting no rows. The marker is
        #   **kept on the outgoing offset** so a later steady-state read is not mistaken for
        #   a fresh capture and re-parked; the flow idles here until Restore clears
        #   table.migration.
        # * **Fail closed** -- an empty checkpoint with no parked token means the flow has
        #   not reached a hand-off-able position (steady-state streaming, an in-flight
        #   incremental copy, or a mid-serve blocking snapshot). There is nothing to record,
        #   so raise rather than silently snapshotting: the operator must run the flow
        #   without table.migration until it is streaming (or mid initial snapshot), then
        #   set it.

        if not _cdc_capable(table):
            # 208: table.migration requires a CDC-capable table; '{}' is snapshot-only
            raise ValueError(_m(208, table.exposed_name))
        if effective_start and effective_start.get("handoff_seeded"):
            # The committed seed has arrived: the token has served its purpose, so drop it
            # (crash-safe -- a crash before this re-seeds from the still-present token).
            # Record and stop: no rows, same offset back. Keep the marker so a later
            # steady-state read is not mistaken for a fresh capture and re-parked. Streaming
            # resumes only after Restore clears table.migration.
            self._delete_handoff_token(table)
            return iter(()), dict(effective_start)
        if not effective_start:
            token = self._read_handoff_token(table)
            if token is not None:
                seeded = self._handoff_seed_from_token(table, token)
                # The seed is this update's resume position, as a checkpoint's is (see
                # _read_table_attempt): a delete reader with no token of its own bootstraps
                # from it now, not after this reader's next read -- which, for a seed taken
                # mid-snapshot, comes only once Restore has let the snapshot finish.
                self._publish_upsert_channel_start(
                    table,
                    int(seeded.get("begin_lsn") or seeded["commit_lsn"]),
                    str(seeded["schema_id"]),
                    str(seeded["schema_fingerprint"]),
                    self._pipeline_scope(),
                )
                return iter(()), seeded
            # 209: table.migration=true for '{}' has no checkpoint to record and no parked token:
            # the flow has not reached a hand-off-able position. Run it without table.migration
            # until it is streaming (or mid initial snapshot), then set table.migration=true so it
            # parks its offset -- or start fresh with snapshot.mode=cdc_only (no history) or
            # snapshot.mode=initial (full snapshot).
            raise InformixError(_m(209, table.exposed_name))
        # A stream-phase offset, or a blocking initial snapshot parked mid-serve
        # (phase=="snapshot"): both are portable, so park the offset as the token and
        # stop. A snapshot offset's staged pages stay under this (capturing) scope; the
        # cutover records that scope on stage_scope so the new flow can read them, and
        # after Restore finishes the snapshot from the parked page_index before streaming.
        self._write_handoff_token(table, effective_start, keyed=True)
        return iter(()), dict(effective_start)

    def _read_handoff_deletes(
        self,
        table_name: str,
        effective_start: dict,
        options: dict[str, str],
        bootstrap_offset: dict,
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: Delete-channel side of ``table.migration``.
        #
        # Mirrors :meth:`_read_handoff` on the independently checkpointed delete reader,
        # so a keyed relocation that keeps its ``scd_type`` records the delete position
        # across the seam too -- so that, after Restore, the delete reader resumes at
        # *exactly* where the old flow's delete reader stopped, rather than restarting from
        # the upsert boundary (which could skip deletes that lay between the two readers'
        # independent positions). Like :meth:`_read_handoff`, it never streams: it records a
        # checkpoint, emits no rows, and stops. The delete token uses a separate channel
        # from the upsert token.
        #
        # The SCD1->append switch has no delete flow at all, so this is only reached for
        # a table that keeps a delete channel; a snapshot-only or append table freezes.

        table = self._table(table_name, options)
        if self._append_only_table(table, options) or not _cdc_capable(table):
            # No delete channel to hand off; freeze at the current offset like any other
            # handoff read so the flow stops without advancing.
            return iter(()), dict(effective_start)
        if effective_start and effective_start.get("handoff_seeded"):
            # Seed committed: drop the delete-channel token and stop, like _read_handoff.
            # Keep the marker so a later read is not mistaken for a capture and re-parked.
            self._delete_handoff_token(table, _HANDOFF_CHANNEL_DELETE)
            return iter(()), dict(effective_start)
        if not effective_start:
            token = self._read_handoff_token(table, _HANDOFF_CHANNEL_DELETE)
            if token is not None:
                return iter(()), self._handoff_seed_from_token(table, token)
            # No parked delete position (e.g. capture ran before the delete reader had a
            # checkpoint): fall back to the ordinary bootstrap, which adopts the upsert
            # channel's newly published boundary under this flow's scope.
            rows, seeded = self._read_table_deletes(
                table_name,
                effective_start,
                options,
                snapshot_mode="cdc_only",
                bootstrap_offset=bootstrap_offset,
            )
            if "commit_lsn" in seeded:
                # A seed, marked as a token's is, so the next read during the migration
                # keeps it rather than parking it as a delete token of its own.
                seeded = {**seeded, "handoff_seeded": True}
            return rows, seeded
        if effective_start.get("phase") == "stream":
            self._write_handoff_token(table, effective_start, _HANDOFF_CHANNEL_DELETE)
        return iter(()), dict(effective_start)

    def _read_cdc_only(
        self, table: Table, table_options: dict[str, str] | None = None
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: Establish a schema-safe CDC boundary without reading existing rows.

        pipeline_scope = self._pipeline_scope()
        table = self._refresh_table_schema(table, None)
        if not _cdc_capable(table):
            # 210: snapshot.mode=cdc_only requires a CDC-capable table; '{}' is snapshot-only
            raise ValueError(_m(210, table.exposed_name))
        high_water = self._initial_lsn(table, scope=pipeline_scope)
        if high_water is None:  # owner=True always publishes or raises
            # 211: Unable to establish no-data boundary for '{}'
            raise InformixError(_m(211, table.exposed_name))
        schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
        self._publish_snapshot_boundary(
            table,
            schema_id,
            high_water,
            high_water,
            pipeline_scope,
            floor=not _replay_keeps_floor(table_options),
        )
        return iter(()), _offset(
            high_water,
            high_water,
            high_water,
            None,
            "stream",
            table,
            schema_id,
            pipeline_scope,
        )

    def _table_state_keys(self, table: Table) -> tuple[str, str]:
        # Docstring: Return the (connection, table) key pair that identifies a table's state.
        #
        # The single source of truth for both. ``_shared_table_state_root`` joins
        # these under the shared state location; snapshot staging joins the same two
        # under the staging location. Deriving them here rather than recovering them
        # by relpath is what lets staging stand alone once the shared state location
        # is gone.

        return self._table_state_keys_for(table.native_identity)

    def _table_state_keys_for(self, native_identity: str) -> tuple[str, str]:
        # Docstring: Identity-string form of :meth:`_table_state_keys`.
        #
        # Depends only on ``native_identity`` and the connection options (both
        # available on a Spark executor), so the staged-page read path can run there
        # without a ``Table`` -- i.e. without a live Informix catalog lookup.

        namespace = "\0".join(
            (
                "v2",
                self.options.get("hostname", "").strip().rstrip(".").casefold(),
                str(int(self.options.get("port") or 9088)),
                self.options.get("server", "").strip(),
                self.options.get("database", "").strip(),
            )
        )
        return (
            hashlib.sha256(namespace.encode()).hexdigest()[:24],
            hashlib.sha256(native_identity.encode()).hexdigest()[:24],
        )

    def _snapshot_stage_namespace(self, table: Table, pipeline_scope: str, schema_id: str) -> str:
        return self._snapshot_stage_namespace_for(table.native_identity, pipeline_scope, schema_id)

    def _snapshot_stage_namespace_for(
        self, native_identity: str, pipeline_scope: str, schema_id: str
    ) -> str:
        # Computed directly from the table's identity rather than recovered by
        # relpath against the shared state location, so staging no longer depends
        # on that location existing at all. The resulting path is byte-identical
        # to what the relpath form produced.
        connection_key, table_key = self._table_state_keys_for(native_identity)
        return os.path.join(
            self._snapshot_staging_location,
            connection_key,
            table_key,
            "snapshot-page-data",
            pipeline_scope,
            schema_id,
        )

    def _snapshot_manifest_namespace(
        self, table: Table, pipeline_scope: str, schema_id: str
    ) -> str:
        # Docstring: Record key for a staged snapshot's manifest.
        #
        # The manifest is metadata -- page count, row count, snapshot LSN -- so it
        # lives with the rest of the state rather than beside the page payloads it
        # describes. Keying it by identity instead of by staging path means moving
        # or repointing the staging Volume does not orphan it.

        connection_key, table_key = self._table_state_keys(table)
        return "/".join(
            (
                connection_key,
                table_key,
                "snapshot-page-manifest",
                pipeline_scope,
                schema_id,
            )
        )

    def _cleanup_completed_snapshot_stage(self, table: Table, checkpoint: dict[str, Any]) -> None:
        validated = _validated_offset(checkpoint)
        # The scope that staged the pages: a snapshot finished in a later update than it
        # started keeps the starting update's scope on its offsets, and one served for a
        # relocation names the capturing flow's (see _naming_scan).
        scope = validated.get("staged_scope") or self._offset_scopes(validated)[1]
        schema_id = str(validated["schema_id"])
        path = self._snapshot_stage_namespace(table, scope, schema_id)
        if path in self._cleaned_snapshot_stages:
            return
        # The serve path deletes consumed pages on a background pool (see
        # _cleanup_consumed_snapshot_stage_pages), so a page can vanish while this walk is
        # visiting it, and the walk then stops part-way with FileNotFoundError. Walk again
        # until the stage is gone, and record it as cleaned only then, so the rest of it
        # is not left behind for the retention sweep.
        for _ in range(_SNAPSHOT_STAGE_CLEANUP_ATTEMPTS):
            try:
                PurePythonInformixBridge._remove_connection_slot_tree(path)
            except FileNotFoundError:
                if _path_present(path):
                    continue
            except OSError:
                # 212: Cannot remove completed Informix snapshot staging data: {}
                logging.getLogger(__name__).warning(
                    _m(212, path),
                    exc_info=True,
                )
                return
            break
        if _path_present(path):
            # 213: Completed Informix snapshot staging data is still changing; leaving {} for a
            # later read or the retention sweep
            logging.getLogger(__name__).warning(
                _m(213, path),
            )
            return
        self._cleaned_snapshot_stages.add(path)

    def _staging_transport(self) -> WorkspaceFilesApi | None:
        # Docstring: Resolve the snapshot-page staging transport once, caching the choice.
        #
        # Returns a :class:`WorkspaceFilesApi` to stage pages over the Files REST API,
        # or ``None`` to use the FUSE mount. ``fuse`` forces FUSE; ``rest`` forces REST
        # and raises if a token cannot be minted; ``auto`` (default) uses REST when a
        # staging token can be minted and falls back to FUSE when the workspace forbids
        # token creation. The first mint is forced here so a "creation disabled"
        # workspace falls back at resolution time rather than mid-drain.

        if self._staging_transport_resolved:
            files_api = self._staging_files_api
            if (
                files_api is not None
                and getattr(files_api, "rejected", False)
                and not self._staging_rest_required
            ):
                # The token was rejected and nothing could mint a successor (neither
                # the minting token nor the reader's starting credential): stage over
                # FUSE from now on, as when no token could be minted at all.
                # 214: The snapshot-staging token was rejected and can't be replaced; snapshot
                # staging uses the FUSE transport from now on
                logging.getLogger(__name__).warning(_m(214))
                self._staging_files_api = files_api = None
            return files_api
        self._staging_transport_resolved = True
        mode = (
            str(
                self.options.get(
                    _SNAPSHOT_STAGING_TRANSPORT_OPTION, _DEFAULT_SNAPSHOT_STAGING_TRANSPORT
                )
            )
            .strip()
            .lower()
        )
        self._staging_rest_required = mode == "rest"
        if mode == "fuse":
            return None
        provider = self._build_staging_credential_provider(required=mode == "rest")
        if provider is None:
            return None
        files_api = WorkspaceFilesApi(self._staging_workspace_host, provider)
        try:
            provider.bearer()
        except StagingTokenUnavailable:
            if mode == "rest":
                raise
            # 215: Personal access token creation is unavailable; snapshot staging uses the FUSE
            # transport
            logging.getLogger(__name__).info(_m(215))
            return None
        except WorkspaceHttpError as error:
            # A 401 here is the reader's starting credential refused: it lasts about an
            # hour, and the process mints its first tokens from it, at the first stream
            # read (_keep_staging_credential). Stage over FUSE, as when token creation is
            # forbidden.
            if mode == "rest" or error.status_code != 401:
                raise
            # 216: The credential that mints the snapshot-staging token was rejected; snapshot
            # staging uses the FUSE transport
            logging.getLogger(__name__).warning(_m(216))
            return None
        self._staging_files_api = files_api
        return files_api

    def _keep_staging_credential(self) -> None:
        # Docstring: Start this process's staging-token chain while the reader's starting
        # credential is valid, and keep it fresh: each stream read of an upsert flow calls
        # here. A process that first staged after that credential had expired (it lasts
        # about an hour), or whose chain had gone unused past a token's lifetime, could
        # mint nothing, and staged over FUSE. A failure is left to the stage that needs
        # the transport, which resolves it again.

        transport = self.options.get(_SNAPSHOT_STAGING_TRANSPORT_OPTION, "")
        if str(transport).strip().lower() == "fuse":
            return
        # Tried again a minute on, not on every read: a mint retries its request.
        if time.monotonic() < getattr(self, "_staging_keep_retry_at", 0.0):
            return
        try:
            files_api = self._staging_transport()
            if files_api is not None:
                files_api.keep_fresh()
        except Exception:  # pylint: disable=broad-except
            self._staging_transport_resolved = False
            self._staging_keep_retry_at = time.monotonic() + 60.0
            logging.getLogger(__name__).debug(
                "Could not start the snapshot-staging token chain", exc_info=True
            )

    def _staging_fell_back(self, error: WorkspaceHttpError) -> bool:
        # Docstring: Whether a Files API error was a 401 nothing could recover that moved an
        # ``auto`` staging transport to FUSE, so the caller can redo its operation there.
        # ``rest`` keeps its transport, and every other error stands.

        return error.status_code == 401 and self._staging_transport() is None
