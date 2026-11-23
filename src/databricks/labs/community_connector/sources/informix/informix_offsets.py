"""Offset validation, schema-change compatibility, and backlog ranking."""

from __future__ import annotations

import re
from typing import Any

from databricks.labs.community_connector.sources.informix.informix_filters import (
    _EXCLUDE_COLUMNS_OPTION,
    _INCLUDE_COLUMNS_OPTION,
    _RENAME_COLUMNS_OPTION,
    _ROW_FILTER_OPTION,
    _TRANSFORM_COLUMNS_OPTION,
    _TRANSFORM_HASH_KEY_OPTION,
    _snapshot_filter,
    _transform_spark_type,
    _unsupported_column,
    _unsupported_column_list,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _PRIMARY_KEYS_OPTION,
    _PRIMARY_KEYS_WILDCARD,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CONNECTION_BACKLOG_RANK_LEVELS,
    _CONNECTION_BACKLOG_TRUNCATION_LEVELS,
    _OFFSET_VERSION,
    _PIPELINE_SCOPE,
    _SEED_FLOOR_FIELD,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import Table
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _recorded_transforms,
    _wildcard_key,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _strict_lsn,
    _strict_tx_id,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _decimal_types_changed,
    _filter_rewritten_alike,
    _float_filter_regraded,
    _renames_cleanup_only,
    _selection_cleanup_only,
    _shortened,
    _spark_type_widened,
    _transforms_cleanup_only,
    _widened,
)


# pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
def _ensure_compatible_schema_change(
    previous: Table,
    current: Table,
    *,
    extend_wildcard_key: bool = False,
    shrink_wildcard_key: bool = False,
    wildcard_except: tuple[str, ...] | None = (),
) -> None:
    # Docstring: Fail closed unless the stream can adopt ``current`` without a full refresh.
    #
    # Columns are matched by name. Allowed, in any combination:
    #
    # - widening a column so every old value still fits (see :func:`_widened`); a numeric
    #   widening also widens the Spark type, which the destination adopts in place when
    #   the pipeline enables type widening, and otherwise fails that update loudly;
    # - shortening a CHAR, NCHAR, VARCHAR, or NVARCHAR column that isn't part of the key
    #   (see :func:`_shortened`), which is a string in the destination either way. A key
    #   column -- any column, under a wildcard key -- fails closed: Informix truncates its
    #   existing values without logging a change, so landed keys would stop matching;
    # - dropping a non-key column, NOT NULL or not -- the connector stops reporting it, and
    #   the destination keeps it: an SCD1 row updated afterwards keeps its last value there,
    #   while rows inserted and SCD2 versions written afterwards hold NULL. AUTO CDC creates
    #   every target column nullable, even from a NOT NULL source, so NULL always fits;
    # - appending nullable, CDC-supported columns after every surviving one. A column of a
    #   type the connector can't read is left out of ``current`` before this runs, so
    #   adding one changes nothing here.
    #
    # Anything else -- reordering, another type change (a column changed to a type the
    # connector can't read would otherwise pass as dropped), a key column dropped, a NOT NULL
    # or key column added -- fails closed, and so does changing include_columns /
    # exclude_columns or row_filter: rows already landed were selected under the old
    # setting, and the new one cannot be applied to them without a re-read. The selection
    # edits accepted leave the ingested columns as they were: taking out a name whose
    # column no longer exists, or adding or taking out an exclude_columns name of a column
    # the connector leaves out anyway (see _selection_cleanup_only). The key
    # must be unchanged, except that with ``extend_wildcard_key`` a wildcard key -- the whole
    # row, or the whole row but its except(...) columns -- may grow by exactly the appended
    # columns that join it (and nothing dropped), and with ``shrink_wildcard_key`` as well
    # -- the first read of a new update -- shrink by exactly the dropped columns (and
    # nothing appended). A running update keeps merging on the key it started with,
    # so a shorter key mid-update fails until the pipeline restarts with it.

    name = current.exposed_name
    current_names = {column.name for column in current.columns}
    survivors = [column for column in previous.columns if column.name in current_names]
    dropped = [column for column in previous.columns if column.name not in current_names]
    additions = current.columns[len(survivors) :]
    # The identity first: a re-created table can pass for a dropped column, a key that
    # shrank, or a retyped column, and its new table ID is the change to report.
    if (previous.database, previous.owner, previous.name, previous.incarnation) != (
        current.database,
        current.owner,
        current.name,
        current.incarnation,
    ):
        # 375: Informix schema change for '{}' changed table identity or primary keys; run a full
        # refresh
        raise InformixError(_m(375, name))
    # Then a retyped column, ahead of the key: under a whole-row key one left out would
    # otherwise pass for a column dropped from the key. Informix can't convert a column
    # to TEXT or BYTE in place (-608), so this is mostly a DATETIME or INTERVAL qualifier
    # the decoder doesn't handle. A column the history already records with an
    # unsupported type (a build that kept it in the schema) didn't change type: leaving
    # it out now is a drop, accepted below like any other.
    unsupported = {column.name: column for column in current.unsupported_columns}
    retyped = [
        unsupported[column.name]
        for column in dropped
        if column.name in unsupported and not _unsupported_column(column)
    ]
    if retyped:
        # 376: Informix schema change for '{}' changed column(s) to a type the connector can't read:
        # {}; run a full refresh
        raise InformixError(_m(376, name, _unsupported_column_list(retyped)))
    keys_match = previous.primary_keys == current.primary_keys
    shrunk = False
    # A wildcard key is the columns in catalog order, less the names a "* except(...)" one
    # excepts, and every appended column it doesn't except joins it. Each side is judged
    # by its own except list as recorded with its schema, else (a record from before they
    # were kept) by ``wildcard_except``, the live table's: by the live list alone, a step
    # replayed after an edit the README asks for (a dropped column taken out of the list)
    # read as a key change. A changed except list fails here as any other key change does.
    # By "is None", not truthiness: a bare "*" recorded as such has the empty list, and read
    # as unknown it took the live list, say "* except(d)" after a dropped d came back.
    wildcard_except = tuple(wildcard_except or ())
    previous_except = (
        wildcard_except if previous.key_wildcard_except is None else previous.key_wildcard_except
    )
    current_except = (
        wildcard_except if current.key_wildcard_except is None else current.key_wildcard_except
    )
    wildcard_shaped = previous.primary_keys == tuple(
        _wildcard_key(previous.columns, previous_except)
    )
    if not keys_match and extend_wildcard_key and wildcard_shaped:
        added = tuple(_wildcard_key(additions, current_except))
        removed = {column.name for column in dropped}
        if added and not dropped:
            keys_match = current.primary_keys == previous.primary_keys + added
        elif (
            dropped
            and not added
            and current.primary_keys
            == tuple(key for key in previous.primary_keys if key not in removed)
        ):
            if not shrink_wildcard_key:
                # 377: Informix schema change for '{}' dropped {} from its {}='{}' key during a
                # running update; stop and restart the pipeline so the destination merges on the
                # shorter key
                raise InformixError(
                    _m(
                        377,
                        name,
                        (", ".join(column.name for column in dropped)),
                        _PRIMARY_KEYS_OPTION,
                        _PRIMARY_KEYS_WILDCARD,
                    )
                )
            keys_match = shrunk = True
    if not keys_match:
        # 375: Informix schema change for '{}' changed table identity or primary keys; run a full
        # refresh
        raise InformixError(_m(375, name))
    if [column.name for column in current.columns[: len(survivors)]] != [
        column.name for column in survivors
    ]:
        # 378: Informix schema change for '{}' reordered existing columns or added one before them;
        # run a full refresh
        raise InformixError(_m(378, name))
    # _widened accepts what Delta widens in the column's own landed type. A transform can
    # land it as another (ROUND(int_col, -3) as a BIGINT), whose change Delta may refuse:
    # INTEGER to DECIMAL(12,2) is an in-place widening, the BIGINT to the DECIMAL(13,2)
    # ROUND then gives is not, and the update failed in the destination.
    transforms = {
        column: operand
        for column, _, operand in _recorded_transforms(current.column_transforms) or ()
    }
    for old, new in zip(survivors, current.columns):
        if old != new and old.name in transforms and _widened(old, new):
            landed = _transform_spark_type(transforms[old.name], old, {})
            landing = _transform_spark_type(transforms[old.name], new, {})
            if landed != landing and not _spark_type_widened(landed, landing):
                # 464: Informix schema change for '{}' widened column {}, whose transform
                # lands it as {} and would now land it as {}, which Delta can't widen in
                # place; run a full refresh
                raise InformixError(
                    _m(
                        464,
                        name,
                        old.name,
                        landed.simpleString().upper(),
                        landing.simpleString().upper(),
                    )
                )
        if old == new or _widened(old, new):
            continue
        if not _shortened(old, new):
            # 379: Informix schema change for '{}' changed column {} other than by widening it or
            # shortening a string; run a full refresh
            raise InformixError(_m(379, name, old.name))
        if old.name in previous.primary_keys or old.name in current.primary_keys:
            # Informix truncates the existing values without logging a change, so the
            # keys already landed would no longer match the source's.
            wildcard = previous.key_wildcard or current.key_wildcard
            excepting = previous.key_wildcard_except or current.key_wildcard_except
            raise InformixError(
                f"Informix schema change for '{name}' shortened key column {old.name}"
                + (
                    f" (every column it doesn't except is in its {_PRIMARY_KEYS_OPTION}="
                    f"'{_PRIMARY_KEYS_WILDCARD} except(...)' key)"
                    if excepting
                    else f" (every column is in its {_PRIMARY_KEYS_OPTION}="
                    f"'{_PRIMARY_KEYS_WILDCARD}' key)"
                    if wildcard
                    else ""
                )
                + ": Informix truncates existing values without logging a change, so rows "
                "already landed would no longer match by key; run a full refresh"
            )
    if any(not column.nullable or not column.cdc_supported for column in additions):
        # 380: Informix schema change for '{}' added a non-nullable or CDC-unsupported column; run a
        # full refresh
        raise InformixError(_m(380, name))
    for column in dropped:
        if not shrunk and (
            column.name in previous.primary_keys or column.name in current.primary_keys
        ):
            # Whatever its nullability: a primary.keys override or a nullable-index key can
            # name a nullable column, and every row would then key on NULL. A whole-row
            # key that shrank above no longer names the column.
            # 381: Informix schema change for '{}' dropped key column {}; run a full refresh
            raise InformixError(_m(381, name, column.name))
    if previous.column_selection != current.column_selection and not _selection_cleanup_only(
        previous, current
    ):
        # 382: '{}' / '{}' changed for '{}'; run a full refresh so every row is read under the new
        # selection
        raise InformixError(_m(382, _INCLUDE_COLUMNS_OPTION, _EXCLUDE_COLUMNS_OPTION, name))
    if previous.row_filter != current.row_filter and not _filter_rewritten_alike(
        previous, current
    ):
        # 383: '{}' changed for '{}'; run a full refresh so every row is read under the new filter
        raise InformixError(_m(383, _ROW_FILTER_OPTION, name))
    regraded = _float_filter_regraded(previous, current)
    if regraded:
        # 482: Informix schema change for '{}' widened {} to FLOAT, which row_filter compares with
        # {}: as a FLOAT the comparison gives another answer for some values, and rows already
        # landed under the old one would stay; write {} there instead, which gives every value
        # the column held the same answer as both types, or run a full refresh
        raise InformixError(_m(482, name, regraded[0], regraded[1], regraded[2]))
    if (
        previous.column_transforms != current.column_transforms
        and not _transforms_cleanup_only(previous, current)
    ) or (
        # A key id only where HASH is used: taking out the last HASH entry with its dropped
        # column leaves none to compare, and adding one is not a cleanup.
        previous.transform_key_id is not None
        and current.transform_key_id is not None
        and previous.transform_key_id != current.transform_key_id
    ):
        # 384: '{}' or '{}' changed for '{}'; run a full refresh so every row lands under the new
        # transforms
        raise InformixError(_m(384, _TRANSFORM_COLUMNS_OPTION, _TRANSFORM_HASH_KEY_OPTION, name))
    if previous.column_renames != current.column_renames and not _renames_cleanup_only(
        previous, current
    ):
        # 385: '{}' changed for '{}'; run a full refresh so every row lands under the new names
        raise InformixError(_m(385, _RENAME_COLUMNS_OPTION, name))
    if previous.decimal_targets is not None and _decimal_types_changed(previous, current):
        # A schema recorded before the types were (None) takes them up. Otherwise rows
        # already landed, and staged pages, hold values of the old type.
        # 457: '{}' or '{}' changed for '{}'; run a full refresh so every row lands as the new
        # type
        raise InformixError(_m(457, "decimal.variable.type", "decimal.variable.column.type", name))


def _shared_state_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("shared_state_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 413: Informix offset has an invalid shared_state_retry_count
        raise ValueError(_m(413))
    return value


def _dropped_mount_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("dropped_mount_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 414: Informix offset has an invalid dropped_mount_retry_count
        raise ValueError(_m(414))
    return value


def _schema_node_fallback_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("schema_node_fallback_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 415: Informix offset has an invalid schema_node_fallback_retry_count
        raise ValueError(_m(415))
    return value


def _capacity_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("capacity_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 416: Informix offset has an invalid capacity_retry_count
        raise ValueError(_m(416))
    return value


def _capacity_pressure(offset: dict[str, Any]) -> int:
    # Docstring: Decaying acquisition-pressure signal that feeds the yield budget.
    #
    # Distinct from ``capacity_retry_count``: that counts *consecutive* misses and
    # resets to zero the instant a read gets a slot, because it is the failure
    # guard (a permanently starved flow fails at ``capacity.retry.max.retries``).
    # Pressure instead *decays* on success rather than resetting, so a flow that is
    # contended but occasionally served keeps some of the elevated budget it earned
    # while starved instead of dropping straight back to the least-patient value.
    # Optional and backward compatible: an offset written before this field existed
    # defaults to zero.

    value = offset.get("capacity_pressure", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 417: Informix offset has an invalid capacity_pressure
        raise ValueError(_m(417))
    return value


def _backlog_streak(offset: dict[str, Any]) -> int:
    # Docstring: Validate the run of consecutive reads that filled their row budget.
    #
    # Optional and backward compatible: an offset written before the streak existed,
    # or by a flow that has never truncated, simply has no key and reads as zero.

    value = offset.get("backlog_streak", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 418: Informix offset has an invalid backlog_streak
        raise ValueError(_m(418))
    return value


def _backlog_rank_value(offset: dict[str, Any]) -> int:
    # Docstring: Validate an optional persisted backlog rank.
    #
    # Optional and backward compatible: an offset written before hints existed, or
    # by a reader that found no usable hint, simply has no rank and reads as zero.

    value = offset.get("backlog_rank", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 419: Informix offset has an invalid backlog_rank
        raise ValueError(_m(419))
    if value >= _CONNECTION_BACKLOG_RANK_LEVELS:
        # 420: Informix offset has an out-of-range backlog_rank
        raise ValueError(_m(420))
    return value


def _trigger_boundary_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("trigger_boundary_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 421: Informix offset has an invalid trigger_boundary_retry_count
        raise ValueError(_m(421))
    return value


def _schema_transition_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("schema_transition_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        # 422: Informix offset has an invalid schema_transition_retry_count
        raise ValueError(_m(422))
    return value


def _checked_advisory_fields(offset: dict[str, Any]) -> dict[str, Any]:
    # Docstring: A copy of ``offset`` with each advisory field it holds checked by its own
    # validator, which raises on a malformed value; one list for every caller.

    result = dict(offset)
    for name, check in (
        ("shared_state_retry_count", _shared_state_retry_count),
        ("dropped_mount_retry_count", _dropped_mount_retry_count),
        ("trigger_boundary_retry_count", _trigger_boundary_retry_count),
        ("schema_transition_retry_count", _schema_transition_retry_count),
        ("schema_node_fallback_retry_count", _schema_node_fallback_retry_count),
        ("capacity_retry_count", _capacity_retry_count),
        ("capacity_pressure", _capacity_pressure),
        ("backlog_rank", _backlog_rank_value),
        ("backlog_streak", _backlog_streak),
    ):
        if name in result:
            result[name] = check(result)
    return result


# pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
def _validated_offset(offset: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(offset, dict):
        # 387: Informix offset must be a dictionary
        raise ValueError(_m(387))
    result = _checked_advisory_fields(offset)
    if result.get("version") != _OFFSET_VERSION:
        # 388: Informix offset version {!r} is unsupported; run a full refresh with this connector
        # version
        raise ValueError(_m(388, result.get("version")))
    values = {}
    for key in ("commit_lsn", "change_lsn", "begin_lsn"):
        if key not in result:
            # 389: Informix stream offset is missing '{}'
            raise ValueError(_m(389, key))
        values[key] = _strict_lsn(result[key], key)
    if not values["begin_lsn"] <= values["change_lsn"] <= values["commit_lsn"]:
        # 390: Informix offset must satisfy begin_lsn <= change_lsn <= commit_lsn
        raise ValueError(_m(390))
    phase = result.get("phase")
    if phase not in {"snapshot", "stream"}:
        # 391: Informix offset phase must be 'snapshot' or 'stream'
        raise ValueError(_m(391))
    fingerprint = result.get("schema_fingerprint")
    if fingerprint is not None and (
        not isinstance(fingerprint, str)
        or len(fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in fingerprint)
    ):
        # 392: Informix offset has an invalid schema_fingerprint
        raise ValueError(_m(392))
    layout = result.get("update_fingerprint")
    if layout is not None and (
        not isinstance(layout, str)
        or len(layout) != 64
        or any(character not in "0123456789abcdef" for character in layout)
    ):
        # 424: Informix offset has an invalid update_fingerprint
        raise ValueError(_m(424))
    schema_id = result.get("schema_id")
    if not isinstance(schema_id, str) or not re.fullmatch(r"[0-9a-f]{32}", schema_id):
        # 393: Informix offset has an invalid schema_id; run a full refresh with this connector
        # version
        raise ValueError(_m(393))
    pipeline_scope = result.get("pipeline_scope")
    if not isinstance(pipeline_scope, str) or _PIPELINE_SCOPE.fullmatch(pipeline_scope) is None:
        # 394: Informix offset has an invalid pipeline_scope; run a full refresh with this connector
        # version
        raise ValueError(_m(394))
    # stage_scope, when present, redirects a handoff relocation's staged-page reads to
    # the capturing flow's scope. Only valid on a snapshot-phase offset (it is dropped at
    # the stream transition) and must be a well-formed scope.
    stage_scope = result.get("stage_scope")
    if stage_scope is not None:
        if not isinstance(stage_scope, str) or _PIPELINE_SCOPE.fullmatch(stage_scope) is None:
            # 395: Informix offset has an invalid stage_scope; run a full refresh with this
            # connector version
            raise ValueError(_m(395))
        if phase != "snapshot":
            # 396: Informix offset stage_scope requires the snapshot phase
            raise ValueError(_m(396))
    # scan_scope and scan_schema name the update whose index-ordered scan the stream is
    # still within: they lead to that scan's manifest, so both must be well formed.
    scan_scope, scan_schema = result.get("scan_scope"), result.get("scan_schema")
    if (scan_scope is not None or scan_schema is not None) and not (
        isinstance(scan_scope, str)
        and _PIPELINE_SCOPE.fullmatch(scan_scope) is not None
        and isinstance(scan_schema, str)
        and re.fullmatch(r"[0-9a-f]{32}", scan_schema)
    ):
        # 445: Informix offset has an invalid scan_scope or scan_schema; run a full
        # refresh with this connector version
        raise ValueError(_m(445))
    # staged_scope names the update whose staged pages the stream cleans up (see
    # _naming_scan): checked as the other scopes are, where a bad one was passed over
    # for the flow's own scope and its pages waited for the retention sweep.
    staged_scope = result.get("staged_scope")
    if staged_scope is not None and (
        not isinstance(staged_scope, str) or _PIPELINE_SCOPE.fullmatch(staged_scope) is None
    ):
        # 467: Informix offset has an invalid staged_scope; run a full refresh with this
        # connector version
        raise ValueError(_m(467))
    # floor_seed names the update whose snapshot floor a delete position started at.
    floor_seed = result.get("floor_seed")
    if floor_seed is not None and (
        not isinstance(floor_seed, str) or _PIPELINE_SCOPE.fullmatch(floor_seed) is None
    ):
        # 469: Informix offset has an invalid floor_seed; run a full refresh with this
        # connector version
        raise ValueError(_m(469))
    # seed_floor rides a floor_seed of a full refresh's own start: the floor beside it.
    seed_floor = result.get(_SEED_FLOOR_FIELD)
    if _SEED_FLOOR_FIELD in result and (
        floor_seed is None
        or not isinstance(seed_floor, str)
        or (seed_floor and _PIPELINE_SCOPE.fullmatch(seed_floor) is None)
    ):
        # 469: Informix offset has an invalid floor_seed; run a full refresh with this
        # connector version
        raise ValueError(_m(469))
    trigger_generation = result.get("trigger_generation")
    if trigger_generation is not None and (
        not isinstance(trigger_generation, str)
        or not re.fullmatch(r"[0-9a-f]{32}", trigger_generation)
    ):
        # 397: Informix offset has an invalid trigger_generation
        raise ValueError(_m(397))
    trigger_high_water = result.get("trigger_high_water")
    if trigger_generation is None:
        if trigger_high_water is not None:
            # 398: Informix offset trigger_high_water requires trigger_generation
            raise ValueError(_m(398))
    elif trigger_high_water is None:
        # 399: Informix offset trigger_generation requires trigger_high_water
        raise ValueError(_m(399))
    else:
        parsed_trigger_high_water = _strict_lsn(trigger_high_water, "trigger_high_water")
        result["trigger_high_water"] = str(parsed_trigger_high_water)
    tx_id = result.get("tx_id")
    if tx_id is not None:
        result["tx_id"] = _strict_tx_id(tx_id)
    if phase == "snapshot":
        if "snapshot_lsn" not in result or _strict_lsn(result["snapshot_lsn"], "snapshot_lsn") < 0:
            # 400: Informix snapshot offset has an invalid snapshot_lsn
            raise ValueError(_m(400))
        if any(
            value != _strict_lsn(result["snapshot_lsn"], "snapshot_lsn")
            for value in values.values()
        ):
            # 401: Informix snapshot offset requires snapshot_lsn, begin_lsn, change_lsn, and
            # commit_lsn to be equal
            raise ValueError(_m(401))
        snapshot = result.get("snapshot")
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("last_pk"), list):
            # 402: Informix snapshot offset is missing snapshot.last_pk
            raise ValueError(_m(402))
        page_index = snapshot.get("page_index")
        if isinstance(page_index, bool) or not isinstance(page_index, int) or page_index < 0:
            # 403: Informix snapshot offset has an invalid snapshot.page_index
            raise ValueError(_m(403))
        record_index = snapshot.get("record_index", 0)
        if isinstance(record_index, bool) or not isinstance(record_index, int) or record_index < 0:
            # 404: Informix snapshot offset has an invalid snapshot.record_index
            raise ValueError(_m(404))
    incremental = result.get("incremental")
    if isinstance(incremental, dict):
        # A copy: the values normalized below would otherwise change in the caller's
        # offset too, and an offset used as a lookup key afterwards no longer matched
        # the one Spark hands back.
        incremental = result["incremental"] = dict(incremental)
    if incremental is not None:
        if phase != "stream":
            # 405: Informix incremental snapshot state requires the stream phase
            raise ValueError(_m(405))
        if not isinstance(incremental, dict):
            # 406: Informix incremental snapshot state must be a dictionary
            raise ValueError(_m(406))
        if not isinstance(incremental.get("started"), bool):
            # 407: Informix incremental snapshot state must record a boolean 'started'
            raise ValueError(_m(407))
        chunk_lsn = incremental.get("chunk_lsn")
        if chunk_lsn is not None:
            result["incremental"]["chunk_lsn"] = str(_strict_lsn(chunk_lsn, "chunk_lsn"))
        for key in ("last_pk", "max_pk"):
            value = incremental.get(key)
            if value is not None and not isinstance(value, list):
                # 408: Informix incremental snapshot {} must be a list or null
                raise ValueError(_m(408, key))
        if not incremental["started"] and (
            incremental.get("last_pk") is not None or incremental.get("max_pk") is not None
        ):
            # 409: Informix incremental snapshot state cannot carry bounds before it starts
            raise ValueError(_m(409))
        # Provenance stamped by _seed_incremental_block. Absent on offsets written
        # before it existed, which is exactly the "cannot be validated" case the
        # resume path re-seeds rather than trusts.
        boundary_lsn = incremental.get("boundary_lsn")
        if boundary_lsn is not None:
            result["incremental"]["boundary_lsn"] = str(
                _strict_lsn(boundary_lsn, "incremental boundary_lsn")
            )
        stamped_scope = incremental.get("scope")
        if stamped_scope is not None and (
            not isinstance(stamped_scope, str) or _PIPELINE_SCOPE.fullmatch(stamped_scope) is None
        ):
            # 410: Informix incremental snapshot scope has an invalid identity
            raise ValueError(_m(410))
        chunk_exprs = incremental.get("chunk_exprs")
        if chunk_exprs is not None and (
            not isinstance(chunk_exprs, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in chunk_exprs.items()
            )
        ):
            # 411: Informix incremental snapshot chunk_exprs must map strings to strings
            raise ValueError(_m(411))
        snapshot_filter = incremental.get("snapshot_filter")
        if snapshot_filter is not None:
            if not isinstance(snapshot_filter, str):
                # 412: Informix incremental snapshot filter must be a string or null
                raise ValueError(_m(412))
            try:
                result["incremental"]["snapshot_filter"] = _snapshot_filter(
                    {"snapshot.filter": snapshot_filter}
                )
            except ValueError as error:
                # A copy begun before snapshot.filter took row_filter's grammar.
                # 75: Informix incremental snapshot was copying under a snapshot.filter that
                # row_filter's grammar does not hold ({}); run a full refresh
                raise ValueError(_m(75, error)) from error
    return result


def _unl_position(offset: dict[str, Any], *, terminal: int | None = None) -> tuple[int, int]:
    # Docstring: ``(split index, record index)`` a file-snapshot offset points at.
    #
    # An empty offset is the start of the extract. A stream-phase offset is past its end:
    # ``terminal`` (the split count) when given -- the end of a window that finished the
    # extract -- and an error otherwise.

    if not offset:
        return 0, 0
    checkpoint = _validated_offset(offset)
    if checkpoint.get("phase") != "snapshot":
        if terminal is None:
            # 386: A file snapshot window cannot start at a stream-phase offset
            raise InformixError(_m(386))
        return terminal, 0
    snapshot = checkpoint["snapshot"]
    return int(snapshot["page_index"]), int(snapshot.get("record_index", 0))


def _handoff_position(offset: dict[str, Any]) -> str:
    # Docstring: A parked offset's stream position as messages give it: its commit LSN, and
    # its change LSN where that is another.

    commit, change = str(offset.get("commit_lsn")), str(offset.get("change_lsn"))
    return commit if change == commit else f"{commit} (change {change})"


def _with_backlog_streak(end: dict, start: dict, row_budget_reached: bool) -> dict:
    # Docstring: Record whether this read ended at its row budget rather than the log's end.
    #
    # A read that filled its budget has *proved* more changes are waiting for this
    # table, which is the only first-hand backlog evidence a reader has: the
    # published hint is the server's global log position, so it measures staleness
    # and cannot tell a quiet table from a lagging one. Counting consecutive
    # truncations distinguishes a sustained backlog from one busy read, and draining
    # the log clears the streak outright so a caught-up flow never keeps priority it
    # no longer needs.
    #
    # The count saturates so it cannot grow without bound in an offset that a
    # permanently backlogged flow persists on every batch.

    if not row_budget_reached:
        if "backlog_streak" not in end:
            return end
        end = dict(end)
        end.pop("backlog_streak", None)
        return end
    end = dict(end)
    end["backlog_streak"] = min(_backlog_streak(start) + 1, _CONNECTION_BACKLOG_TRUNCATION_LEVELS)
    return end


def _backlog_rank(backlog: int, streak: int = 0) -> int:
    # Docstring: Bucket an estimated backlog onto a small, bounded, log-scaled rank.
    #
    # Two signals combine, because each is blind where the other sees.
    #
    # ``backlog`` is derived from a published *global* log position, so its
    # magnitude reflects whole-server write volume rather than this table's row
    # count; a log scale absorbs that bias so only order-of-magnitude differences
    # move the rank. It is comparable *across* readers, which is what ordering a
    # contended queue needs, but it over-states a quiet table's lag.
    #
    # ``streak`` counts consecutive reads that ended at their row budget. That is
    # first-hand proof this table has work outstanding, so it discriminates a
    # genuinely backlogged flow from a merely stale one -- the discrimination the
    # global signal cannot supply (measured in production: every waiter ranked
    # identically because all checkpoints were recent). It is only a self
    # assessment, so it cannot order one reader against another on its own.
    #
    # The streak takes the *high* band of the rank space and the estimate the low
    # one, rather than the two being combined by a maximum. A maximum cannot
    # separate readers whose estimates have already saturated, which is exactly the
    # observed failure: when every waiter's hint-derived estimate lands on the same
    # high rank, a proven backlog has to be able to outrank it. Banding guarantees
    # that any truncating flow outranks every non-truncating one, while the estimate
    # still orders readers within each band.

    proven_floor = _CONNECTION_BACKLOG_RANK_LEVELS // 2
    estimated = 0 if backlog <= 0 else min(proven_floor - 1, backlog.bit_length() // 4)
    if streak <= 0:
        return estimated
    # Scale the streak across the upper band so a saturated streak reaches the top
    # rank, matching what "provably behind on every read" deserves.
    span = _CONNECTION_BACKLOG_RANK_LEVELS - 1 - proven_floor
    return proven_floor + min(span, streak * span // max(1, _CONNECTION_BACKLOG_TRUNCATION_LEVELS))
