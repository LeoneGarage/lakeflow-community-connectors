"""Schema changes, channel starts, delete floors and immutable records in the connector's state."""

from __future__ import annotations

import hashlib
import logging
import re
import secrets
import time
from dataclasses import replace
from typing import Any

from databricks.labs.community_connector.sources.informix.informix_filters import (
    _ensure_selected_keys,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _capture_descriptor,
    _cdc_capable,
    _database_encoding,
    _ensure_materializable,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _PRIMARY_KEYS_OPTION,
    _PRIMARY_KEYS_WILDCARD,
    _key_override,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _ensure_compatible_schema_change,
    _validated_offset,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _IDENTIFIER,
    _IMMUTABLE_STATE_VERSION,
    _KEPT_FLOOR_SCOPES,
    _PIPELINE_SCOPE,
    _SHARED_STATE_WAIT_SECONDS,
    InformixError,
    _m,
    _split_identity,
    _sql_identifier,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _MAX_SCHEMA_DESCENT,
    LogRetentionError,
    SchemaTransitionUnavailable,
    Table,
    _open_state_connection,
    _sleep_with_backoff,
)
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _carry_table_options,
    _ensure_excepted_columns_exist,
    _schema_fingerprint,
    _wildcard_key,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _refuse_float_key_mid_copy,
    _schema_state,
    _table_from_schema_state,
)
from databricks.labs.community_connector.sources.informix.lakebase_state import (
    publish_maximum_lsn_state_record,
    publish_minimum_lsn_state_record,
    publish_own_start_state_record,
    read_state_record,
)
from databricks.labs.community_connector.sources.informix.lakebase_state_store import LakebaseState


class _SchemaMixin:
    # Docstring: Schema changes, channel starts, delete floors and immutable records in the
    # connector's state.

    def _record_current_schema(
        self,
        table: Table,
        checkpoint_lsn: int,
        checkpoint_schema_id: str,
        checkpoint_fingerprint: str,
        pipeline_scope: str,
        *,
        owner: bool,
    ) -> None:
        if not owner:
            return
        existing = self._read_immutable_head(
            self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id)
        )
        if existing is not None:
            self._validate_immutable_record_header(existing, "schema-node", table.exposed_name)
            schema = existing.get("schema")
            predecessor = schema.get("predecessor") if isinstance(schema, dict) else None
            if (
                # pylint: disable-next=too-many-boolean-expressions
                not isinstance(schema, dict)
                or schema.get("id") != checkpoint_schema_id
                or schema.get("fingerprint") != checkpoint_fingerprint
                or checkpoint_fingerprint != _schema_fingerprint(table)
                or (
                    predecessor is not None
                    and (
                        not isinstance(predecessor, str)
                        or not re.fullmatch(r"[0-9a-f]{32}", predecessor)
                    )
                )
            ):
                # 287: Checkpoint schema {} conflicts with immutable history for '{}'
                raise InformixError(_m(287, checkpoint_schema_id, table.exposed_name))
            if (
                _table_from_schema_state(schema, table.database).native_identity
                != table.native_identity
            ):
                # 288: Invalid immutable schema-node state for '{}'
                raise InformixError(_m(288, table.exposed_name))
            return
        minimum, current = self._bridge.minimum_lsn(), self._bridge.current_lsn()
        if not minimum <= checkpoint_lsn <= current:
            # 289: Cannot rebuild immutable schema state for '{}' from checkpoint LSN {};
            # retained/current range is [{}, {}]
            raise InformixError(_m(289, table.exposed_name, checkpoint_lsn, minimum, current))
        self._bridge.validate_initial_lsn(
            _capture_descriptor(table, _database_encoding(self.options)), checkpoint_lsn
        )
        authoritative = self._find_immutable_schema_record(
            table, checkpoint_schema_id, pipeline_scope
        )
        if authoritative is None:
            if checkpoint_fingerprint != _schema_fingerprint(table):
                # 290: Schema history for checkpoint node {} is missing for '{}' and cannot be
                # reconstructed after a schema change; run a full refresh
                raise InformixError(_m(290, checkpoint_schema_id, table.exposed_name))
            authoritative = {
                "created_at": time.time(),
                "schema": _schema_state(table, checkpoint_lsn, schema_id=checkpoint_schema_id),
            }
        # Schema nodes are global for a physical table and schema ID. Pipeline
        # scope belongs to initialization/trigger records, not this shared node.
        authoritative = {
            "created_at": authoritative.get("created_at", time.time()),
            "schema": authoritative["schema"],
        }
        winner = self._publish_immutable_head(
            self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id),
            authoritative,
            record_type="schema-node",
        )
        self._validate_immutable_record_header(winner, "schema-node", table.exposed_name)
        schema = winner.get("schema")
        if (
            not isinstance(schema, dict)
            or schema.get("id") != checkpoint_schema_id
            or _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
            or schema.get("fingerprint") != _schema_fingerprint(table)
        ):
            # 287: Checkpoint schema {} conflicts with immutable history for '{}'
            raise InformixError(_m(287, checkpoint_schema_id, table.exposed_name))

    def _shared_trigger_boundary(
        self,
        table: Table,
        checkpoint: dict[str, Any],
        *,
        current_lsn: int | None = None,
    ) -> tuple[int, str]:
        # One pipeline update owns exactly one immutable per-table boundary.
        # Upsert and delete checkpoints may legitimately have different prior
        # generations after retries or cancellation, so predecessor identity
        # must not partition their current-update coordination.
        #
        # Whichever of the table's two readers comes first sets it. Set by the upsert
        # reader alone, a delete flow started ahead of its upsert flow yielded until that
        # one had read, in one of the few flow slots a triggered update has: with enough
        # tables the waiting delete flows could take every slot from the upsert flows they
        # waited for. A reader whose position is already past the boundary -- an upsert
        # reader whose snapshot was taken after the delete reader set it -- has nothing
        # to read up to it: its own position is its bound for this trigger.
        scope = self._pipeline_scope(checkpoint)
        position = int(checkpoint.get("commit_lsn", 0))
        cached = self._trigger_boundaries.get(table.identity)
        if cached is not None:
            high_water, generation, cached_scope = cached
            if cached_scope == scope:
                # Once Spark checkpoints this trigger's generation it invokes
                # latestOffset again to prove that AvailableNow is exhausted.
                # Keep returning the same frozen boundary in this reader
                # instance; a new pipeline update constructs a new reader and
                # legitimately advances from generation as its predecessor.
                return max(high_water, position), generation
            self._trigger_boundaries.pop(table.identity, None)
        namespace = self._immutable_namespace(table, "triggers", "scopes", scope)
        deadline = time.monotonic() + int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        delay = 0.1
        while True:
            trigger = self._read_immutable_head(namespace)
            if not isinstance(trigger, dict):
                candidate_high_water = (
                    current_lsn if current_lsn is not None else self._bridge.current_lsn()
                )
                if candidate_high_water < int(checkpoint["commit_lsn"]):
                    # 295: Current LSN {} precedes checkpoint LSN {} for '{}'
                    raise InformixError(
                        _m(295, candidate_high_water, checkpoint["commit_lsn"], table.exposed_name)
                    )
                # The publish returns the election's winner, this reader's or another's.
                # Read back after a pause instead, it cost every table's first triggered read
                # a sleep and a round trip.
                trigger = self._publish_immutable_head(
                    namespace,
                    {
                        "created_at": time.time(),
                        "generation": secrets.token_hex(16),
                        "high_water": str(candidate_high_water),
                        "scope": scope,
                    },
                    record_type="trigger",
                )
            if isinstance(trigger, dict):
                self._validate_immutable_record_header(trigger, "trigger", table.exposed_name)
                generation = trigger.get("generation")
                try:
                    high_water = self._immutable_lsn(trigger, "high_water", table.exposed_name)
                except InformixError as error:
                    # 292: Invalid shared trigger boundary for '{}'
                    raise InformixError(_m(292, table.exposed_name)) from error
                if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{32}", generation):
                    # 293: Invalid shared trigger generation for '{}'
                    raise InformixError(_m(293, table.exposed_name))
                if trigger.get("scope") != scope:
                    # 294: Invalid immutable trigger identity for '{}'
                    raise InformixError(_m(294, table.exposed_name))
                self._trigger_boundaries[table.identity] = (
                    high_water,
                    generation,
                    scope,
                )
                return max(high_water, position), generation
            if time.monotonic() >= deadline:
                # 296: Timed out publishing the triggered boundary for '{}' in update scope '{}'
                raise InformixError(_m(296, table.exposed_name, scope))
            delay = _sleep_with_backoff(deadline, delay)

    def _ensure_recorded_path_to_live(
        self,
        table: Table,
        node_table: Table,
        node_id: str,
        rules: dict[str, Any],
        *,
        copying: bool = False,
    ) -> None:
        # Docstring: Check the steps a reader takes from a step's target on to the live layout.
        #
        # A reader that steps to a generation the live schema has moved past takes the rest
        # one recorded step at a time, so they are checked that way here: a whole-row key
        # may make only a drop or an add in one step, and the target and the live layout can
        # differ by both. Where the recorded history ends short of the live layout, the
        # owner records the rest as one step, so the last node is checked against it, with
        # the default check that step gets when a column joins the key. A record that is
        # not a valid successor fails as the step would, and so does history that loops
        # (layout-only ids recorded before transition ids hashed their predecessor); one
        # running past the bound is checked against the live layout from where it stopped.

        live = _schema_fingerprint(table)
        seen: set[str] = set()
        while _schema_fingerprint(node_table) != live:
            if node_id in seen:
                # No node of a loop is left without a recorded successor, so a reader steps
                # around it forever and never reaches the live layout.
                # 425: Informix schema history for '{}' returns to node {} without reaching the
                # live layout; run a full refresh
                raise InformixError(_m(425, table.exposed_name, node_id))
            if len(seen) >= _MAX_SCHEMA_DESCENT:
                break
            seen.add(node_id)
            record = self._read_immutable_head(self._immutable_namespace(table, "schemas", node_id))
            successor = None
            if record is not None:
                self._validate_immutable_record_header(
                    record, "schema-transition", table.exposed_name
                )
                successor = record.get("schema")
                if not isinstance(successor, dict):
                    # 300: Invalid immutable Informix schema transition
                    raise InformixError(_m(300))
                if (
                    not re.fullmatch(r"[0-9a-f]{32}", str(successor.get("id")))
                    or successor.get("predecessor") != node_id
                ):
                    # 302: Invalid immutable schema identity for '{}'
                    raise InformixError(_m(302, table.exposed_name))
            following = (
                None if successor is None else _table_from_schema_state(successor, table.database)
            )
            # rules: the wildcard-key growth and shrink the caller allows.
            _ensure_compatible_schema_change(
                node_table, following or table, wildcard_except=table.key_wildcard_except, **rules
            )
            if copying:
                # Each step: one to a float key taken later failed only on the next batch.
                _refuse_float_key_mid_copy(node_table, following or table)
            if following is None:
                if set(table.primary_keys) - set(node_table.primary_keys):
                    self._ensure_added_key_columns_have_no_default(node_table, table)
                return
            node_table, node_id = following, str(successor["id"])
        else:
            return
        _ensure_compatible_schema_change(
            node_table, table, wildcard_except=table.key_wildcard_except, **rules
        )
        if copying:
            _refuse_float_key_mid_copy(node_table, table)
        if set(table.primary_keys) - set(node_table.primary_keys):
            self._ensure_added_key_columns_have_no_default(node_table, table)

    # pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
    def _schema_transition(
        self,
        table: Table,
        checkpoint_schema_id: str,
        checkpoint_lsn: int,
        *,
        owner: bool,
        allow_key_extension: bool = False,
        new_update: bool = False,
        copying: bool = False,
    ) -> tuple[Table, Table, int, str]:
        # copying: an incremental copy is still in progress (see _refuse_float_key_mid_copy).
        # A wildcard key may grow by the appended columns, and on the first read of a new
        # update shrink by the dropped ones (see _PRIMARY_KEYS_WILDCARD).
        extend_key = allow_key_extension and table.key_wildcard
        shrink_key = extend_key and new_update
        namespace = self._immutable_namespace(table, "schemas", checkpoint_schema_id)
        deadline = time.monotonic() + int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        delay = 0.1
        while True:
            previous_record = self._read_immutable_head(
                self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id)
            )
            previous = previous_record.get("schema") if previous_record else None
            if not isinstance(previous, dict):
                # 298: Schema history for checkpoint node {} is missing for '{}'; run a full refresh
                raise InformixError(_m(298, checkpoint_schema_id, table.exposed_name))
            self._validate_immutable_record_header(
                previous_record, "schema-node", table.exposed_name
            )
            if previous.get("id") != checkpoint_schema_id:
                # 299: Immutable schema node identity mismatch for '{}'
                raise InformixError(_m(299, table.exposed_name))
            previous_table = _table_from_schema_state(previous, table.database)
            transition_record = self._read_immutable_head(namespace)
            if transition_record is None:
                # No step recorded yet: the owner records one straight to the live schema.
                # A recorded step is checked below one step at a time instead, so a reader
                # a generation behind is not refused a drop and an add the pipeline ran
                # between, as each step may make only one of them.
                try:
                    _ensure_compatible_schema_change(
                        previous_table,
                        table,
                        extend_wildcard_key=extend_key,
                        shrink_wildcard_key=shrink_key,
                        wildcard_except=table.key_wildcard_except,
                    )
                except InformixError as error:
                    if owner:
                        # The delete reader can pass this check (no incremental copy in its
                        # offset, or its own first read of the update) and would wait for a
                        # step that never comes: it fails with this error instead.
                        self._publish_schema_rejection(table, checkpoint_schema_id, error)
                    raise
            if transition_record is None and owner:
                try:
                    if copying:
                        _refuse_float_key_mid_copy(previous_table, table)
                    if set(table.primary_keys) - set(previous_table.primary_keys):
                        # The key grew: only safe when existing rows really hold NULL in
                        # the new columns at the source, as they will in the target. A
                        # key that shrank needs no check: the dropped columns leave it.
                        self._ensure_added_key_columns_have_no_default(previous_table, table)
                    transition = self._bridge.current_lsn()
                    if transition < checkpoint_lsn:
                        # 295: Current LSN {} precedes checkpoint LSN {} for '{}'
                        raise InformixError(_m(295, transition, checkpoint_lsn, table.exposed_name))
                    self._bridge.validate_initial_lsn(
                        _capture_descriptor(table, _database_encoding(self.options)),
                        transition,
                    )
                except InformixError as error:
                    # The delete reader waits for this transition; record why it won't
                    # come, so that reader fails with this error instead of waiting.
                    # Transient failures (socket timeouts) aren't InformixError and
                    # leave nothing behind.
                    self._publish_schema_rejection(table, checkpoint_schema_id, error)
                    raise
                node = _schema_state(table, transition, predecessor=checkpoint_schema_id)
                transition_record = self._publish_immutable_head(
                    namespace,
                    {"created_at": time.time(), "schema": node},
                    record_type="schema-transition",
                )
            if transition_record is not None:
                self._schema_transition_waits.pop((table.identity, checkpoint_schema_id), None)
                self._validate_immutable_record_header(
                    transition_record, "schema-transition", table.exposed_name
                )
                current = transition_record.get("schema")
                if not isinstance(current, dict):
                    # 300: Invalid immutable Informix schema transition
                    raise InformixError(_m(300))
                transition = self._immutable_lsn(current, "start_lsn", table.exposed_name)
                minimum, now = self._bridge.minimum_lsn(), self._bridge.current_lsn()
                if not minimum <= transition <= now:
                    # 301: Schema transition LSN {} for '{}' is outside retained/current range [{},
                    # {}]
                    raise InformixError(_m(301, transition, table.exposed_name, minimum, now))
                # The transition LSN records when this immutable successor was
                # first observed; it is not a replay boundary.  Another update
                # can therefore retain the transition after this checkpoint has
                # advanced beyond that observation.  The identity and additive
                # successor checks below make it safe to adopt the metadata while
                # continuing replay from checkpoint_lsn.
                target_table = _table_from_schema_state(current, table.database)
                if (
                    not isinstance(current.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", str(current["id"]))
                    or current.get("predecessor") != checkpoint_schema_id
                ):
                    # 302: Invalid immutable schema identity for '{}'
                    raise InformixError(_m(302, table.exposed_name))
                try:
                    _ensure_compatible_schema_change(
                        previous_table,
                        target_table,
                        extend_wildcard_key=extend_key,
                        shrink_wildcard_key=shrink_key,
                        wildcard_except=table.key_wildcard_except,
                    )
                    if copying:
                        # A step recorded before the copy's reader checked it, or by another.
                        _refuse_float_key_mid_copy(previous_table, target_table)
                    self._ensure_recorded_path_to_live(
                        table,
                        target_table,
                        str(current["id"]),
                        {"extend_wildcard_key": extend_key, "shrink_wildcard_key": shrink_key},
                        copying=copying,
                    )
                except InformixError as error:
                    if owner:
                        # As before a step is recorded: the delete reader, whose own checks
                        # this one can pass, takes the step and waits for the next one, then
                        # failed only at its deadline. Under both steps it may be waiting at.
                        for waited_at in (checkpoint_schema_id, str(current["id"])):
                            self._publish_schema_rejection(table, waited_at, error)
                    raise
                schema_winner = self._publish_immutable_head(
                    self._immutable_namespace(table, "schema-nodes", str(current["id"])),
                    transition_record,
                    record_type="schema-node",
                )
                self._validate_schema_node_winner(schema_winner, current, table)
                return previous_table, target_table, transition, str(current["id"])
            if not owner:
                # Never wait here while holding a connection slot. Reaching this
                # method already acquired one (the schema refresh in _read_stream
                # connects before this call), so a delete reader that slept until
                # the deadline would occupy a slot for the whole wait while doing
                # no source work -- and the upsert reader it is waiting for needs
                # a slot of its own to publish (current_lsn above). Because both
                # channels can claim every slot, enough waiting delete readers hold
                # the entire pool and the publication can never happen: a genuine deadlock,
                # broken only by this deadline expiring and failing the flow.
                #
                # Yield instead, as the other non-owner coordination point does
                # (_shared_table_lsn returns None via wait=False). The
                # caller converts this into an unchanged-offset empty batch, which
                # releases the slot and lets the owning upsert reader acquire one.
                #
                # Fail with the upsert reader's own error where it refused the change
                # before publishing, or this reader would wait for a transition that
                # never comes. A published transition wins over a refusal, read first
                # above: an upsert retry in the same update may have succeeded since.
                refusal = self._schema_rejection(table, checkpoint_schema_id)
                if refusal is not None:
                    # 303: The upsert reader refused the schema change for '{}': {}
                    raise InformixError(_m(303, table.exposed_name, refusal))
                self._ensure_schema_transition_wait_bounded(table, checkpoint_schema_id)
                # 304: The upsert reader has not published the schema transition for '{}' from
                # schema node {} yet
                raise SchemaTransitionUnavailable(_m(304, table.exposed_name, checkpoint_schema_id))
            if time.monotonic() >= deadline:
                # 305: Timed out waiting for the upsert reader to publish schema transition state
                # for '{}'
                raise InformixError(_m(305, table.exposed_name))
            delay = _sleep_with_backoff(deadline, delay)

    def _schema_rejection_namespace(self, table: Table, checkpoint_schema_id: str) -> str | None:
        # Docstring: Where an update's upsert reader records refusing a schema change.
        #
        # Scoped to the update: the next one may succeed where this one failed (the cause
        # fixed, or a refusal that no longer holds), so its delete reader must not inherit
        # the refusal. Without a registration scope the two readers share no update, and
        # the delete reader falls back to its bounded wait.

        scope = self._registration_scope
        if not isinstance(scope, str) or _PIPELINE_SCOPE.fullmatch(scope) is None:
            return None
        return self._immutable_namespace(
            table, "schema-rejections", "scopes", scope, checkpoint_schema_id
        )

    def _publish_schema_rejection(
        self, table: Table, checkpoint_schema_id: str, error: Exception
    ) -> None:
        # Docstring: Record the upsert reader's refusal of a schema change; best effort.
        #
        # The refusal itself fails the upsert flow either way; if it can't be recorded,
        # the delete reader still stops at its bounded wait.

        namespace = self._schema_rejection_namespace(table, checkpoint_schema_id)
        if namespace is None:
            return
        try:
            self._publish_immutable_head(
                namespace,
                # The scope as well as the key: the update's cleanup and the stale-pipeline
                # sweep find a record by it, and kept this one forever.
                {"created_at": time.time(), "error": str(error), "scope": self._registration_scope},
                record_type="schema-rejection",
            )
        except Exception as publish_error:  # pylint: disable=broad-except
            # 306: Could not record that the upsert reader refused the schema change for {}: {}
            logging.getLogger(__name__).warning(
                _m(306, table.exposed_name, publish_error),
            )

    def _schema_rejection(self, table: Table, checkpoint_schema_id: str) -> str | None:
        # Docstring: This update's recorded refusal of the schema change, if any.

        namespace = self._schema_rejection_namespace(table, checkpoint_schema_id)
        if namespace is None:
            return None
        record = self._read_immutable_head(namespace)
        if record is None:
            return None
        self._validate_immutable_record_header(record, "schema-rejection", table.exposed_name)
        return str(record.get("error") or "no reason was recorded")

    def _triggered_wait_exceeded(self, key: tuple) -> int | None:
        # Docstring: The wait limit once a triggered reader has waited past it on ``key``.
        #
        # Measured from the first yield of the wait, at cdc.shared.state.wait.seconds; None
        # for a continuous update, which fails as a whole once the flow it waits on fails.

        if not self._trigger_available_now:
            return None
        started = self._schema_transition_waits.setdefault(key, time.monotonic())
        limit = int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        return limit if time.monotonic() - started >= limit else None

    def _resnapshot_retry_offset(self, table: Table, start_offset: dict) -> dict:
        # Docstring: Yield while the upsert reader publishes the update's resnapshot boundary.
        #
        # Bounded in a triggered update, which ends only when every flow has: the upsert
        # reader's flow may have failed, or its own checkpoint is still retained while this
        # reader's is not, and then no boundary comes.

        limit = self._triggered_wait_exceeded((table.identity, "resnapshot"))
        if limit is not None:
            # 427: The upsert reader has not published a resnapshot boundary for '{}' in {}
            # seconds, so this update cannot finish: its flow has most likely failed (see its
            # error), or its checkpoint is still retained while the delete channel's is not, which
            # needs a full refresh
            raise InformixError(_m(427, table.exposed_name, limit))
        return self._trigger_boundary_retry_offset(start_offset)

    def _ensure_schema_transition_wait_bounded(
        self, table: Table, checkpoint_schema_id: str
    ) -> None:
        # Docstring: Fail a triggered delete reader that has waited too long for its upsert reader.
        #
        # A triggered update ends only when every flow has. If the upsert reader fails
        # before publishing the transition, its flow stops but this reader keeps yielding
        # empty batches, so the update never ends. A continuous update fails as a whole
        # once a flow fails, so only AvailableNow is bounded, at
        # cdc.shared.state.wait.seconds from the first yield. A healthy upsert reader
        # publishes on its first read after the change.

        limit = self._triggered_wait_exceeded((table.identity, checkpoint_schema_id))
        if limit is not None:
            # 307: The upsert reader has not published the schema transition for '{}' in {} seconds,
            # so this update cannot finish. Its flow has most likely failed: see its error, fix the
            # cause, and restart the pipeline
            raise InformixError(_m(307, table.exposed_name, limit))

    def _ensure_added_key_columns_have_no_default(self, previous: Table, current: Table) -> None:
        # Docstring: Fail closed when a column joining a wildcard key was added with a default.
        #
        # Existing rows would carry the default in their before-images at the source while
        # the target holds NULL for them, so their updates and deletes would never match.

        known = {column.name for column in previous.columns}
        joining = set(current.primary_keys)
        # Only the added columns that join the key: a "* except(...)" wildcard may leave one
        # out, and it then needs no NULL in existing rows to match by.
        added = [
            column.name
            for column in current.columns
            if column.name not in known and column.name in joining
        ]
        lookup = getattr(self._bridge, "column_defaults", None)
        if not callable(lookup):
            # 308: Cannot check the columns added to '{}' for defaults, so its {}='{}' key cannot
            # grow to include them; run a full refresh
            raise InformixError(
                _m(308, current.exposed_name, _PRIMARY_KEYS_OPTION, _PRIMARY_KEYS_WILDCARD)
            )
        with_default = lookup(current.identity, added)  # pylint: disable=not-callable
        if with_default:
            # 309: Column(s) {} were added to '{}' with a default and join its {}='{}' key. Existing
            # rows carry that default at the source but NULL in the target, so their updates and
            # deletes would not match; run a full refresh
            raise InformixError(
                _m(
                    309,
                    (", ".join(with_default)),
                    current.exposed_name,
                    _PRIMARY_KEYS_OPTION,
                    _PRIMARY_KEYS_WILDCARD,
                )
            )

    def _refresh_table_schema(self, table: Table, expected_fingerprint: str | None) -> Table:
        bare = Table.parse(self._bridge.get_table(table.identity), table.database)
        refreshed = bare
        # Keep the table options (and therefore the fingerprint) across a schema refresh
        # instead of reverting to the bare catalog table. A primary.keys override is only
        # carried when the caller was already overridden, so a genuine catalog key change
        # on a normal table is still detected as drift; its adopted key-index directions
        # come along, so a mixed-direction override keeps paging in its index order. A
        # wildcard key is every column, so it is re-expanded instead: an added column
        # joins the key, which the schema transition accepts for a streaming table.
        try:
            refreshed = _carry_table_options(refreshed, table)
        except (InformixError, ValueError) as error:
            # Options that were taken: the table changed under them (an INTEGER widened to a
            # FLOAT no ABS reads), which their own error, about an option no one changed,
            # didn't say.
            # 487: The schema of Informix table '{}' changed so that its table options no
            # longer apply ({}); change them for the new schema and run a full refresh
            raise InformixError(_m(487, table.exposed_name, error)) from error
        _ensure_materializable(refreshed)
        fingerprint = _schema_fingerprint(refreshed)
        if expected_fingerprint is not None and expected_fingerprint != fingerprint:
            # 243: Informix schema changed for '{}' during ingestion; run a full refresh before
            # reading additional snapshot or CDC records
            raise InformixError(_m(243, table.exposed_name))
        if self._tables is not None:
            # The cache holds catalog tables as read; _table applies the options per call.
            self._tables[table.exposed_name] = bare
        return refreshed

    def _table_int_option(
        self,
        table_options: dict[str, str],
        name: str,
        default: int,
        *,
        minimum: int,
        maximum: int | None = None,
    ) -> int:
        # An empty value is a setting not made, in a table's options as in the
        # connection's; one of blanks is judged as given, as the constructor judges it.
        raw = table_options.get(name)
        if raw is None or raw == "":
            raw = self.options.get(name)
        if raw is None or raw == "":
            raw = str(default)
        try:
            value = int(raw)
        except (TypeError, ValueError) as error:
            raise ValueError(f"Option '{name}' must be a whole number") from error
        if value < minimum:
            raise ValueError(f"Option '{name}' must be >= {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"Option '{name}' must be <= {maximum}")
        return value

    def _initial_lsn(
        self,
        table: Table,
        *,
        owner: bool = True,
        wait: bool = True,
        scope: str | None = None,
    ) -> int | None:
        # Docstring: Return one durable per-table boundary shared by upsert and delete readers.

        scope = scope or self._pipeline_scope()
        cache_key = (scope, table.identity)
        if cache_key not in self._snapshot_high_water:
            shared = self._shared_table_lsn(table, owner=owner, wait=wait, scope=scope)
            if shared is None:
                return None
            value, schema_id = shared
            minimum = self._bridge.minimum_lsn()
            if value < minimum:
                # 310: Configured initial LSN {} is older than minimum retained LSN {}; choose a
                # retained boundary after enabling full-row logging
                raise LogRetentionError(_m(310, value, minimum))
            current = self._bridge.current_lsn()
            if value > current:
                # 311: Configured initial LSN {} is newer than current Informix LSN {}; choose a
                # position captured after enabling full-row logging
                raise InformixError(_m(311, value, current))
            self._snapshot_high_water[cache_key] = value
            self._snapshot_schema_ids[cache_key] = schema_id
        return self._snapshot_high_water[cache_key]

    def _publish_upsert_channel_start(
        self,
        table: Table,
        start_lsn: int,
        schema_id: str,
        fingerprint: str,
        scope: str,
    ) -> None:
        # Docstring: Publish the upsert reader's effective start for an unseeded delete flow.
        #
        # Lakeflow checkpoints the upsert and delete flows independently.  The
        # incoming upsert offset is the only authoritative evidence of where a
        # resumed update starts; source ``current_lsn`` and global schema history
        # can both be later. A later poll can discover an older open transaction,
        # so publishers atomically lower the shared boundary and never raise it.

        self._publish_channel_start(
            (table.native_identity, *self._table_state_keys(table)),
            table.exposed_name,
            scope,
            (start_lsn, schema_id, fingerprint),
            table,
        )

    def _publish_channel_start(self, identity, exposed, scope, start, table=None) -> None:
        # Docstring: Publish an upsert start ``(lsn, schema_id, fingerprint)`` for ``identity``
        # ``(native, connection key, table key)``, and lower the own-start record to it.
        #
        # Once a registration a start: every microbatch published it twice, the one before
        # the slot and the one after, each a Lakebase transaction that changed nothing. A
        # schema stepped at the same position is checked as a successor where ``table``
        # is given (the reader's own); the pre-capacity publish takes the checkpoint's.

        native, connection_key, table_key = identity
        start_lsn, schema_id, fingerprint = start
        base = f"{connection_key}/{table_key}"
        record_key = f"{base}/channel-starts/{scope}/upsert"
        noted = self._noted_upsert_starts.get(record_key)
        if noted is not None and (noted[0] < start_lsn or noted == (start_lsn, schema_id)):
            return
        winner = self._state(
            publish_minimum_lsn_state_record,
            connection_key,
            record_key,
            {
                "created_at": time.time(),
                "fingerprint": fingerprint,
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": "channel-start",
                "schema_id": schema_id,
                "scope": scope,
                "start_lsn": str(start_lsn),
                "table": native,
            },
            record_type="channel-start",
            operation="upsert channel-start publish",
        )
        self._validate_immutable_record_header(winner, "channel-start", exposed)
        winner_start = self._immutable_lsn(winner, "start_lsn", exposed)
        moved = winner_start == start_lsn and (
            winner.get("schema_id") != schema_id or winner.get("fingerprint") != fingerprint
        )
        stepped_off = (
            moved
            and table is not None
            and not self._schema_descends_from(
                table, schema_id, fingerprint, winner.get("schema_id")
            )
        )
        if (
            winner.get("scope") != scope
            or winner.get("table") != native
            or winner_start > start_lsn
            or stepped_off
        ):
            # 312: Conflicting Informix upsert start boundary for '{}' in update scope '{}'
            raise InformixError(_m(312, exposed, scope))
        if table is not None or not moved:
            # Not noted while a schema at the same position is unchecked: the reader's own
            # publish, after this pre-capacity one, checks it, and noted it was skipped.
            self._noted_upsert_starts[record_key] = (start_lsn, schema_id)
        # After the start, as _full_refresh_delete_seed announces before it looks for one.
        self._note_upsert_start(f"{base}/own-start/{self._pipeline_id(scope)}", native, start)

    def _schema_descends_from(
        self,
        table: Table,
        schema_id: str,
        fingerprint: str,
        ancestor_id: Any,
    ) -> bool:
        # Docstring: Return whether a schema node safely replaces one at the same position.
        #
        # A reader can step more than once without moving (metadata-only steps while no
        # change commits), so the update's channel start keeps its first schema while the
        # reader holds a later one. Any node reached from it through recorded predecessors
        # is a step the delete flow takes the same way once it seeds there.

        if not isinstance(ancestor_id, str):
            return False
        current, expected_fingerprint, seen = schema_id, fingerprint, set()
        while current not in seen and len(seen) < _MAX_SCHEMA_DESCENT:
            seen.add(current)
            record = self._read_immutable_head(
                self._immutable_namespace(table, "schema-nodes", current)
            )
            schema = record.get("schema") if isinstance(record, dict) else None
            if not isinstance(schema, dict) or schema.get("id") != current:
                return False
            if (
                expected_fingerprint is not None
                and schema.get("fingerprint") != expected_fingerprint
            ):
                return False
            predecessor = schema.get("predecessor")
            if predecessor == ancestor_id:
                return True
            if not isinstance(predecessor, str):
                return False
            current, expected_fingerprint = predecessor, None
        return False

    def _publish_own_start(self, key: str, native_identity: str, fields: dict) -> None:
        # Docstring: Publish to a pipeline's own-start record for a table
        # (publish_own_start_state_record): a full refresh's own start, or a later start.

        self._state(
            publish_own_start_state_record,
            self._lakebase_state_namespace(),
            key,
            {
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": "own-start",
                "table": native_identity,
                **fields,
            },
            operation="own-start publish",
        )

    def _note_upsert_start(self, key: str, native_identity: str, start: tuple) -> None:
        # Docstring: Lower the own-start record under ``key`` to an upsert reader's start,
        # once a process unless the start goes lower.

        lsn, schema_id, fingerprint = start
        if lsn < self._noted_upsert_starts.get(key, (lsn + 1,))[0]:
            self._publish_own_start(
                key,
                native_identity,
                {"start_lsn": str(lsn), "schema_id": schema_id, "fingerprint": fingerprint},
            )
            self._noted_upsert_starts[key] = (lsn,)

    @staticmethod
    def _floor_scopes(record: dict | None) -> tuple:
        # Docstring: The snapshots a floor record stands for: its own, and for an auto_snapshot
        # resnapshot's, each before it back to the last whose snapshot emptied the target,
        # whose rows the destination kept (one in a record of 36acdad9-053c47a7).

        own = None if record is None else record.get("snapshot_scope")
        kept = record.get("kept_scope") if own and record.get("resnapshot") else []
        return (own, *(kept if isinstance(kept, list) else [kept]))

    def _floor_record(self, table: Table, scope: str | None = None) -> dict | None:
        return self._read_immutable_head(
            self._immutable_namespace(
                table, "delete-floor", self._pipeline_id(scope or self._pipeline_scope())
            )
        )

    def _own_start_upsert_start(self, table: Table, seeded: str) -> tuple[int, str, str] | None:
        # Docstring: The lowest start an upsert reader published after full refresh
        # ``seeded``'s own start, as _read_upsert_channel_start gives one, or None.
        #
        # From the pipeline's own-start record, which outlives the updates' (the cleanup of
        # a later one removed the refreshing update's start, and a triggered update's delete
        # flow could end before its upsert flow published), and from the refreshing and
        # current updates' starts (a position seeded before the record).

        starts = [
            self._read_upsert_channel_start(table, scope)
            for scope in dict.fromkeys((seeded, self._pipeline_scope()))
        ]
        own = self._read_immutable_head(
            self._immutable_namespace(table, "own-start", self._pipeline_id(seeded))
        )
        if own is not None and own.get("own_start") == seeded and "start_lsn" in own:
            starts.append(self._upsert_start(table, own, "own-start", True))
        return min(filter(None, starts), default=None)

    def _read_upsert_channel_start(self, table: Table, scope: str) -> tuple[int, str, str] | None:
        record = self._read_immutable_head(
            self._immutable_namespace(table, "channel-starts", scope, "upsert")
        )
        if record is None:
            return None
        return self._upsert_start(table, record, "channel-start", record.get("scope") == scope)

    def _upsert_start(
        self, table: Table, record: dict, record_type: str, keyed: bool
    ) -> tuple[int, str, str]:
        # Docstring: An upsert reader's published start, as (lsn, schema_id, fingerprint),
        # from a record of ``record_type`` whose key fields ``keyed`` says match.

        self._validate_immutable_record_header(record, record_type, table.exposed_name)
        schema_id = record.get("schema_id")
        fingerprint = record.get("fingerprint")
        if (
            # pylint: disable-next=too-many-boolean-expressions
            not keyed
            or record.get("table") != table.native_identity
            or not isinstance(schema_id, str)
            or not re.fullmatch(r"[0-9a-f]{32}", schema_id)
            or not isinstance(fingerprint, str)
            or not re.fullmatch(r"[0-9a-f]{64}", fingerprint)
        ):
            # 313: Invalid Informix upsert start boundary for '{}'
            raise InformixError(_m(313, table.exposed_name))
        return (
            self._immutable_lsn(record, "start_lsn", table.exposed_name),
            schema_id,
            fingerprint,
        )

    @staticmethod
    def _pipeline_id(scope: str) -> str:
        # Docstring: The stable pipeline identity within an update scope.
        #
        # A scope is ``<pipeline_id>_@_<update_id>``; the pipeline id is constant across
        # a pipeline's restarts (only the update id changes) yet distinct between
        # pipelines, which is exactly the key the delete-floor needs: it must persist
        # across the restarts that push the upsert resume position forward, without one
        # pipeline's snapshot boundary ever lowering another's (concurrent full refreshes
        # of the same table land on different destinations at different snapshot LSNs).

        return scope.partition("_@_")[0]

    def _publish_delete_snapshot_floor(
        self,
        table: Table,
        schema_id: str,
        snapshot_lsn: int,
        pipeline_scope: str,
        *,
        resnapshot: bool = False,
    ) -> None:
        # Docstring: Record the snapshot boundary a delete flow should bootstrap from.
        #
        # Keyed by table and pipeline id, so it outlives update-scope GC and survives
        # restarts while staying isolated per pipeline. The schema generation the snapshot
        # was taken under rides inside the record rather than in the key, so a bootstrap
        # after a schema change can tell that its floor belongs to a superseded layout
        # (and say so) instead of simply not finding one. Published with each snapshot:
        # within one update the highest wins, and another update's replaces it whatever its
        # LSN (see _PUBLISH_MAXIMUM_LSN_RECORD), since the destination now holds that
        # snapshot, whose boundary can be lower (a log reinitialized, an older extract).
        # Ordinary polls leave it untouched. The delete channel is idempotent, so a floor
        # that lags only causes a harmless re-read; one left too high loses deletes.
        #
        # A snapshot from no checkpoint lands in a destination that is new or emptied, and
        # takes the floor whatever the update was started for: an automatic retry of a full
        # refresh, an update the pipelines API can't describe, a table selected for a
        # selective refresh. ``resnapshot`` is auto_snapshot's of an expired checkpoint
        # (_RESNAPSHOT_OPTION), which leaves the destination's rows where they are,
        # and the deletes after an earlier floor still apply to them: a floor of another
        # update that is still in the log, of this layout and not above this boundary
        # stays, a table's left out of a selective refresh too. Replaced, it sent a delete
        # reader past retained deletes of rows the new snapshot no longer has, which then
        # stayed in the destination.
        marks = {}
        if resnapshot:
            kept = self._floor_record(table, pipeline_scope)
            if (
                kept is not None
                and kept.get("snapshot_scope") != pipeline_scope
                and kept.get("table") == table.native_identity
                and kept.get("fingerprint") == _schema_fingerprint(table)
                and self._bridge.minimum_lsn()
                <= self._immutable_lsn(kept, "start_lsn", table.exposed_name)
                <= snapshot_lsn
            ):
                return
            # Replaced, the floor names the snapshot before it too, whose rows the
            # destination kept: a delete position marked with that one goes on (291 sent it
            # to this floor, past the deletes between).
            # All the replaced one stands for: a second resnapshot kept the first's rows too,
            # and a position marked with it read as one a full refresh had sent past (291).
            stood = list(dict.fromkeys(self._floor_scopes(kept)))
            marks = {"resnapshot": True, "kept_scope": stood[:_KEPT_FLOOR_SCOPES]}

        self._state(
            publish_maximum_lsn_state_record,
            self._lakebase_state_namespace(),
            self._immutable_namespace(table, "delete-floor", self._pipeline_id(pipeline_scope)),
            {
                "created_at": time.time(),
                "fingerprint": _schema_fingerprint(table),
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": "delete-floor",
                "schema_id": schema_id,
                # Not "scope", which would have the update's cleanup delete it.
                "snapshot_scope": pipeline_scope,
                "start_lsn": str(snapshot_lsn),
                "table": table.native_identity,
                **marks,
            },
            record_type="delete-floor",
            operation="delete-floor publish",
        )

    def _read_delete_snapshot_floor(
        self, table: Table, schema_id: str, fingerprint: str
    ) -> tuple[int, str, str] | None:
        # Docstring: Return the snapshot-boundary floor for a bootstrapping delete flow.
        #
        # The floor with the schema id and fingerprint it was captured under: the channel
        # start's, or one the channel start's descends from through recorded predecessors
        # (the source was altered after the snapshot while the delete flow never
        # checkpointed). The bootstrap seeds under that schema and steps from it as any
        # lagging reader does. ``None`` when no compatible floor exists, so the caller keeps
        # anchoring at the upsert channel-start: this pipeline has not published one, or the
        # floor's schema is not in the channel start's history. The latter is reported: the
        # deletes committed between the snapshot and the resume position are not replayed.

        record = self._floor_record(table)
        if record is None:
            return None
        self._validate_immutable_record_header(record, "delete-floor", table.exposed_name)
        if record.get("table") != table.native_identity:
            return None
        floor = self._immutable_lsn(record, "start_lsn", table.exposed_name)
        floor_schema_id, floor_fingerprint = record.get("schema_id"), record.get("fingerprint")
        if (floor_schema_id, floor_fingerprint) != (schema_id, fingerprint) and not (
            isinstance(floor_fingerprint, str)
            and self._schema_descends_from(table, schema_id, fingerprint, floor_schema_id)
        ):
            # 314: Informix delete snapshot floor {} for '{}' was captured under schema generation
            # {}, but the upsert channel now runs generation {}; the delete channel bootstraps at
            # the upsert resume position instead, so deletes committed between that snapshot and the
            # resume position are not replayed. Run a full refresh if the destination must match the
            # source exactly
            logging.getLogger(__name__).warning(
                _m(314, floor, table.exposed_name, floor_schema_id, schema_id),
            )
            return None
        return floor, str(floor_schema_id), floor_fingerprint

    def _upsert_channel_start_exists(self, table_name: str, table_options: dict[str, str]) -> bool:
        # Docstring: Check current-scope delete coordination without opening Informix.

        exposed = table_options.get("qualified_source_table", table_name)
        # Preserve eager configuration/schema failures. Registration commonly
        # leaves the table in this process-local cache, so validation here costs
        # no source connection. An explicitly append-only table (per table or for the
        # connection) must always reach the normal path, which raises that it has no
        # delete channel.
        if self._append_only_mode(table_options) == "true":
            return True
        cached = self._tables.get(exposed) if self._tables is not None else None
        if cached is not None:
            # The cache holds the bare catalog table, so narrow it to the columns the read
            # path ingests first, or a column left out there would fail the check here. Then
            # give it the key the read path will (primary.keys, or a promoted nullable index)
            # without the index lookup that can need: a keyless table keyed by an option has
            # a delete channel, and must wait here for its upsert reader like any keyed one.
            override = _key_override(table_options)
            cached = self._ingested_columns(cached, table_options, override)
            key = self._requested_key(cached, table_options, override)
            # A key the options name but the table does not ingest fails here as the read
            # path would, instead of waiting on an upsert reader that fails the same way.
            if key is None:
                # The primary.keys wildcard: every ingested column but those it excepts.
                _ensure_excepted_columns_exist(cached, override[1])
                key = tuple(_wildcard_key(cached.columns, override[1]))
                if not key:
                    # 192: Option '{}' excepts every column of '{}', leaving no key
                    raise InformixError(_m(192, _PRIMARY_KEYS_OPTION, cached.exposed_name))
            elif override is not None:
                ingested = {column.name for column in cached.columns}
                missing = [name for name in key if name not in ingested]
                if missing:
                    # 193: Option '{}' names column(s) not present in '{}': {}
                    raise InformixError(
                        _m(193, _PRIMARY_KEYS_OPTION, cached.exposed_name, ", ".join(missing))
                    )
            cached = replace(cached, primary_keys=key)
            _ensure_selected_keys(cached)
            _ensure_materializable(cached, self._with_connection_scan(table_options))
            if self._append_only_table(cached, table_options) or not _cdc_capable(cached):
                return True
        identity = self._native_table_state_identity(exposed)
        if identity is None:
            return True
        _, connection_key, table_key = identity
        record_key = "/".join(
            (
                connection_key,
                table_key,
                "channel-starts",
                self._pipeline_scope(),
                "upsert",
            )
        )
        return (
            self._state(
                read_state_record,
                connection_key,
                record_key,
                operation="channel-start probe",
            )
            is not None
        )

    def _native_table_state_identity(self, exposed: str) -> tuple[str, str, str] | None:
        # Docstring: Derive native and Lakebase table identities without opening Informix.

        parts = _split_identity(exposed)
        if len(parts) == 2:
            database = self.options.get("database", "")
            owner, name = parts
        elif len(parts) == 3:
            database, owner, name = parts
        else:
            # Preserve the normal metadata path and its user-facing validation
            # for connector-specific aliases that are not native identities.
            return None
        if not database or not all(_IDENTIFIER.fullmatch(part) for part in (database, owner, name)):
            return None
        native_identity = f"{database}:{_sql_identifier(owner)}.{_sql_identifier(name)}"
        connection_key = self._lakebase_state_namespace()
        table_key = hashlib.sha256(native_identity.encode()).hexdigest()[:24]
        return native_identity, connection_key, table_key

    def _publish_checkpoint_channel_start_before_capacity(
        self,
        table_name: str,
        effective_start: dict[str, Any],
        table_options: dict[str, str],
    ) -> None:
        # Docstring: Publish an authoritative resume checkpoint before waiting for a slot.
        #
        # A canceled microbatch leaves its positional end in Spark's checkpoint but
        # no commit marker. The next update must replay that range, and replay may
        # wait for capacity for minutes. Publishing only after acquiring capacity
        # leaves the paired delete reader unable to initialize for that entire wait.
        # The incoming positional checkpoint already fixes the safe resume boundary,
        # schema id, and fingerprint, so publishing it requires Lakebase only.

        if (
            effective_start.get("phase") not in ("stream", "snapshot")
            or effective_start.get("schema_fingerprint") is None
        ):
            return
        checkpoint = _validated_offset(effective_start)
        exposed = table_options.get("qualified_source_table", table_name)
        identity = self._native_table_state_identity(exposed)
        if identity is None:
            return
        self._publish_channel_start(
            identity,
            exposed,
            self._pipeline_scope(checkpoint),
            (
                int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"]),
                str(checkpoint["schema_id"]),
                str(checkpoint["schema_fingerprint"]),
            ),
        )

    def _shared_table_lsn(
        self,
        table: Table,
        *,
        owner: bool,
        wait: bool = True,
        scope: str | None = None,
    ) -> tuple[int, str] | None:
        scope = scope or self._pipeline_scope()
        namespace = self._immutable_namespace(table, "initialization", scope)
        deadline = time.monotonic() + int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        delay = 0.1
        while True:
            record = self._read_immutable_head(namespace)
            if record is not None:
                self._validate_immutable_record_header(record, "initialization", table.exposed_name)
                if record.get("table") != table.native_identity or record.get("scope") != scope:
                    # 315: Informix immutable initialization table mismatch
                    raise InformixError(_m(315))
                node = record.get("schema")
                if (
                    not isinstance(node, dict)
                    or not isinstance(node.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", str(node.get("id")))
                    or node.get("fingerprint") != _schema_fingerprint(table)
                ):
                    # 316: Invalid Informix immutable initialization schema
                    raise InformixError(_m(316))
                value = self._immutable_lsn(record, "initial_lsn", table.exposed_name)
                if (
                    self._immutable_lsn(node, "start_lsn", table.exposed_name) != value
                    or _table_from_schema_state(node, table.database).native_identity
                    != table.native_identity
                    or node.get("predecessor") is not None
                ):
                    # 316: Invalid Informix immutable initialization schema
                    raise InformixError(_m(316))
                if not owner:
                    snapshot = self._read_immutable_head(
                        self._immutable_namespace(table, "snapshots", scope, str(node["id"]))
                    )
                    if snapshot is None:
                        record = None
                    else:
                        self._validate_immutable_record_header(
                            snapshot, "snapshot", table.exposed_name
                        )
                        if (
                            snapshot.get("scope") != scope
                            or snapshot.get("schema_id") != node["id"]
                            or self._immutable_lsn(snapshot, "initial_lsn", table.exposed_name)
                            != value
                        ):
                            # 317: Invalid immutable snapshot boundary for '{}'
                            raise InformixError(_m(317, table.exposed_name))
                        value = self._immutable_lsn(snapshot, "snapshot_lsn", table.exposed_name)
                if record is not None:
                    self._bridge.validate_initial_lsn(
                        _capture_descriptor(table, _database_encoding(self.options)), value
                    )
                    if owner:
                        schema_winner = self._publish_immutable_head(
                            self._immutable_namespace(table, "schema-nodes", str(node["id"])),
                            {
                                "created_at": record.get("created_at", time.time()),
                                "schema": node,
                            },
                            record_type="schema-node",
                        )
                        self._validate_schema_node_winner(schema_winner, node, table)
                    return value, str(node["id"])
            if owner:
                value = self._bridge.prepare_initial_capture([table.native_identity])
                self._bridge.validate_initial_lsn(
                    _capture_descriptor(table, _database_encoding(self.options)), value
                )
                self._publish_immutable_head(
                    namespace,
                    {
                        "created_at": time.time(),
                        "initial_lsn": str(value),
                        "schema": _schema_state(table, value),
                        "scope": scope,
                        "table": table.native_identity,
                    },
                    record_type="initialization",
                )
                continue
            if not wait:
                return None
            if time.monotonic() >= deadline:
                role = "upsert initialization" if owner else "the table's upsert reader"
                # 318: Timed out waiting for {} to publish shared CDC state for '{}' at '{}'
                raise InformixError(_m(318, role, table.exposed_name, namespace))
            delay = _sleep_with_backoff(deadline, delay)

    def _publish_snapshot_boundary(
        self,
        table: Table,
        schema_id: str,
        initial_lsn: int,
        snapshot_lsn: int,
        pipeline_scope: str,
        *,
        floor: bool = True,
        resnapshot: bool = False,
    ) -> None:
        # ``floor`` False leaves the delete floor as it is (see _replay_keeps_floor);
        # ``resnapshot`` keeps one still in the log (see _publish_delete_snapshot_floor).
        winner = self._publish_immutable_head(
            self._immutable_namespace(table, "snapshots", pipeline_scope, schema_id),
            {
                "created_at": time.time(),
                "initial_lsn": str(initial_lsn),
                "scope": pipeline_scope,
                "schema_id": schema_id,
                "snapshot_lsn": str(snapshot_lsn),
            },
            record_type="snapshot",
        )
        self._validate_immutable_record_header(winner, "snapshot", table.exposed_name)
        if self._immutable_lsn(winner, "initial_lsn", table.exposed_name) != initial_lsn:
            # 319: Conflicting immutable snapshot boundary for '{}'
            raise InformixError(_m(319, table.exposed_name))
        if (
            winner.get("scope") != pipeline_scope
            or winner.get("schema_id") != schema_id
            or self._immutable_lsn(winner, "snapshot_lsn", table.exposed_name) < initial_lsn
        ):
            # 317: Invalid immutable snapshot boundary for '{}'
            raise InformixError(_m(317, table.exposed_name))
        # Durable floor for a delete flow's bootstrap. The per-scope upsert channel-start
        # below is GC'd with its update scope and records the upsert *resume* position,
        # which drifts forward across restarts; this floor is keyed by schema id and
        # pipeline id so it survives scope cleanup and restarts while staying isolated
        # per pipeline, and is forward-moving so a later resnapshot advances it. It is the
        # LSN the current destination rows were captured at -- the correct lower bound for
        # replaying deletes (see the bootstrap path in _read_table_deletes_attempt).
        if floor:
            self._publish_delete_snapshot_floor(
                table, schema_id, snapshot_lsn, pipeline_scope, resnapshot=resnapshot
            )
        # The floor first: a delete reader that started at an earlier snapshot's floor takes
        # this update's start as the sign that the floor is settled (_confirmed_floor_seed).
        # Publish only once the strategy has finalized its safe handoff:
        # incremental snapshots pass initial_lsn == snapshot_lsn, while a
        # blocking consistent snapshot may finish later and must seed deletes
        # from that later snapshot position.
        self._publish_upsert_channel_start(
            table,
            snapshot_lsn,
            schema_id,
            _schema_fingerprint(table),
            pipeline_scope,
        )

    def _find_immutable_schema_record(
        self, table: Table, schema_id: str, scope: str
    ) -> dict[str, object] | None:
        initialization = self._read_immutable_head(
            self._immutable_namespace(table, "initialization", scope)
        )
        if initialization is not None:
            self._validate_immutable_record_header(
                initialization, "initialization", table.exposed_name
            )
            schema = initialization.get("schema")
            if isinstance(schema, dict) and schema.get("id") == schema_id:
                return {
                    "created_at": initialization.get("created_at", time.time()),
                    "schema": schema,
                }
        # A checkpoint is emitted only after its exact schema-node head is
        # committed. Do not scan unbounded, unrelated transition history to
        # compensate for an externally removed historical node.
        return None

    def _lakebase_connection(self) -> Any:
        # Docstring: Return this connector's Postgres connection, provisioning on first use.
        #
        # The connector and its bridge each keep their own connection: the bridge's
        # serves slot operations while a read is in flight, and sharing one across
        # both would serialise state reads behind slot maintenance.

        connection = _open_state_connection(getattr(self, "_lakebase_conn", None))
        if connection is not None:
            return connection
        state = getattr(self, "_lakebase_state", None)
        if state is None:
            identity = "\0".join(
                (
                    self.options.get("hostname", "").strip().rstrip(".").casefold(),
                    str(int(self.options.get("port") or 9088)),
                    self.options.get("server", "").strip().casefold(),
                )
            )
            state = LakebaseState(self.options, identity)
            state.provision()
            self._lakebase_state = state
        connection = state.connect()
        self._lakebase_conn = connection
        return connection

    def _immutable_namespace(self, table: Table, *parts: str) -> str:
        # Docstring: Build the record key identifying one piece of per-table state.
        #
        # Keys are ``<connection>/<table>/<part>/...`` where both leading
        # components are hashes of the Informix identity, so a key is reproducible
        # by any worker without coordination. A component that is not already
        # restricted to a safe character set is hashed, which keeps a scope or
        # schema id containing a separator from forging a different key.

        connection_key, table_key = self._table_state_keys(table)
        safe = [connection_key, table_key]
        for part in parts:
            value = str(part)
            if not re.fullmatch(r"[A-Za-z0-9_.@-]+", value):
                value = hashlib.sha256(value.encode()).hexdigest()
            safe.append(value)
        return "/".join(safe)
