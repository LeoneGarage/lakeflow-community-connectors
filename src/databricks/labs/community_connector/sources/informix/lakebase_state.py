"""Lakebase-backed connection slots and shared CDC state for the Informix connector.

Replaces the UC Volume implementation of connection capacity slots, backlog
hints, connection limits, and immutable per-table state records. Snapshot page
*payloads* deliberately stay on a Volume: they are large gzip blobs that suit
object storage, and only their metadata lives here.

Why this exists at all. The Volume implementation had to synthesise atomicity
from directory operations -- ``mkdir`` as a lock, marker files as ownership
proof, and a three-stage rename fence to reclaim a dead lease. That machinery
was correct only under FUSE semantics that a UC Volume does not reliably give:
``st_size``/``st_mtime_ns``/``st_mode`` writes are silently dropped, so a
filename was the only channel that round-tripped, and a mount whose goofys
daemon dies returns ``ENOTCONN`` for every path at once. Postgres supplies the
one primitive all of it was emulating -- an atomic compare-and-swap -- so a
single ``UPDATE ... FOR UPDATE SKIP LOCKED`` replaces the whole protocol.

Provisioning is self-contained and idempotent. The first reader to need state
creates a Lakebase Autoscaling project, branch, compute endpoint, and schema;
every later reader in every later update finds them and proceeds. Nothing is
pre-created by hand.

Two identities, with different lifetimes and different rights:

* The **state** role, :data:`STATE_ROLE` with ``lakebase.password``, does all
  ordinary state access. It is long-lived, so no control-plane call sits in the
  steady-state path and nothing expires mid-stream, and it needs no rights beyond
  its own tables. The role name is fixed and only the password is configured:
  every pipeline sharing an endpoint must agree on the role that owns the tables,
  so naming it per connection could only misconfigure that.
* The **admin** role, ``lakebase.admin_user``/``lakebase.admin_password``, is used
  only to provision the state role -- create it, or reset its password to the
  configured value. It needs CREATEROLE. When no admin password is configured it
  authenticates with a short-lived OAuth credential minted for the pipeline's own
  workspace identity, which is what makes that the zero-configuration case.

Separating them keeps the credential distributed to every reader process from
being one that can create roles. ``lakebase.password`` is therefore required:
without it there is no long-lived credential for state access at all.

Verified against the workspace API (2026-08-08), because several details are not
what the public shape suggests:

* Autoscaling lives under ``/api/2.0/postgres`` (projects/branches/endpoints),
  *not* ``/api/2.0/database/instances``. The latter offers only fixed CU_1..CU_8
  and has no auto-suspend at all, so it cannot scale to zero.
* ``suspend_timeout_duration`` is settable only through a project's
  ``default_endpoint_settings``, and only takes effect for endpoints created
  afterwards. Patching ``spec.suspend_timeout_duration`` on an existing endpoint
  is rejected (``Unknown field path in update_mask``), and a project's default
  is *not* retroactively applied to its already-created ``primary``. Both values
  must therefore be supplied at project-creation time, which is what
  :func:`ensure_project` does.
* A branch permits exactly one read-write endpoint ("read_write endpoint already
  exists"), so the auto-created ``primary`` is the endpoint to use rather than
  one to add alongside.
* ``POST /api/2.0/postgres/credentials`` takes ``{"endpoint": ...}`` alone. The
  documented ``claims`` array is rejected here ("Unsupported permission set"),
  so it is deliberately not sent.
* Writable fields live under ``spec``; ``status`` is the read-back view. Sending
  ``default_endpoint_settings`` at the top level, or nested under ``status``, is
  accepted with HTTP 200 and *silently ignored* -- the endpoint then comes up at
  the 1/1 CU, 86400s defaults.
* Password logins require ``spec.enable_pg_native_login``, which defaults to
  false. It is set at project creation, alongside the endpoint defaults.
* There is no API for setting a role's password: no field on the role spec and no
  ``:resetPassword``-style method anywhere in the surface. A password can only be
  set in SQL, and only for a role created in SQL -- an API-created role cannot be
  altered by this identity ("permission denied to alter role"), because the
  control plane owns it.
* Postgres reports an absent role and a wrong password with the same message and
  no SQLSTATE, so a failed login cannot be classified from the error. Deciding
  between creating a role and resetting its password requires querying
  ``pg_roles`` as the OAuth identity.
"""

from __future__ import annotations

import importlib
import json
from typing import Any

from databricks.labs.community_connector.sources.informix.lakebase_state_store import (
    _CLAIM_STATE_GC,
    _CONNECTION_LOST_ERROR_TYPES,
    _DELETE_OBSOLETE_SCOPED_RECORDS,
    _DELETE_STALE_ACTIVITY,
    _DELETE_STALE_PIPELINE_STATE,
    _DELETE_UNUSED_TABLE_STATE,
    _INSERT_RECORD,
    _PUBLISH_MAXIMUM_LSN_RECORD,
    _PUBLISH_MINIMUM_LSN_RECORD,
    _PUBLISH_OWN_START_RECORD,
    _SELECT_RECORD,
    _TOUCH_TABLE_ACTIVITY,
    _sql,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    LakebaseStateError,
)


def connection_lost_error_types() -> tuple[type[BaseException], ...]:
    # Docstring: Return the driver exception types raised when a connection is severed.
    #
    # A Lakebase (Postgres) connection left idle -- e.g. across a long wait for
    # connection capacity -- is reaped by the server, and the next query on it
    # fails with ``OperationalError`` ("SSL SYSCALL error: EOF detected", "server
    # closed the connection unexpectedly") or ``InterfaceError`` ("connection
    # already closed"). Callers catch these to reconnect and retry an idempotent
    # state operation. Resolved once against whichever driver is installed
    # (``psycopg2`` or ``psycopg`` v3); an empty tuple when neither is importable
    # so the caller simply never matches and propagates.

    cached = _CONNECTION_LOST_ERROR_TYPES.get("types")
    if cached is not None:
        return cached
    types: list[type[BaseException]] = []
    for module_name in ("psycopg2", "psycopg"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        for attribute in ("OperationalError", "InterfaceError"):
            error_type = getattr(module, attribute, None)
            if isinstance(error_type, type) and issubclass(error_type, BaseException):
                types.append(error_type)
    # setdefault resolves the race: concurrent callers may each resolve the tuple,
    # but every one returns whichever landed first.
    return _CONNECTION_LOST_ERROR_TYPES.setdefault("types", tuple(types))


def _as_dict(value: Any) -> dict[str, Any]:
    # Docstring: Normalise a jsonb column, which drivers return as dict or as text.

    if isinstance(value, dict):
        return value
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise LakebaseStateError("state record is not a JSON object")
    return decoded


def publish_state_record(
    connection: Any,
    namespace: str,
    record_key: str,
    record: dict[str, Any],
    *,
    record_type: str = "generic",
) -> dict[str, Any]:
    # Docstring: Elect exactly one record for a key and return the elected value.
    #
    # Always returns the *winning* record, which may be another writer's. Callers
    # depend on that: a record is the shared truth for a key, so a loser must adopt
    # the winner rather than assume its own value took effect.

    payload = json.dumps(record, sort_keys=True)
    with connection.cursor() as cursor:
        cursor.execute(
            _INSERT_RECORD,
            {
                "namespace": namespace,
                "record_key": record_key,
                "record": payload,
                "record_type": record_type,
            },
        )
        row = cursor.fetchone()
        if row is None:
            cursor.execute(_SELECT_RECORD, {"namespace": namespace, "record_key": record_key})
            row = cursor.fetchone()
    connection.commit()
    if row is None:
        # The row existed for the conflict yet vanished before the re-read. Only
        # an external deletion can do this, and silently returning the caller's
        # own unelected value would let two readers disagree about shared truth.
        raise LakebaseStateError(f"state record {namespace}/{record_key} vanished during election")
    return _as_dict(row[0])


def _publish_conditional_record(
    connection: Any,
    statement: str,
    namespace: str,
    record_key: str,
    record: dict[str, Any],
    record_type: str,
    what: str,
) -> dict[str, Any]:
    # Docstring: Publish a record by an ON CONFLICT ... DO UPDATE ... WHERE statement and
    # return the elected value, the stored one where the condition kept it.

    parameters = {
        "namespace": namespace,
        "record_key": record_key,
        "record": json.dumps(record, sort_keys=True),
        "record_type": record_type,
    }
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        row = cursor.fetchone()
        if row is None:
            cursor.execute(_SELECT_RECORD, parameters)
            row = cursor.fetchone()
    connection.commit()
    if row is None:
        raise LakebaseStateError(f"state record {namespace}/{record_key} vanished during {what}")
    return _as_dict(row[0])


def publish_minimum_lsn_state_record(
    connection: Any,
    namespace: str,
    record_key: str,
    record: dict[str, Any],
    *,
    record_type: str,
) -> dict[str, Any]:
    # Docstring: Atomically publish the lowest compatible ``start_lsn`` for one key.
    #
    # A later CDC poll can discover an older still-open transaction, moving its
    # effective restart position backwards. PostgreSQL serializes the conflict
    # update so concurrent upsert workers converge on the minimum. Incompatible
    # identity metadata prevents the update; schema metadata travels with the
    # winning minimum because schema transitions can legitimately occur later in
    # the same update.

    return _publish_conditional_record(
        connection,
        _PUBLISH_MINIMUM_LSN_RECORD,
        namespace,
        record_key,
        record,
        record_type,
        "minimum-LSN publication",
    )


def publish_maximum_lsn_state_record(
    connection: Any,
    namespace: str,
    record_key: str,
    record: dict[str, Any],
    *,
    record_type: str,
) -> dict[str, Any]:
    # Docstring: Atomically publish the highest compatible ``start_lsn`` for one key.
    #
    # The scope-independent counterpart of :func:`publish_minimum_lsn_state_record`,
    # used for the delete-bootstrap snapshot floor: within an update the highest boundary
    # wins, and a record whose ``snapshot_scope`` differs (a later update's snapshot,
    # which the destination now holds) replaces it whatever its LSN; it stays put
    # otherwise. PostgreSQL serializes the conflict update so concurrent publishers
    # converge.

    return _publish_conditional_record(
        connection,
        _PUBLISH_MAXIMUM_LSN_RECORD,
        namespace,
        record_key,
        record,
        record_type,
        "maximum-LSN publication",
    )


def publish_own_start_state_record(
    connection: Any, namespace: str, record_key: str, record: dict[str, Any]
) -> dict[str, Any]:
    # Docstring: Publish a full refresh's own start, or a start after it (see
    # _PUBLISH_OWN_START_RECORD).

    return _publish_conditional_record(
        connection,
        _PUBLISH_OWN_START_RECORD,
        namespace,
        record_key,
        record,
        "own-start",
        "own-start publication",
    )


def read_state_record(connection: Any, namespace: str, record_key: str) -> dict[str, Any] | None:
    # Docstring: Return an elected record, or ``None`` when the key is unwritten.

    with connection.cursor() as cursor:
        cursor.execute(_SELECT_RECORD, {"namespace": namespace, "record_key": record_key})
        row = cursor.fetchone()
    # End the implicit transaction a bare SELECT opens. Without this the connection
    # is left "idle in transaction" and Lakebase reaps it after
    # idle_in_transaction_session_timeout -- fatal when a caller interleaves a slow
    # non-Lakebase step (a full-table snapshot scan) before its next state write.
    connection.rollback()
    if row is None:
        return None
    return _as_dict(row[0])


def delete_obsolete_scoped_state_records(
    connection: Any,
    namespace: str,
    record_key_prefix: str,
    pipeline_scope_prefix: str,
    current_scope: str,
    retained_scope: str | None = None,
) -> int:
    # Docstring: Delete old update scopes while preserving the scope in Spark's checkpoint.

    with connection.cursor() as cursor:
        cursor.execute(
            _DELETE_OBSOLETE_SCOPED_RECORDS,
            {
                "namespace": namespace,
                "record_key_pattern": f"{record_key_prefix}/%",
                "pipeline_scope_pattern": f"{pipeline_scope_prefix}%",
                "current_scope": current_scope,
                "retained_scope": retained_scope,
            },
        )
        deleted = len(cursor.fetchall())
    connection.commit()
    return deleted


# --- spilled CDC runs ------------------------------------------------------
#
# One record per spilled run per pipeline: its transaction's commit LSN once a poll saw
# the COMMIT, and the channels that have committed past it. An update merges its fields in
# and reads the record back in one statement, so of two channels finishing together
# exactly one sees the set complete (Postgres serializes the conflicting update on the
# row) and deletes the pages. The record has no ``scope`` field, so the obsolete-update
# cleanup leaves it alone; the table-state GC removes it with the table.
_MERGE_SPILL_RUN = _sql(
    """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (%(namespace)s, %(record_key)s, %(record)s::jsonb, 'cdc-spill-run')
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = state_records.record || (EXCLUDED.record - 'done') || jsonb_build_object(
  'done', COALESCE(state_records.record->'done', '{}'::jsonb)
    || COALESCE(EXCLUDED.record->'done', '{}'::jsonb)
)
WHERE state_records.record_type = 'cdc-spill-run'
RETURNING record
"""
)

_DELETE_SPILL_RUN = _sql(
    """
DELETE FROM state_records
WHERE namespace = %(namespace)s AND record_key = %(record_key)s
  AND record_type = 'cdc-spill-run'
"""
)

_DELETE_EXPIRED_SPILL_RUNS = _sql(
    """
DELETE FROM state_records
WHERE namespace = %(namespace)s
  AND record_type = 'cdc-spill-run'
  AND record_key LIKE %(record_key_pattern)s
  AND created_at < now() - make_interval(secs => %(retention_seconds)s)
RETURNING record
"""
)

_SELECT_SPILL_RUNS = _sql(
    """
SELECT record FROM state_records
WHERE namespace = %(namespace)s
  AND record_type = 'cdc-spill-run'
  AND record_key LIKE %(record_key_pattern)s
  AND NOT (COALESCE(record->'done', '{}'::jsonb) ? %(channel)s)
"""
)


def merge_spill_run(
    connection: Any, namespace: str, record_key: str, record: dict[str, Any]
) -> dict[str, Any]:
    # Docstring: Merge ``record`` into a spilled run's record, ``done`` channel by channel;
    # return the record as stored.

    with connection.cursor() as cursor:
        cursor.execute(
            _MERGE_SPILL_RUN,
            {
                "namespace": namespace,
                "record_key": record_key,
                "record": json.dumps(record, sort_keys=True),
            },
        )
        row = cursor.fetchone()
    connection.commit()
    if row is None:
        raise LakebaseStateError(f"state record {namespace}/{record_key} is not a spilled run")
    return _as_dict(row[0])


def delete_spill_run(connection: Any, namespace: str, record_key: str) -> None:
    # Docstring: Remove a spilled run's record (its transaction rolled back). Idempotent.

    with connection.cursor() as cursor:
        cursor.execute(_DELETE_SPILL_RUN, {"namespace": namespace, "record_key": record_key})
    connection.commit()


def list_spill_runs(
    connection: Any,
    namespace: str,
    spill_prefix: str,
    scope: str,
    channel: str,
    retention_seconds: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    # Docstring: A table's spilled-run records under one scope that ``channel`` has not
    # finished, and the records this call expired.
    #
    # Records older than ``retention_seconds`` go first, under every scope of the table, and
    # come back so the caller can delete their pages: a pipeline that no longer runs has no
    # reader left to expire its own. 0 keeps them.

    expired: list[dict[str, Any]] = []
    with connection.cursor() as cursor:
        if retention_seconds > 0:
            cursor.execute(
                _DELETE_EXPIRED_SPILL_RUNS,
                {
                    "namespace": namespace,
                    "record_key_pattern": f"{spill_prefix}/%",
                    "retention_seconds": float(retention_seconds),
                },
            )
            expired = [_as_dict(row[0]) for row in cursor.fetchall()]
        cursor.execute(
            _SELECT_SPILL_RUNS,
            {
                "namespace": namespace,
                "record_key_pattern": f"{spill_prefix}/{scope}/%",
                "channel": channel,
            },
        )
        rows = cursor.fetchall()
    connection.commit()
    return [_as_dict(row[0]) for row in rows], expired


# One row per staging root: when its next sweep of every table staged there is due. A
# reader takes it once the time has passed, so of the readers staging there one sweeps
# per interval.
_CLAIM_STAGING_SWEEP = _sql(
    """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (
  %(namespace)s, %(record_key)s,
  jsonb_build_object(
    'format_version', 1,
    'next_run_at', extract(epoch FROM clock_timestamp()) + %(interval_seconds)s
  ),
  'staging-sweep'
)
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = EXCLUDED.record
WHERE state_records.record_type = 'staging-sweep'
  AND (state_records.record->>'next_run_at')::double precision
      <= extract(epoch FROM clock_timestamp())
RETURNING record
"""
)


def claim_staging_sweep(
    connection: Any, namespace: str, record_key: str, interval_seconds: float
) -> bool:
    # Docstring: Whether this caller runs the staging sweep due under ``record_key``.

    with connection.cursor() as cursor:
        cursor.execute(
            _CLAIM_STAGING_SWEEP,
            {
                "namespace": namespace,
                "record_key": record_key,
                "interval_seconds": float(interval_seconds),
            },
        )
        claimed = cursor.fetchone() is not None
    connection.commit()
    return claimed


def touch_table_activity(
    connection: Any,
    namespace: str,
    record_key: str,
    table_prefix: str,
    pipeline_id: str,
    touch_interval_seconds: float,
) -> bool:
    # Docstring: Refresh one table/pipeline activity record when its write interval elapsed.

    with connection.cursor() as cursor:
        cursor.execute(
            _TOUCH_TABLE_ACTIVITY,
            {
                "namespace": namespace,
                "record_key": record_key,
                "table_prefix": table_prefix,
                "pipeline_id": pipeline_id,
                "touch_interval_seconds": touch_interval_seconds,
            },
        )
        touched = cursor.fetchone() is not None
    connection.commit()
    return touched


def collect_stale_table_state(
    connection: Any,
    namespace: str,
    gc_record_key: str,
    retention_days: int,
    gc_interval_hours: float,
) -> int:
    # Docstring: Run at most one database-scoped table-state collection per interval.

    with connection.cursor() as cursor:
        cursor.execute(
            _CLAIM_STATE_GC,
            {
                "namespace": namespace,
                "record_key": gc_record_key,
                "gc_interval_seconds": gc_interval_hours * 3600,
            },
        )
        if cursor.fetchone() is None:
            connection.commit()
            return 0
        cursor.execute("SELECT extract(epoch FROM clock_timestamp())")
        cutoff_epoch = float(cursor.fetchone()[0]) - retention_days * 86400
        parameters = {"namespace": namespace, "cutoff_epoch": cutoff_epoch}
        deleted = 0
        for statement in (
            _DELETE_STALE_PIPELINE_STATE,
            _DELETE_UNUSED_TABLE_STATE,
            _DELETE_STALE_ACTIVITY,
        ):
            cursor.execute(statement, parameters)
            deleted += len(cursor.fetchall())
    connection.commit()
    return deleted
