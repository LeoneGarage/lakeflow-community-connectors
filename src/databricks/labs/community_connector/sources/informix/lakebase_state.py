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

import hashlib
import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any

_LOGGER = logging.getLogger(__name__)

# Autoscaling bounds. 0.5 is the smallest compute unit the API accepts; the
# ceiling exists so a runaway reader cannot scale the endpoint without limit.
# Measured cost at the floor is well under the S3 request charges this replaces.
_DEFAULT_MIN_CU = 0.5
_DEFAULT_MAX_CU = 2.0
# Idle seconds before the endpoint suspends to zero compute. Measured resume
# cost from IDLE is ~3.3s to connect plus one ordinary query, so a short timeout
# is affordable: a continuously streaming pipeline never reaches it, and a
# pipeline that does was idle anyway.
_DEFAULT_SUSPEND_SECONDS = 60
_AUTO_PROJECT_PREFIX = "informix-state-"

# A suspended endpoint must be allowed to wake. This bounds the *connect*, not a
# steady-state query, and is generous relative to the ~3.3s measured resume.
_CONNECT_TIMEOUT_SECONDS = 120
# How long a slot-queue ticket stays authoritative without a heartbeat. A waiter
# heartbeats every sweep (<= 2s), so this is many sweeps of slack -- enough that a
# briefly-stalled live waiter is never mistaken for a crashed one -- yet far below the
# 120s slot lease, so a truly dead waiter's ticket stops blocking the queue quickly.
_DEFAULT_WAITER_TTL_SECONDS = 15.0
# OAuth credentials last an hour; recycle pooled connections well before that so
# no connection is ever handed out holding a nearly-expired token.
_CREDENTIAL_REFRESH_SECONDS = 2700

_DEFAULT_DATABASE = "databricks_postgres"
# Point libpq's client-certificate lookup at a path that does not exist.
#
# Authentication here is by password or OAuth credential, never by client
# certificate, but libpq still probes ``$HOME/.postgresql/postgresql.crt`` and
# treats *any* error opening it as fatal -- including EACCES. The pipeline's reader
# processes run with ``HOME=/root``, which they cannot traverse, so every connection
# failed with ``could not open certificate file ... Permission denied``.
#
# A missing explicit path is skipped, which an empty string is not: an empty
# ``sslcert`` falls back to the default location and fails again, and ``/dev/null``
# is read and rejected ("no start line"). Verified against a live endpoint, where
# this still negotiates TLSv1.3 -- it disables only client-certificate auth, which
# is unused, and leaves ``sslmode=require`` in force.
_NO_CLIENT_CERTIFICATE = {
    "sslcert": "/nonexistent/informix-state-no-client-cert.crt",
    "sslkey": "/nonexistent/informix-state-no-client-cert.key",
}
# The state role is fixed rather than configurable. It exists solely to own this
# connector's state tables, so letting a connection name it would add a way to
# misconfigure the pipeline without adding anything it could express: two
# pipelines pointing at one endpoint must agree on the role that owns the tables.
STATE_ROLE = "informix_state_user"
_DEFAULT_BRANCH = "production"
_DEFAULT_ENDPOINT = "primary"

# Long-running provisioning calls return an operation envelope; poll until done.
_OPERATION_POLL_SECONDS = 2.0
_OPERATION_TIMEOUT_SECONDS = 900.0

_API_TIMEOUT_SECONDS = 120.0
_API_ATTEMPTS = 4
_DEFAULT_PROVISION_TIMEOUT_SECONDS = 180.0


class LakebaseStateError(RuntimeError):
    """Raised when Lakebase-backed state cannot be provisioned or reached."""


def project_id_for_connection(identity: str, prefix: str = "informix-state") -> str:
    """Derive a stable, DNS-compliant project id from a connection identity.

    The id must be reproducible: every worker in every update has to resolve the
    same project without coordinating, and the project is the unit that ties
    state to one Informix connection. Hashing the identity also avoids leaking a
    hostname into a globally visible resource name.
    """

    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


class _WorkspaceApi:
    """Minimal REST client for the workspace control plane.

    Deliberately stdlib-only. This module is imported inside Spark workers where
    adding a dependency is a deployment problem, and the surface needed here is
    four verbs against JSON endpoints.
    """

    def __init__(self, host: str, token: str) -> None:
        self._host = host.rstrip("/")
        self._token = token

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        tolerate_missing: bool = False,
        deadline: float | None = None,
    ) -> dict[str, Any] | None:
        url = f"{self._host}{path}"
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        last_error: Exception | None = None
        for attempt in range(_API_ATTEMPTS):
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise LakebaseStateError(f"Lakebase provisioning timed out during {method} {path}")
            request = urllib.request.Request(
                url,
                data=payload,
                method=method,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                timeout = (
                    _API_TIMEOUT_SECONDS
                    if remaining is None
                    else min(_API_TIMEOUT_SECONDS, max(0.1, remaining))
                )
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    raw = response.read()
                return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as error:
                if error.code == 404 and tolerate_missing:
                    return None
                # 409 means someone else won a concurrent create; the caller
                # treats that as success, so do not burn retries on it.
                if error.code == 409:
                    raise LakebaseStateError(f"conflict on {method} {path}") from error
                if error.code < 500 and error.code != 429:
                    detail = error.read().decode("utf-8", "replace")[:400]
                    raise LakebaseStateError(
                        f"{method} {path} failed with HTTP {error.code}: {detail}"
                    ) from error
                last_error = error
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
                last_error = error
            if attempt + 1 < _API_ATTEMPTS:
                delay = min(1.5 * (2**attempt), 8.0)
                if deadline is not None:
                    delay = min(delay, max(0.0, deadline - time.monotonic()))
                if delay:
                    time.sleep(delay)
        raise LakebaseStateError(f"{method} {path} failed after {_API_ATTEMPTS} attempts") from (
            last_error
        )

    def await_operation(
        self, envelope: dict[str, Any] | None, *, deadline: float | None = None
    ) -> dict[str, Any]:
        """Resolve a long-running-operation envelope to its final response.

        Provisioning calls may return either a finished resource or an operation
        that has to be polled; both shapes arrive on the same code path.
        """

        if not envelope:
            return {}
        if "name" not in envelope or "/operations/" not in str(envelope.get("name", "")):
            return envelope
        operation_deadline = time.monotonic() + _OPERATION_TIMEOUT_SECONDS
        if deadline is not None:
            operation_deadline = min(operation_deadline, deadline)
        operation = envelope
        while not operation.get("done"):
            if time.monotonic() > operation_deadline:
                raise LakebaseStateError(
                    f"Lakebase provisioning timed out waiting for operation {operation.get('name')}"
                )
            time.sleep(
                min(_OPERATION_POLL_SECONDS, max(0.0, operation_deadline - time.monotonic()))
            )
            operation = (
                self.request(
                    "GET", f"/api/2.0/postgres/{operation['name']}", deadline=operation_deadline
                )
                or {}
            )
        if "error" in operation:
            raise LakebaseStateError(f"operation failed: {operation['error']}")
        return operation.get("response") or {}


# Workspace credentials captured on the driver, for use where none exist.
#
# Measured in a serverless pipeline (2026-08-08): ambient workspace identity is
# resolvable *only* while the pipeline module executes on the driver, where
# ``WorkspaceClient()`` returns ``auth_type=runtime``. Every process the reader
# actually runs in -- the Python streaming source runner and Spark's Python
# workers -- has no credential material at all: no auth environment variables, no
# ``~/.databrickscfg``, no active Spark session, and ``dbutils`` itself fails to
# initialise. ``HOME=/root`` there is unreadable, so the SDK's config-file loader
# raises ``PermissionError`` before it even reaches auth resolution; repointing
# ``HOME`` merely exchanges that for "cannot configure default credentials".
#
# Provisioning cannot simply move to the driver: at module-execution time no
# connection and no options exist yet, so the ``lakebase.*`` overrides it needs
# are unreachable. Options and ambient auth are available in disjoint processes.
# Capturing the credential on the driver and carrying it to where the options
# live is what bridges them. Held in a dict so the merged single-file deployment,
# which turns module globals into function locals, can still mutate it.
_CAPTURED_WORKSPACE: dict[str, str] = {}


def capture_workspace_credentials() -> dict[str, str]:
    """Record the driver's ambient workspace credentials for later use.

    Call once while the pipeline module executes on the driver. Failure is not
    fatal here: an explicitly configured host/token still works, and raising
    would break registration for deployments that never touch Lakebase.
    """

    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Only a Databricks runtime has an ambient identity worth capturing. This
        # keeps unit tests -- which import and register the module -- from trying
        # to authenticate at all.
        return _CAPTURED_WORKSPACE
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        client = WorkspaceClient()
        header = client.config.authenticate() or {}
        token = str(header.get("Authorization", "")).split(" ", 1)[-1]
        host = str(client.config.host or "")
        if host and token:
            _CAPTURED_WORKSPACE["host"] = host
            _CAPTURED_WORKSPACE["token"] = token
            _LOGGER.info("Captured workspace credentials for Lakebase provisioning")
    except Exception:  # pragma: no cover - depends on runtime
        # Debug, not warning: a package import outside a Databricks runtime takes
        # this path routinely and legitimately.
        _LOGGER.debug("No ambient workspace credentials on the driver", exc_info=True)
    return _CAPTURED_WORKSPACE


def _workspace_credentials(options: dict[str, str]) -> tuple[str, str]:
    """Resolve the workspace host and bearer token for control-plane calls.

    Prefers explicit options, then the credential captured on the driver, then
    the ambient identity. The captured value is what makes this work inside a
    pipeline at all, because the calling process has no identity of its own.
    """

    host = (
        options.get("lakebase.workspace.host")
        or _CAPTURED_WORKSPACE.get("host")
        or os.environ.get("DATABRICKS_HOST")
        or ""
    ).strip()
    token = (
        options.get("lakebase.workspace.token")
        or _CAPTURED_WORKSPACE.get("token")
        or os.environ.get("DATABRICKS_TOKEN")
        or ""
    ).strip()
    if host and not host.startswith("http"):
        host = f"https://{host}"
    if not host or not token:
        # The SDK resolves notebook/job/serverless credentials that are not
        # exposed as environment variables. Imported lazily so the module keeps
        # working where the SDK is absent but explicit options are supplied.
        try:
            from databricks.sdk import WorkspaceClient  # noqa: PLC0415
        except ImportError as error:  # pragma: no cover - depends on runtime
            raise LakebaseStateError(
                "Lakebase state needs a workspace host and token; set "
                "lakebase.workspace.host/lakebase.workspace.token or install databricks-sdk"
            ) from error
        client = WorkspaceClient()
        host = host or client.config.host
        token = token or client.config.authenticate()["Authorization"].split(" ", 1)[1]
    return host, token


def ensure_project(
    api: _WorkspaceApi,
    project_id: str,
    *,
    min_cu: float = _DEFAULT_MIN_CU,
    max_cu: float = _DEFAULT_MAX_CU,
    suspend_seconds: int = _DEFAULT_SUSPEND_SECONDS,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Create the project if absent and return the endpoint's connection facts.

    ``default_endpoint_settings`` is supplied at creation because that is the
    only point at which ``suspend_timeout_duration`` reaches the auto-created
    ``primary`` endpoint (see module docstring). An existing project is left
    exactly as it is: adjusting a live endpoint's capacity is an operator
    decision, not something a reader should do behind the operator's back.
    """

    deadline_kwargs = {"deadline": deadline} if deadline is not None else {}
    existing = api.request(
        "GET",
        f"/api/2.0/postgres/projects/{project_id}",
        tolerate_missing=True,
        **deadline_kwargs,
    )
    if existing is not None and existing.get("delete_time"):
        # Lakebase retains a deleted project as a tombstone until its purge
        # time. GET still returns that tombstone, but it has no branch or
        # endpoint and its id cannot be recreated. Purge it explicitly so the
        # connector can safely recreate its deterministic project id.
        purged = api.request(
            "DELETE",
            f"/api/2.0/postgres/projects/{project_id}?purge=true",
            **deadline_kwargs,
        )
        if deadline is None:
            api.await_operation(purged)
        else:
            api.await_operation(purged, deadline=deadline)
        existing = api.request(
            "GET",
            f"/api/2.0/postgres/projects/{project_id}",
            tolerate_missing=True,
            **deadline_kwargs,
        )
        if existing is not None:
            raise LakebaseStateError(f"purged Lakebase project {project_id} is still visible")
        _LOGGER.info("Purged deleted Lakebase project tombstone %s", project_id)
    if existing is None:
        body = {
            "spec": {
                "default_endpoint_settings": {
                    "autoscaling_limit_min_cu": min_cu,
                    "autoscaling_limit_max_cu": max_cu,
                    "suspend_timeout_duration": f"{int(suspend_seconds)}s",
                },
                # Password (SCRAM) logins are refused unless this is on, and it is
                # off by default. Set here because creation is also the only point
                # the endpoint settings above take effect, so one call does both.
                "enable_pg_native_login": True,
            }
        }
        try:
            created = api.request(
                "POST",
                f"/api/2.0/postgres/projects?project_id={project_id}",
                body,
                **deadline_kwargs,
            )
            if deadline is None:
                api.await_operation(created)
            else:
                api.await_operation(created, deadline=deadline)
            _LOGGER.info(
                "Created Lakebase project %s (%.1f-%.1f CU, suspend %ss)",
                project_id,
                min_cu,
                max_cu,
                suspend_seconds,
            )
        except LakebaseStateError:
            # A concurrent worker may have created it between our GET and POST.
            # Re-reading is the check that matters; only a still-absent project
            # is a real failure.
            if (
                api.request(
                    "GET",
                    f"/api/2.0/postgres/projects/{project_id}",
                    tolerate_missing=True,
                    **deadline_kwargs,
                )
                is None
            ):
                raise
    return _endpoint_facts(api, project_id, deadline=deadline)


def _endpoint_facts(
    api: _WorkspaceApi, project_id: str, *, deadline: float | None = None
) -> dict[str, Any]:
    """Read the read-write endpoint's host and resource name.

    Waits for the endpoint to exist because project creation provisions it
    asynchronously; an IDLE endpoint is ready to use, since connecting is what
    wakes it.
    """

    path = (
        f"/api/2.0/postgres/projects/{project_id}"
        f"/branches/{_DEFAULT_BRANCH}/endpoints/{_DEFAULT_ENDPOINT}"
    )
    endpoint_deadline = time.monotonic() + _OPERATION_TIMEOUT_SECONDS
    if deadline is not None:
        endpoint_deadline = min(endpoint_deadline, deadline)
    while True:
        request_kwargs = {"deadline": endpoint_deadline} if deadline is not None else {}
        endpoint = api.request("GET", path, tolerate_missing=True, **request_kwargs)
        status = (endpoint or {}).get("status") or {}
        host = (status.get("hosts") or {}).get("host")
        if host:
            return {
                "endpoint": (
                    f"projects/{project_id}/branches/{_DEFAULT_BRANCH}"
                    f"/endpoints/{_DEFAULT_ENDPOINT}"
                ),
                "host": host,
                "state": status.get("current_state"),
                "min_cu": status.get("autoscaling_limit_min_cu"),
                "max_cu": status.get("autoscaling_limit_max_cu"),
                "suspend": status.get("suspend_timeout_duration"),
            }
        if time.monotonic() > endpoint_deadline:
            raise LakebaseStateError(
                f"Lakebase provisioning timed out waiting for endpoint for project {project_id}"
            )
        time.sleep(min(_OPERATION_POLL_SECONDS, max(0.0, endpoint_deadline - time.monotonic())))


def generate_credential(api: _WorkspaceApi, endpoint: str) -> str:
    """Mint a short-lived Postgres OAuth token for ``endpoint``.

    Sends ``endpoint`` alone: the documented ``claims`` array is rejected by this
    workspace's API (see module docstring), and the caller's own identity already
    scopes the credential.
    """

    response = api.request("POST", "/api/2.0/postgres/credentials", {"endpoint": endpoint})
    token = (response or {}).get("token")
    if not token:
        raise LakebaseStateError(f"no Postgres credential returned for {endpoint}")
    return token


def _quoted_identifier(connection: Any, name: str) -> str:
    """Return ``name`` safely quoted for use as a Postgres identifier.

    A role name arrives from a connection option and lands in DDL, where it
    cannot be a bind parameter. ``quote_ident`` is the server's own quoting, so
    the escaping matches exactly what the server will parse.
    """

    with connection.cursor() as cursor:
        cursor.execute("SELECT quote_ident(%s)", (name,))
        row = cursor.fetchone()
    if not row or not row[0]:
        raise LakebaseStateError(f"could not quote Lakebase role name {name!r}")
    return str(row[0])


def ensure_login_role(admin_connection: Any, database: str, user: str, password: str) -> str:
    """Create the password login role, or reset its password if it exists.

    Runs as the OAuth-authenticated workspace identity, which is the only caller
    able to do this. Returns ``"created"`` or ``"reset"`` to say which happened.

    Reset rather than drop-and-recreate, deliberately. Once the role owns the
    state tables -- which it does from its first run onwards -- ``DROP ROLE``
    fails with ``DependentObjectsStillExist``, and the forced route
    (``REASSIGN OWNED``/``DROP OWNED``) fails with ``InsufficientPrivilege``
    because this identity holds CREATEROLE rather than superuser. Dropping would
    also be the wrong outcome: the role owns ``conn_slots``, ``backlog_hints``
    and ``state_records``, so removing it would discard the shared CDC state and
    force every table to resnapshot. ``ALTER ROLE ... PASSWORD`` achieves the
    same end -- the previous password stops working immediately -- while leaving
    the data and its ownership untouched (all verified against a live endpoint).
    """

    identifier = _quoted_identifier(admin_connection, user)
    database_identifier = _quoted_identifier(admin_connection, database)
    with admin_connection.cursor() as cursor:
        # The Python lock in LakebaseState.provision() is process-local, while a
        # Lakeflow pipeline starts one Python worker per flow. Serialize the
        # cluster-wide role tuple inside Postgres itself before inspecting or
        # changing it. This lock needs no connector schema and is released by
        # the commit/rollback below, so it is safe during first provisioning.
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"lakeflow-informix-login-role:{user}",),
        )
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
        exists = cursor.fetchone() is not None
        # LOGIN is stated explicitly in both branches: a pre-existing role may
        # have been created without it, and a role that cannot log in would fail
        # validation forever with an error indistinguishable from a bad password.
        verb = "ALTER" if exists else "CREATE"
        try:
            cursor.execute(f"{verb} ROLE {identifier} WITH LOGIN PASSWORD %s", (password,))
        except Exception as error:
            sqlstate = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)
            constraint = getattr(getattr(error, "diag", None), "constraint_name", None)
            if exists or sqlstate != "23505" or constraint != "pg_authid_rolname_index":
                raise
            # A caller not using this advisory-lock protocol created the global
            # role after our pg_roles read.
            # PostgreSQL resolves the unique-index conflict only after the
            # winning CREATE commits, and every worker has the same configured
            # password. The failed CREATE aborts this transaction; rolling back
            # is sufficient. Issuing ALTER here would make all losing workers
            # update pg_authid concurrently and can fail with "tuple concurrently
            # updated".
            admin_connection.rollback()
            exists = True
    with admin_connection.cursor() as cursor:
        # Idempotent, and required for the role to create and own the schema.
        cursor.execute(f"GRANT ALL ON SCHEMA public TO {identifier}")
        cursor.execute(f"GRANT CREATE ON DATABASE {database_identifier} TO {identifier}")
    admin_connection.commit()
    return "reset" if exists else "created"


# Schema for every piece of state that moved off the Volume. Written with
# IF NOT EXISTS so concurrent provisioners converge instead of racing.
#
# ``conn_slots`` rows are pre-seeded per namespace, so acquisition is an UPDATE
# of a free row rather than an INSERT. That keeps the table a fixed size and
# makes capacity a property of how many rows exist, which cannot be exceeded by
# any interleaving.
_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS conn_slots (
        namespace   text        NOT NULL,
        slot_id     integer     NOT NULL,
        owner       text,
        epoch       bigint      NOT NULL DEFAULT 0,
        scope       text,
        renewed_at  timestamptz,
        PRIMARY KEY (namespace, slot_id)
    )
    """,
    # Partial index: waiters only ever look for free or expired rows.
    """
    CREATE INDEX IF NOT EXISTS conn_slots_free_idx
        ON conn_slots (namespace, slot_id) WHERE owner IS NULL
    """,
    # Fair waiter queue: one row per reader currently blocked on a slot, carrying
    # its eligible band [floor, ceiling). ``ticket_id`` is a global monotonic
    # sequence, so a smaller ticket enqueued earlier -- acquisition consults it to
    # give the oldest still-heartbeating waiter first claim on each slot (see
    # _ACQUIRE_SLOT_FAIR), turning the raw race into per-slot FIFO.
    """
    CREATE TABLE IF NOT EXISTS slot_waiters (
        namespace    text        NOT NULL,
        ticket_id    bigserial   NOT NULL,
        owner        text        NOT NULL,
        floor        integer     NOT NULL,
        ceiling      integer     NOT NULL,
        heartbeat_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, ticket_id)
    )
    """,
    # Ordering scans walk a namespace's tickets oldest-first; the reaper filters by age.
    """
    CREATE INDEX IF NOT EXISTS slot_waiters_order_idx
        ON slot_waiters (namespace, ticket_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS conn_limits (
        namespace        text        PRIMARY KEY,
        max_connections  integer     NOT NULL,
        reserved_deletes integer     NOT NULL DEFAULT 0,
        version          integer     NOT NULL,
        updated_at       timestamptz NOT NULL DEFAULT now()
    )
    """,
    # One row per table, last writer wins under GREATEST. Replaces the
    # hints/gen-<bucket>/lsn-<20 digits> directory tree, its quantised bucket
    # walk-back, and its reaper: a monotonic upsert needs none of them.
    """
    CREATE TABLE IF NOT EXISTS backlog_hints (
        namespace   text        NOT NULL,
        table_name  text        NOT NULL,
        lsn         bigint      NOT NULL,
        updated_at  timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, table_name)
    )
    """,
    # Write-once immutable records (schema nodes, initialization, schemas,
    # trigger scopes, snapshot metadata). INSERT ... ON CONFLICT DO NOTHING is
    # the head election that _publish_candidate_into_exclusive_head emulated
    # with a candidate file plus an exclusive rename.
    """
    CREATE TABLE IF NOT EXISTS state_records (
        namespace   text        NOT NULL,
        record_key  text        NOT NULL,
        record      jsonb       NOT NULL,
        record_type text        NOT NULL DEFAULT 'generic',
        created_at  timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, record_key)
    )
    """,
    # Exact state reads use the primary key. GC and activity maintenance filter
    # by record type, so keep those scans inside one namespace and record class.
    """
    CREATE INDEX IF NOT EXISTS state_records_type_idx
        ON state_records (namespace, record_type)
    """,
    # Daily GC selects stale table-activity rows by the numeric JSON timestamp.
    # A partial expression index avoids scanning and casting every state record.
    """
    CREATE INDEX IF NOT EXISTS state_records_activity_age_idx
        ON state_records (
            namespace,
            ((record->>'last_used_at')::double precision)
        )
        WHERE record_type = 'table-activity'
    """,
)


def ensure_schema(connection: Any) -> None:
    """Create the state tables if they do not already exist.

    Serialized on a transaction-scoped advisory lock: ``CREATE TABLE IF NOT EXISTS``
    is *not* concurrency-safe on first creation -- when a Lakeflow pipeline starts one
    Python worker per flow and they all create a brand-new table at once (the first run
    after a table is added), the existence check passes for all of them and every worker
    but one then fails inserting the catalog row with ``duplicate key ... pg_type_typname
    _nsp_index``. The lock makes the losers wait, so they find the objects already present.
    It needs no connector schema and is released by the commit below, so it is safe during
    first provisioning; the key is distinct from the role-provisioning lock's.
    """

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("lakeflow-informix-ensure-schema",),
        )
        for statement in _SCHEMA_STATEMENTS:
            cursor.execute(statement)
    connection.commit()


def seed_slots(connection: Any, namespace: str, slot_count: int) -> None:
    """Ensure ``slot_count`` rows exist for ``namespace``.

    Capacity is the row count, so seeding *is* configuring the semaphore. Rows
    are only ever added: removing them while a holder is live would silently
    revoke a lease, so a reduced limit is enforced by the acquire query's
    ``slot_id`` bound instead.
    """

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO conn_slots (namespace, slot_id)
            SELECT %s, generated.slot_id
            FROM generate_series(0, %s - 1) AS generated(slot_id)
            ON CONFLICT (namespace, slot_id) DO NOTHING
            """,
            (namespace, slot_count),
        )
    connection.commit()


class LakebaseState:
    """Connection factory and provisioning entry point for Lakebase state.

    One instance per reader process. Provisioning runs at most once per project
    per process behind a lock, so the 60-odd flows a pipeline starts converge on
    a single set of control-plane calls instead of each issuing their own.
    """

    _provisioned: dict[str, dict[str, Any]] = {}
    # Held in a dict and built on first use rather than at class creation. Spark
    # cloudpickles the reader class, and the merged single-file deployment puts this
    # class in the same namespace the reader closes over, so a lock alive at import
    # would travel with it and make serialization fail outright ("cannot pickle
    # '_thread.lock' object"). Mutating a dict also avoids rebinding a name, which
    # the merged form -- where module globals become function locals -- cannot do.
    _provision_locks: dict[str, threading.Lock] = {}
    _project_gc_checked: dict[str, float] = {}

    @classmethod
    def _lock(cls) -> threading.Lock:
        lock = cls._provision_locks.get("provision")
        if lock is None:
            # setdefault resolves the race: racing callers may each build a lock,
            # but all of them return whichever one landed first.
            lock = cls._provision_locks.setdefault("provision", threading.Lock())
        return lock

    def __init__(self, options: dict[str, str], connection_identity: str) -> None:
        self._options = options
        self._project_id = options.get(
            "lakebase.project.id", ""
        ).strip() or project_id_for_connection(connection_identity)
        self._min_cu = float(options.get("lakebase.autoscale.min.cu", _DEFAULT_MIN_CU))
        self._max_cu = float(options.get("lakebase.autoscale.max.cu", _DEFAULT_MAX_CU))
        self._suspend_seconds = int(
            options.get("lakebase.suspend.timeout.seconds", _DEFAULT_SUSPEND_SECONDS)
        )
        self._provision_timeout_seconds = float(
            options.get("lakebase.provision.timeout.seconds", _DEFAULT_PROVISION_TIMEOUT_SECONDS)
        )
        if (
            not math.isfinite(self._provision_timeout_seconds)
            or self._provision_timeout_seconds <= 0
        ):
            raise ValueError("Option 'lakebase.provision.timeout.seconds' must be > 0")
        self._database = options.get("lakebase.database", _DEFAULT_DATABASE)
        # Two distinct identities, deliberately separate.
        #
        # The *state* identity is long-lived and does all ordinary state access, so
        # nothing in the steady-state path can expire mid-stream. Its role name is
        # fixed; only its password is configured. The *admin* identity is used only
        # to provision that role -- create it or reset its password -- and needs the
        # elevated rights to do so. Keeping them apart means the credential
        # distributed to every reader process is not one that can create roles.
        self._user_id = STATE_ROLE
        self._password = options.get("lakebase.password", "").strip()
        self._admin_password = options.get("lakebase.admin_password", "").strip()
        self._api: _WorkspaceApi | None = None
        self._facts: dict[str, Any] | None = None
        self._token: str | None = None
        self._token_minted_at = 0.0

    @property
    def project_id(self) -> str:
        return self._project_id

    def _api_client(self) -> _WorkspaceApi:
        if self._api is None:
            host, token = _workspace_credentials(self._options)
            self._api = _WorkspaceApi(host, token)
        return self._api

    def provision(self) -> dict[str, Any]:
        """Idempotently ensure the project, endpoint, and schema all exist."""

        cached = LakebaseState._provisioned.get(self._project_id)
        if cached is not None:
            self._facts = cached
            return cached
        deadline = time.monotonic() + self._provision_timeout_seconds
        lock = LakebaseState._lock()
        if not lock.acquire(timeout=self._provision_timeout_seconds):
            raise LakebaseStateError(
                f"Lakebase provisioning timed out after {self._provision_timeout_seconds:g}s "
                f"waiting for the provisioning lock for project {self._project_id}"
            )
        try:
            cached = LakebaseState._provisioned.get(self._project_id)
            if cached is not None:
                self._facts = cached
                return cached
            api = self._api_client()
            facts = ensure_project(
                api,
                self._project_id,
                min_cu=self._min_cu,
                max_cu=self._max_cu,
                suspend_seconds=self._suspend_seconds,
                deadline=deadline,
            )
            self._facts = facts
            self._ensure_password_login(facts)
            connection = self.connect()
            try:
                ensure_schema(connection)
            finally:
                connection.close()
            LakebaseState._provisioned[self._project_id] = facts
            return facts
        except LakebaseStateError as error:
            raise LakebaseStateError(
                f"Lakebase provisioning failed for project {self._project_id}: {error}"
            ) from error
        finally:
            lock.release()

    def _password_login_works(self, facts: dict[str, Any]) -> bool:
        """Return whether the configured user_id/password can actually log in."""

        try:
            connection = self._open(facts, self._user_id, self._password)
        except Exception:
            # Postgres reports an absent role and a wrong password identically
            # ("password authentication failed for user ..."), with no SQLSTATE
            # to separate them, so this deliberately does not try to classify the
            # failure. Which repair is needed is decided by querying pg_roles.
            _LOGGER.debug("Lakebase password login failed for %s", self._user_id, exc_info=True)
            return False
        connection.close()
        return True

    def _ensure_password_login(self, facts: dict[str, Any]) -> None:
        """Validate the state role's login, provisioning it if it fails."""

        if not self._password:
            raise LakebaseStateError(
                "Lakebase state needs a password for its state role; set lakebase.password"
            )
        if self._password_login_works(facts):
            _LOGGER.info(
                "Lakebase role %s authenticated with the configured password", self._user_id
            )
            return
        # Fall back to the admin identity, the only one able to create a role or
        # reset its password.
        admin = self._connect_as_admin(facts)
        try:
            action = ensure_login_role(admin, self._database, self._user_id, self._password)
        finally:
            admin.close()
        if not self._password_login_works(facts):
            raise LakebaseStateError(
                f"Lakebase role {self._user_id} still cannot log in after being {action}; "
                "check lakebase.password and that the project has native Postgres "
                "login enabled"
            )
        _LOGGER.info("Lakebase role %s %s and validated", self._user_id, action)

    def _credential(self) -> str:
        now = time.monotonic()
        if self._token is None or now - self._token_minted_at > _CREDENTIAL_REFRESH_SECONDS:
            facts = self._facts or self.provision()
            self._token = generate_credential(self._api_client(), facts["endpoint"])
            self._token_minted_at = now
        return self._token

    def connect(self) -> Any:
        """Open a Postgres connection, waking a suspended endpoint if needed.

        ``psycopg2`` is preinstalled on the Databricks runtime; ``psycopg`` (v3)
        is accepted as an alternative so the module also works where only that
        is present.
        """

        facts = self._facts or self.provision()
        # Always the state role: its password needs no control-plane call and
        # cannot expire mid-stream. The admin identity is reached only through
        # ``_connect_as_admin``, during provisioning.
        return self._open(facts, self._user_id, self._password)

    def collect_unused_projects(self, retention_days: int, interval_hours: float) -> int:
        """Delete inactive, auto-named connector projects once per scan interval."""

        if self._options.get("lakebase.project.id", "").strip():
            return 0
        now = time.monotonic()
        cache_key = self._options.get("lakebase.workspace.host", "workspace")
        if now - self._project_gc_checked.get(cache_key, float("-inf")) < interval_hours * 3600:
            return 0
        self._project_gc_checked[cache_key] = now
        api = self._api_client()
        response = api.request("GET", "/api/2.0/postgres/projects") or {}
        projects = response if isinstance(response, list) else response.get("projects", [])
        deleted = 0
        for project in projects:
            name = str((project or {}).get("name") or (project or {}).get("id") or "")
            project_id = name.rsplit("/", 1)[-1]
            if not project_id.startswith(_AUTO_PROJECT_PREFIX) or project_id == self._project_id:
                continue
            connection = None
            try:
                facts = _endpoint_facts(api, project_id)
                token = generate_credential(api, facts["endpoint"])
                connection = self._open(facts, self._admin_user(), token)
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT
                          COUNT(*) FILTER (WHERE record_type = 'state-gc'),
                          COUNT(*) FILTER (WHERE record_type = 'table-activity'),
                          MAX((record->>'last_used_at')::double precision)
                            FILTER (WHERE record_type = 'table-activity')
                        FROM state_records
                        """
                    )
                    gc_rows, activity_rows, last_used = cursor.fetchone()
                if not gc_rows or not activity_rows or last_used is None:
                    continue
                if float(last_used) >= time.time() - retention_days * 86400:
                    continue
            except Exception:
                _LOGGER.warning(
                    "Skipping Lakebase project GC candidate %s", project_id, exc_info=True
                )
                continue
            finally:
                if connection is not None:
                    connection.close()
            operation = api.request("DELETE", f"/api/2.0/postgres/projects/{project_id}")
            api.await_operation(operation)
            LakebaseState._provisioned.pop(project_id, None)
            deleted += 1
        return deleted

    def _admin_user(self) -> str:
        """Return the role name used for provisioning.

        ``_default_user`` already honours ``lakebase.admin_user`` first, so this
        does not re-check it: two copies of the same precedence rule can disagree.
        """

        return _default_user(self._options)

    def _connect_as_admin(self, facts: dict[str, Any]) -> Any:
        """Connect as the provisioning identity.

        Prefers ``lakebase.admin_password`` when set, because a password works
        even where a control-plane credential cannot be minted. Otherwise mints a
        short-lived OAuth credential for the workspace identity, which is what
        makes the zero-configuration case work.
        """

        if self._admin_password:
            return self._open(facts, self._admin_user(), self._admin_password)
        return self._open(facts, self._admin_user(), self._credential())

    def _open(self, facts: dict[str, Any], user: str, password: str) -> Any:
        """Connect as ``user``, waking a suspended endpoint if needed."""

        parameters = {
            "host": facts["host"],
            "dbname": self._database,
            "user": user,
            "password": password,
            "sslmode": "require",
            "connect_timeout": _CONNECT_TIMEOUT_SECONDS,
            **_NO_CLIENT_CERTIFICATE,
        }
        try:
            import psycopg2  # noqa: PLC0415
        except ImportError:
            pass
        else:
            return psycopg2.connect(**parameters)
        try:
            import psycopg  # noqa: PLC0415
        except ImportError as error:  # pragma: no cover - depends on runtime
            raise LakebaseStateError(
                "Lakebase state needs psycopg2 or psycopg installed on the runtime"
            ) from error
        return psycopg.connect(**parameters)


# --- connection slots ------------------------------------------------------
#
# The whole Volume fencing protocol -- per-slot reservation directories,
# owner-/pulse-/released-/mutating- markers, a three-stage rename fence, and
# retired-/revoked- quarantine -- existed to emulate one atomic compare-and-swap.
# These four statements are that CAS, so none of it is needed here.
#
# Acquisition takes the lowest eligible slot whose lease is free or expired.
# ``FOR UPDATE SKIP LOCKED`` is what makes it safe under contention: a row a
# concurrent acquirer is already mutating is skipped rather than waited on, so N
# racers claim N distinct slots in one round trip each and none can observe a
# half-applied claim. ``epoch`` increments on every claim and becomes the fencing
# token: a holder that stalls past its lease and wakes up later cannot renew or
# release, because its epoch no longer matches.
_ACQUIRE_SLOT = """
UPDATE conn_slots SET owner = %(owner)s, epoch = epoch + 1,
                      scope = %(scope)s, renewed_at = now()
WHERE (namespace, slot_id) = (
    SELECT namespace, slot_id FROM conn_slots
    WHERE namespace = %(namespace)s
      AND slot_id >= %(floor)s
      AND slot_id < %(ceiling)s
      AND (owner IS NULL OR renewed_at < now() - make_interval(secs => %(lease)s))
    ORDER BY slot_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING slot_id, epoch
"""

# Renewal is guarded on (owner, epoch): a zombie's heartbeat is rejected rather
# than silently extending a lease its successor now owns. A caller that gets no
# row has provably lost the slot and must stop using its connection.
_HEARTBEAT_SLOT = """
UPDATE conn_slots SET renewed_at = now()
WHERE namespace = %(namespace)s AND slot_id = %(slot_id)s
  AND owner = %(owner)s AND epoch = %(epoch)s
RETURNING slot_id
"""

# Release carries the same guard, for a sharper reason: without it a zombie's
# late release would free a slot its successor legitimately holds, over-issuing
# capacity. On the Volume this hazard is what the mutating- marker guarded.
_RELEASE_SLOT = """
UPDATE conn_slots SET owner = NULL, scope = NULL, renewed_at = NULL
WHERE namespace = %(namespace)s AND slot_id = %(slot_id)s
  AND owner = %(owner)s AND epoch = %(epoch)s
RETURNING slot_id
"""

# Fair acquisition: the same claim as _ACQUIRE_SLOT, but a slot is eligible only when
# no *older, still-heartbeating* waiter is also eligible for it. That makes acquisition
# first-come-first-served per slot -- the oldest waiter in a band always has priority --
# so no waiter is lapped indefinitely under contention. Ordering is per slot rather than
# global: the band predicate (w.floor <= slot < w.ceiling) means an older delete-channel
# waiter reserved to high slots does not block a younger consumer from a low slot it could
# never use, so there is no cross-band head-of-line blocking. The CAS (SKIP LOCKED + epoch)
# is unchanged, so capacity is still exact: a rare ordering inversion under a read race
# costs at most one extra sweep, never an over-issued slot.
_ACQUIRE_SLOT_FAIR = """
UPDATE conn_slots SET owner = %(owner)s, epoch = epoch + 1,
                      scope = %(scope)s, renewed_at = now()
WHERE (namespace, slot_id) = (
    SELECT s.namespace, s.slot_id FROM conn_slots s
    WHERE s.namespace = %(namespace)s
      AND s.slot_id >= %(floor)s
      AND s.slot_id < %(ceiling)s
      AND (s.owner IS NULL OR s.renewed_at < now() - make_interval(secs => %(lease)s))
      AND NOT EXISTS (
          SELECT 1 FROM slot_waiters w
          WHERE w.namespace = %(namespace)s
            AND w.ticket_id < %(ticket_id)s
            AND w.floor <= s.slot_id AND s.slot_id < w.ceiling
            AND w.heartbeat_at >= now() - make_interval(secs => %(waiter_ttl)s)
      )
    ORDER BY s.slot_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING slot_id, epoch
"""

# A waiter enqueues one ticket for the whole time it blocks, heartbeats it each sweep,
# and deletes it the instant it wins a slot (or gives up). The heartbeat doubles as the
# reaper -- it drops any ticket whose owner stopped heartbeating -- so a crashed or
# timed-out waiter cannot wedge the queue and the table needs no separate GC.
_ENQUEUE_WAITER = """
INSERT INTO slot_waiters (namespace, owner, floor, ceiling)
VALUES (%(namespace)s, %(owner)s, %(floor)s, %(ceiling)s)
RETURNING ticket_id
"""
_HEARTBEAT_WAITER = """
UPDATE slot_waiters SET heartbeat_at = now()
WHERE namespace = %(namespace)s AND ticket_id = %(ticket_id)s
RETURNING ticket_id
"""
_REAP_WAITERS = """
DELETE FROM slot_waiters
WHERE namespace = %(namespace)s
  AND heartbeat_at < now() - make_interval(secs => %(waiter_ttl)s)
"""
_DEQUEUE_WAITER = """
DELETE FROM slot_waiters WHERE namespace = %(namespace)s AND ticket_id = %(ticket_id)s
"""


class ConnectionSlot:
    """A held connection-capacity lease.

    ``epoch`` is the fencing token; it is required to renew or release, so a
    lease that expired while this process was stalled cannot be resurrected.
    """

    __slots__ = ("namespace", "slot_id", "epoch", "owner")

    def __init__(self, namespace: str, slot_id: int, epoch: int, owner: str) -> None:
        self.namespace = namespace
        self.slot_id = slot_id
        self.epoch = epoch
        self.owner = owner

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"ConnectionSlot(slot-{self.slot_id:04d}, epoch={self.epoch})"


def acquire_slot(
    connection: Any,
    namespace: str,
    owner: str,
    *,
    slot_count: int,
    floor: int = 0,
    scope: str | None = None,
    lease_seconds: float = 120.0,
    ticket_id: int | None = None,
    waiter_ttl: float = _DEFAULT_WAITER_TTL_SECONDS,
) -> ConnectionSlot | None:
    """Claim one slot, or return ``None`` when every eligible slot is busy.

    ``floor`` implements the upsert reservation: the delete channel passes the
    reserved count so it can never claim a low slot, which is what makes the
    reservation a guarantee rather than a preference. ``slot_count`` bounds the
    ceiling, so lowering the configured limit takes effect immediately even
    though surplus rows are deliberately left in place.

    ``ticket_id`` opts into the fair queue: pass the id returned by
    ``enqueue_waiter`` (with the same ``waiter_ttl`` used to heartbeat it) and the
    claim yields any slot an older, still-heartbeating waiter is also eligible for,
    so this caller waits its turn. Omit it (the default) for the raw first-come race.
    """

    parameters = {
        "owner": owner,
        "scope": scope,
        "namespace": namespace,
        "floor": max(0, floor),
        "ceiling": slot_count,
        "lease": float(lease_seconds),
    }
    if ticket_id is None:
        statement = _ACQUIRE_SLOT
    else:
        statement = _ACQUIRE_SLOT_FAIR
        parameters["ticket_id"] = int(ticket_id)
        parameters["waiter_ttl"] = float(waiter_ttl)
    with connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        row = cursor.fetchone()
    connection.commit()
    if row is None:
        return None
    return ConnectionSlot(namespace, int(row[0]), int(row[1]), owner)


def enqueue_waiter(connection: Any, namespace: str, owner: str, *, floor: int, ceiling: int) -> int:
    """Register this caller as a slot waiter and return its queue ticket.

    The ticket records the eligible band [floor, ceiling); acquisition consults it
    so an older waiter in the same band is served first. Commit it before waiting so
    other waiters' connections can see it.
    """

    with connection.cursor() as cursor:
        cursor.execute(
            _ENQUEUE_WAITER,
            {
                "namespace": namespace,
                "owner": owner,
                "floor": max(0, floor),
                "ceiling": int(ceiling),
            },
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def heartbeat_waiter(connection: Any, namespace: str, ticket_id: int, *, waiter_ttl: float) -> None:
    """Renew this waiter's ticket and reap any whose owner stopped heartbeating.

    Folding the reap into the heartbeat keeps the queue self-cleaning: a crashed or
    timed-out waiter's ticket ages out and is dropped by the next live heartbeat, so
    it never blocks the queue and no separate GC pass is needed.
    """

    with connection.cursor() as cursor:
        cursor.execute(
            _HEARTBEAT_WAITER,
            {"namespace": namespace, "ticket_id": int(ticket_id)},
        )
        cursor.execute(
            _REAP_WAITERS,
            {"namespace": namespace, "waiter_ttl": float(waiter_ttl)},
        )
    connection.commit()


def dequeue_waiter(connection: Any, namespace: str, ticket_id: int) -> None:
    """Remove this waiter's ticket -- on winning a slot or on giving up."""

    with connection.cursor() as cursor:
        cursor.execute(
            _DEQUEUE_WAITER,
            {"namespace": namespace, "ticket_id": int(ticket_id)},
        )
    connection.commit()


def heartbeat_slot(connection: Any, slot: ConnectionSlot) -> bool:
    """Renew a lease. ``False`` means the lease was lost and must not be reused."""

    with connection.cursor() as cursor:
        cursor.execute(
            _HEARTBEAT_SLOT,
            {
                "namespace": slot.namespace,
                "slot_id": slot.slot_id,
                "owner": slot.owner,
                "epoch": slot.epoch,
            },
        )
        renewed = cursor.fetchone() is not None
    connection.commit()
    return renewed


def release_slot(connection: Any, slot: ConnectionSlot) -> bool:
    """Free a lease. ``False`` means it had already been reclaimed."""

    with connection.cursor() as cursor:
        cursor.execute(
            _RELEASE_SLOT,
            {
                "namespace": slot.namespace,
                "slot_id": slot.slot_id,
                "owner": slot.owner,
                "epoch": slot.epoch,
            },
        )
        released = cursor.fetchone() is not None
    connection.commit()
    return released


# A monotonic "the pool is recycling slots" token. ``epoch`` increments on every claim
# (see ``_ACQUIRE_SLOT``: ``epoch = epoch + 1``), so the sum over a namespace strictly
# increases each time any slot is acquired -- and a freed slot is only re-acquired when
# a waiter wins it, so a rising sum means work is completing and capacity is turning
# over. A caller that cannot win a slot can poll this between sweeps to tell "the pool
# is busy (turning over) -- keep waiting" from "the pool is wedged (frozen) -- give up",
# without needing to see which slot moved. A slot merely held (a long read that neither
# releases nor re-claims) does not advance it: that is deliberate, the token measures
# forward progress of the pool, not liveness of an individual holder.
_POOL_PROGRESS_TOKEN = """
SELECT COALESCE(SUM(epoch), 0) FROM conn_slots WHERE namespace = %(namespace)s
"""


def pool_progress_token(connection: Any, namespace: str) -> int:
    """Return the pool's turnover token (see ``_POOL_PROGRESS_TOKEN``)."""

    with connection.cursor() as cursor:
        cursor.execute(_POOL_PROGRESS_TOKEN, {"namespace": namespace})
        row = cursor.fetchone()
    # Commit so the next poll sees a fresh snapshot rather than this transaction's.
    connection.commit()
    return int(row[0]) if row and row[0] is not None else 0


# --- backlog hints ---------------------------------------------------------
#
# One row per table, and ``GREATEST`` makes the write monotonic. The Volume
# version needed a quantised bucket directory per 15s, a walk-back over recent
# buckets on read, and a reaper for old generations -- all to approximate
# last-writer-wins on a filesystem with no atomic update. A single upsert is
# exact, and one query returns every table's hint instead of walking a directory
# per table.
_WRITE_HINT = """
INSERT INTO backlog_hints (namespace, table_name, lsn, updated_at)
VALUES (%(namespace)s, %(table_name)s, %(lsn)s, now())
ON CONFLICT (namespace, table_name) DO UPDATE
   SET lsn = GREATEST(backlog_hints.lsn, EXCLUDED.lsn), updated_at = now()
"""

_READ_HINTS = """
SELECT table_name, lsn FROM backlog_hints
WHERE namespace = %(namespace)s
  AND updated_at > now() - make_interval(secs => %(max_age)s)
"""


def publish_backlog_hint(connection: Any, namespace: str, table_name: str, lsn: int) -> None:
    """Record a table's log position, never regressing an existing value."""

    with connection.cursor() as cursor:
        cursor.execute(
            _WRITE_HINT, {"namespace": namespace, "table_name": table_name, "lsn": int(lsn)}
        )
    connection.commit()


def read_backlog_hints(
    connection: Any, namespace: str, *, max_age_seconds: float = 45.0
) -> dict[str, int]:
    """Return every fresh hint for a namespace in one round trip.

    Stale rows are filtered by age rather than deleted: a hint is advisory, and
    leaving it costs one row per table instead of a reaper.
    """

    with connection.cursor() as cursor:
        cursor.execute(_READ_HINTS, {"namespace": namespace, "max_age": float(max_age_seconds)})
        rows = cursor.fetchall()
    # End the read-only transaction so the connection is never left idle-in-transaction.
    connection.rollback()
    return {str(name): int(lsn) for name, lsn in rows}


# --- connection limit configuration ---------------------------------------

_WRITE_LIMIT = """
INSERT INTO conn_limits (namespace, max_connections, reserved_deletes, version, updated_at)
VALUES (%(namespace)s, %(max_connections)s, %(reserved)s, %(version)s, now())
ON CONFLICT (namespace) DO UPDATE
   SET max_connections = EXCLUDED.max_connections,
       reserved_deletes = EXCLUDED.reserved_deletes,
       version = EXCLUDED.version,
       updated_at = now()
"""


def publish_connection_limit(
    connection: Any,
    namespace: str,
    max_connections: int,
    reserved_deletes: int,
    version: int,
) -> None:
    """Publish the agreed capacity configuration for a namespace."""

    with connection.cursor() as cursor:
        cursor.execute(
            _WRITE_LIMIT,
            {
                "namespace": namespace,
                "max_connections": int(max_connections),
                "reserved": int(reserved_deletes),
                "version": int(version),
            },
        )
    connection.commit()


def read_connection_limit(connection: Any, namespace: str) -> dict[str, int] | None:
    """Return the published capacity configuration, or ``None`` if unset."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT max_connections, reserved_deletes, version
            FROM conn_limits WHERE namespace = %(namespace)s
            """,
            {"namespace": namespace},
        )
        row = cursor.fetchone()
    # End the read-only transaction so the connection is never left idle-in-transaction.
    connection.rollback()
    if row is None:
        return None
    return {
        "max_connections": int(row[0]),
        "reserved_deletes": int(row[1]),
        "version": int(row[2]),
    }


# --- immutable state records ----------------------------------------------
#
# ``INSERT ... ON CONFLICT DO NOTHING RETURNING`` is the head election. Exactly
# one writer gets a row back; every other writer gets nothing and re-reads the
# winner. That single statement replaces writing a candidate file, fsyncing it,
# and renaming it into an exclusive ``head`` -- along with the candidate sweeper
# that had to clean up whichever candidates lost.
_INSERT_RECORD = """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (%(namespace)s, %(record_key)s, %(record)s::jsonb, %(record_type)s)
ON CONFLICT (namespace, record_key) DO NOTHING
RETURNING record
"""

_SELECT_RECORD = """
SELECT record FROM state_records
WHERE namespace = %(namespace)s AND record_key = %(record_key)s
"""

_DELETE_OBSOLETE_SCOPED_RECORDS = """
DELETE FROM state_records
WHERE namespace = %(namespace)s
  AND record_key LIKE %(record_key_pattern)s
  AND record->>'scope' LIKE %(pipeline_scope_pattern)s
  AND record->>'scope' <> %(current_scope)s
  -- Cast the nullable parameter to text so Postgres can determine its type:
  -- a NULL retained_scope is sent with an unknown type OID, and `$n IS NULL`
  -- alone gives the planner nothing to infer from (AmbiguousParameter otherwise).
  AND (%(retained_scope)s::text IS NULL OR record->>'scope' <> %(retained_scope)s::text)
RETURNING record_key
"""

_TOUCH_TABLE_ACTIVITY = """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (
  %(namespace)s, %(record_key)s,
  jsonb_build_object(
    -- Cast the string parameters: jsonb_build_object value arguments are typed
    -- "any", so an uncast parameter has no determinable type when the statement
    -- is prepared (IndeterminateDatatype: could not determine data type of $n).
    'format_version', 1, 'pipeline_id', %(pipeline_id)s::text,
    'table_prefix', %(table_prefix)s::text,
    'last_used_at', extract(epoch FROM clock_timestamp())
  ),
  'table-activity'
)
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = EXCLUDED.record
WHERE state_records.record_type = 'table-activity'
  AND (state_records.record->>'last_used_at')::double precision
      <= extract(epoch FROM clock_timestamp()) - %(touch_interval_seconds)s
RETURNING record
"""

_CLAIM_STATE_GC = """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (
  %(namespace)s, %(record_key)s,
  jsonb_build_object(
    'format_version', 1,
    'last_run_at', extract(epoch FROM clock_timestamp()),
    'next_run_at', extract(epoch FROM clock_timestamp()) + %(gc_interval_seconds)s
  ),
  'state-gc'
)
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = EXCLUDED.record
WHERE state_records.record_type = 'state-gc'
  AND (state_records.record->>'next_run_at')::double precision
      <= extract(epoch FROM clock_timestamp())
RETURNING record
"""

_DELETE_STALE_PIPELINE_STATE = """
DELETE FROM state_records AS target
USING state_records AS activity
WHERE target.namespace = %(namespace)s
  AND activity.namespace = target.namespace
  AND activity.record_type = 'table-activity'
  AND (activity.record->>'last_used_at')::double precision < %(cutoff_epoch)s
  AND target.record_key LIKE activity.record->>'table_prefix' || '/%%'
  AND target.record->>'scope' LIKE activity.record->>'pipeline_id' || '_@_%%'
RETURNING target.record_key
"""

_DELETE_UNUSED_TABLE_STATE = """
DELETE FROM state_records AS target
WHERE target.namespace = %(namespace)s
  AND target.record_type NOT IN ('table-activity', 'state-gc')
  AND EXISTS (
    SELECT 1 FROM state_records AS stale
    WHERE stale.namespace = target.namespace
      AND stale.record_type = 'table-activity'
      AND (stale.record->>'last_used_at')::double precision < %(cutoff_epoch)s
      AND target.record_key LIKE stale.record->>'table_prefix' || '/%%'
  )
  AND NOT EXISTS (
    SELECT 1 FROM state_records AS active
    WHERE active.namespace = target.namespace
      AND active.record_type = 'table-activity'
      AND (active.record->>'last_used_at')::double precision >= %(cutoff_epoch)s
      AND target.record_key LIKE active.record->>'table_prefix' || '/%%'
  )
RETURNING target.record_key
"""

_DELETE_STALE_ACTIVITY = """
DELETE FROM state_records
WHERE namespace = %(namespace)s
  AND record_type = 'table-activity'
  AND (record->>'last_used_at')::double precision < %(cutoff_epoch)s
RETURNING record_key
"""

_PUBLISH_MINIMUM_LSN_RECORD = """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (%(namespace)s, %(record_key)s, %(record)s::jsonb, %(record_type)s)
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = EXCLUDED.record
WHERE state_records.record_type = EXCLUDED.record_type
  AND state_records.record->>'format_version' = EXCLUDED.record->>'format_version'
  AND state_records.record->>'scope' = EXCLUDED.record->>'scope'
  AND state_records.record->>'table' = EXCLUDED.record->>'table'
  AND (state_records.record->>'start_lsn')::numeric
      > (EXCLUDED.record->>'start_lsn')::numeric
RETURNING record
"""


def publish_state_record(
    connection: Any,
    namespace: str,
    record_key: str,
    record: dict[str, Any],
    *,
    record_type: str = "generic",
) -> dict[str, Any]:
    """Elect exactly one record for a key and return the elected value.

    Always returns the *winning* record, which may be another writer's. Callers
    depend on that: a record is the shared truth for a key, so a loser must adopt
    the winner rather than assume its own value took effect.
    """

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


def publish_minimum_lsn_state_record(
    connection: Any,
    namespace: str,
    record_key: str,
    record: dict[str, Any],
    *,
    record_type: str,
) -> dict[str, Any]:
    """Atomically publish the lowest compatible ``start_lsn`` for one key.

    A later CDC poll can discover an older still-open transaction, moving its
    effective restart position backwards. PostgreSQL serializes the conflict
    update so concurrent upsert workers converge on the minimum. Incompatible
    identity metadata prevents the update; schema metadata travels with the
    winning minimum because schema transitions can legitimately occur later in
    the same update.
    """

    payload = json.dumps(record, sort_keys=True)
    parameters = {
        "namespace": namespace,
        "record_key": record_key,
        "record": payload,
        "record_type": record_type,
    }
    with connection.cursor() as cursor:
        cursor.execute(_PUBLISH_MINIMUM_LSN_RECORD, parameters)
        row = cursor.fetchone()
        if row is None:
            cursor.execute(_SELECT_RECORD, parameters)
            row = cursor.fetchone()
    connection.commit()
    if row is None:
        raise LakebaseStateError(
            f"state record {namespace}/{record_key} vanished during minimum-LSN publication"
        )
    return _as_dict(row[0])


def read_state_record(connection: Any, namespace: str, record_key: str) -> dict[str, Any] | None:
    """Return an elected record, or ``None`` when the key is unwritten."""

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
    """Delete old update scopes while preserving the scope in Spark's checkpoint."""

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


def touch_table_activity(
    connection: Any,
    namespace: str,
    record_key: str,
    table_prefix: str,
    pipeline_id: str,
    touch_interval_seconds: float,
) -> bool:
    """Refresh one table/pipeline activity record when its write interval elapsed."""

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
    """Run at most one database-scoped table-state collection per interval."""

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


def _as_dict(value: Any) -> dict[str, Any]:
    """Normalise a jsonb column, which drivers return as dict or as text."""

    if isinstance(value, dict):
        return value
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise LakebaseStateError("state record is not a JSON object")
    return decoded


def _default_user(options: dict[str, str]) -> str:
    """Resolve the provisioning role name, which is the caller's own identity.

    The last two resolutions exist because the process that needs this name has
    no ambient identity: ``WorkspaceClient()`` cannot be constructed there, so
    asking the SDK who we are is not an option. Instead the driver's captured
    credential is used to *ask the control plane*, which works from anywhere,
    with the SDK kept only for the non-pipeline case.
    """

    explicit = options.get("lakebase.admin_user", "").strip()
    if explicit:
        return explicit
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    if client_id:
        return client_id
    identity = _current_user_from_captured_credential(options)
    if identity:
        return identity
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        return WorkspaceClient().current_user.me().user_name
    except Exception as error:  # pragma: no cover - depends on runtime
        raise LakebaseStateError(
            "Cannot determine the Lakebase provisioning role; set lakebase.admin_user"
        ) from error


def _current_user_from_captured_credential(options: dict[str, str]) -> str:
    """Return the calling identity's user name, or "" if it cannot be resolved.

    A service principal has no ``userName``, so ``id`` is accepted as the
    fallback: for one, that is the client id the Postgres role is named after.
    """

    try:
        host, token = _workspace_credentials(options)
        response = _WorkspaceApi(host, token).request(
            "GET", "/api/2.0/preview/scim/v2/Me", tolerate_missing=True
        )
    except Exception:
        _LOGGER.debug("Could not resolve the calling identity from the workspace", exc_info=True)
        return ""
    body = response or {}
    return str(body.get("userName") or body.get("id") or "").strip()
