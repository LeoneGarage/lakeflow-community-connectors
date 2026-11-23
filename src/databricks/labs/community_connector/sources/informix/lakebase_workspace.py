"""Workspace API calls the connector's state needs: Lakebase provisioning, login roles, the Files
API and snapshot-staging tokens.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# What a request that may succeed when sent again raises. urllib wraps an OSError raised
# while it sends the request in URLError, but not one raised while it reads the answer: a
# connection reset or a TLS error mid-body, or http.client's RemoteDisconnected and
# IncompleteRead, escaped the retries and failed the read at once.
_HTTP_TRANSIENT_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    ConnectionError,
    ssl.SSLError,
    http.client.HTTPException,
)
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
# Between looks at a deleted project's tombstone that another process is purging.
_PURGE_POLL_SECONDS = 2.0

_API_TIMEOUT_SECONDS = 120.0
_API_ATTEMPTS = 4
_DEFAULT_PROVISION_TIMEOUT_SECONDS = 180.0

# --- Snapshot-page staging over the Files REST API -------------------------------
# The connector can stage snapshot page payloads through the Files API instead of the
# UC Volume FUSE mount (see the connector's snapshot.staging.transport option), which
# sidesteps the FUSE-mount disconnects that recur under sustained write frequency.
# Reaching the Files API needs a bearer token, but the reader processes that stage
# pages have no ambient identity -- only the driver, at module-load, does. So the
# driver-captured credential is carried to each reader in _CAPTURED_WORKSPACE, and the
# reader mints its own personal access tokens from it (StagingCredentialProvider): one
# scoped to the File Management APIs, which the Files API calls send, and a minting token
# kept in the process, which mints their successors -- no ambient identity is needed after
# the first mint.
# Each token is tagged with the pipeline id and update id, so a mint can revoke the tokens of
# the pipeline's earlier updates while leaving those its sibling flow processes hold.
STAGING_TOKEN_COMMENT_PREFIX = "informix-snapshot-staging:"
# Four hours: a leaked token is usable no longer than that.
_DEFAULT_STAGING_TOKEN_LIFETIME_SECONDS = 14400
# Refresh at half-life: a token minted with lifetime L is replaced after L/2, so a
# missed or slow refresh still leaves L/2 of validity before anything can 401.
_STAGING_TOKEN_REFRESH_FRACTION = 0.5
_STAGING_TOKEN_MIN_LIFETIME_SECONDS = 600
_FILES_API_TIMEOUT_SECONDS = 300.0
_FILES_API_ATTEMPTS = 4


class LakebaseStateError(RuntimeError):
    # Docstring: Raised when Lakebase-backed state cannot be provisioned or reached.
    pass


class WorkspaceHttpError(LakebaseStateError):
    # Docstring: A workspace control-plane call returned a non-retriable HTTP status.
    #
    # Subclasses ``LakebaseStateError`` so existing ``except LakebaseStateError``
    # handlers are unaffected; it merely carries ``status_code`` so a caller can
    # branch on, for example, a 403 that means a feature is disabled.

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


def project_id_for_connection(identity: str, prefix: str = "informix-state") -> str:
    # Docstring: Derive a stable, DNS-compliant project id from a connection identity.
    #
    # The id must be reproducible: every worker in every update has to resolve the
    # same project without coordinating, and the project is the unit that ties
    # state to one Informix connection. Hashing the identity also avoids leaking a
    # hostname into a globally visible resource name.

    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{digest}"


def _https_host(host: str) -> str:
    # Docstring: ``host`` for a REST call that carries a bearer token: refused unless HTTPS.
    #
    # lakebase.workspace.host given as http://... sent the workspace token, and the staging
    # tokens minted with it, in clear text.

    if not host.lower().startswith("https://"):
        raise LakebaseStateError(
            f"Workspace host {host!r} must be an https:// URL: it is sent bearer tokens"
        )
    return host.rstrip("/")


class _WorkspaceApi:
    # Docstring: Minimal REST client for the workspace control plane.
    #
    # Deliberately stdlib-only. This module is imported inside Spark workers where
    # adding a dependency is a deployment problem, and the surface needed here is
    # four verbs against JSON endpoints.

    def __init__(self, host: str, token: str) -> None:
        self._host = _https_host(host)
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
                    # A bounded read: a proxy's error page can be any size.
                    detail = error.read(4096).decode("utf-8", "replace")[:400]
                    raise WorkspaceHttpError(
                        f"{method} {path} failed with HTTP {error.code}: {detail}",
                        error.code,
                    ) from error
                last_error = error
            except (*_HTTP_TRANSIENT_ERRORS, json.JSONDecodeError) as error:
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
        # Docstring: Resolve a long-running-operation envelope to its final response.
        #
        # Provisioning calls may return either a finished resource or an operation
        # that has to be polled; both shapes arrive on the same code path.

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
        # An operation that succeeded can name the field with no error in it.
        if operation.get("error"):
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
    # Docstring: Record the driver's ambient workspace credentials for later use.
    #
    # Call once while the pipeline module executes on the driver. Failure is not
    # fatal here: an explicitly configured host/token still works, and raising
    # would break registration for deployments that never touch Lakebase.

    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Only a Databricks runtime has an ambient identity worth capturing. This
        # keeps unit tests -- which import and register the module -- from trying
        # to authenticate at all.
        return _CAPTURED_WORKSPACE
    try:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415

        client = WorkspaceClient()
        header = client.config.authenticate() or {}
        # A bearer token only, as _workspace_credentials takes one: the value of a Basic
        # header was kept and later sent as a bearer token.
        scheme, _, value = str(header.get("Authorization", "")).partition(" ")
        token = value.strip() if scheme.lower() == "bearer" else ""
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


def pipeline_update(pipeline_id: str, update_id: str) -> dict[str, Any] | None:
    # Docstring: The update as the pipelines API describes it, or None when it cannot be asked.
    #
    # Called on the driver while the pipeline module executes, with the credentials
    # captured there. Spark gives an update no setting that says whether it is a full
    # refresh; the API's ``full_refresh``, ``full_refresh_selection`` and ``cause`` do.
    # Never fatal: a caller that learns nothing must assume nothing.

    captured = capture_workspace_credentials()
    host, token = captured.get("host"), captured.get("token")
    if not host or not token:
        return None
    try:
        answer = _WorkspaceApi(host, token).request(
            "GET",
            f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
            deadline=time.monotonic() + 30,
        )
    except Exception:
        _LOGGER.debug("Cannot read pipeline update %s", update_id, exc_info=True)
        return None
    update = (answer or {}).get("update")
    return update if isinstance(update, dict) else None


def pipeline_updates(pipeline_id: str, pages: int = 3) -> list[dict[str, Any]] | None:
    # Docstring: The pipeline's updates, newest first, up to ``pages`` pages of 100, as the
    # pipelines API lists them, or None when it cannot be asked. As pipeline_update: on the
    # driver, never fatal.

    captured = capture_workspace_credentials()
    host, token = captured.get("host"), captured.get("token")
    if not host or not token:
        return None
    updates: list[dict[str, Any]] = []
    page_token = None
    try:
        for _ in range(pages):
            query = "max_results=100" + (
                f"&page_token={urllib.parse.quote(str(page_token), safe='')}" if page_token else ""
            )
            answer = _WorkspaceApi(host, token).request(
                "GET",
                f"/api/2.0/pipelines/{pipeline_id}/updates?{query}",
                deadline=time.monotonic() + 30,
            )
            updates += [u for u in (answer or {}).get("updates") or () if isinstance(u, dict)]
            page_token = (answer or {}).get("next_page_token")
            if not page_token:
                break
    except Exception:
        _LOGGER.debug("Cannot list the updates of pipeline %s", pipeline_id, exc_info=True)
        return None
    return updates


def pipeline_events(pipeline_id: str, since: str, pages: int = 3) -> list[dict[str, Any]] | None:
    # Docstring: The pipeline's events from ``since`` (an ISO timestamp) on, oldest first, up
    # to ``pages`` pages of 100, or None when the API cannot be asked. As pipeline_update.

    captured = capture_workspace_credentials()
    host, token = captured.get("host"), captured.get("token")
    if not host or not token:
        return None
    first = {"max_results": "100", "order_by": "timestamp asc", "filter": f"timestamp >= '{since}'"}
    query = urllib.parse.urlencode(first)
    events: list[dict[str, Any]] = []
    try:
        for _ in range(pages):
            answer = _WorkspaceApi(host, token).request(
                "GET",
                f"/api/2.0/pipelines/{pipeline_id}/events?{query}",
                deadline=time.monotonic() + 30,
            )
            events += [e for e in (answer or {}).get("events") or () if isinstance(e, dict)]
            page_token = (answer or {}).get("next_page_token")
            if not page_token:
                break
            # A page token goes alone: the API refuses it beside the filter and the order.
            query = urllib.parse.urlencode({"max_results": "100", "page_token": page_token})
    except Exception:
        _LOGGER.debug("Cannot list the events of pipeline %s", pipeline_id, exc_info=True)
        return None
    return events


def _workspace_credentials(options: dict[str, str]) -> tuple[str, str]:
    # Docstring: Resolve the workspace host and bearer token for control-plane calls.
    #
    # Prefers explicit options, then the credential captured on the driver, then
    # the ambient identity. The captured value is what makes this work inside a
    # pipeline at all, because the calling process has no identity of its own.

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
    if host and not host.lower().startswith("http"):
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
        host = host or client.config.host or ""
        if not token:
            scheme, _, value = str(
                (client.config.authenticate() or {}).get("Authorization", "")
            ).partition(" ")
            token = value.strip() if scheme.lower() == "bearer" else ""
        if not host or not token:
            raise LakebaseStateError(
                "Lakebase state needs a workspace host and a bearer token; set "
                "lakebase.workspace.host/lakebase.workspace.token"
            )
    return host, token


def _purged_project(
    api: _WorkspaceApi, project_id: str, deadline: float | None
) -> dict[str, Any] | None:
    # Docstring: The project once its tombstone is purged: None, or one another process
    # made since. Raises when the tombstone outlasts the wait.

    limit = time.monotonic() + _OPERATION_TIMEOUT_SECONDS
    limit = limit if deadline is None else min(limit, deadline)
    deadline_kwargs = {"deadline": deadline} if deadline is not None else {}
    while True:
        existing = api.request(
            "GET",
            f"/api/2.0/postgres/projects/{project_id}",
            tolerate_missing=True,
            **deadline_kwargs,
        )
        if existing is None or not existing.get("delete_time"):
            return existing
        if time.monotonic() >= limit:
            raise LakebaseStateError(f"purged Lakebase project {project_id} is still visible")
        time.sleep(_PURGE_POLL_SECONDS)


def _endpoint_facts(
    api: _WorkspaceApi, project_id: str, *, deadline: float | None = None
) -> dict[str, Any]:
    # Docstring: Read the read-write endpoint's host and resource name.
    #
    # Waits for the endpoint to exist because project creation provisions it
    # asynchronously; an IDLE endpoint is ready to use, since connecting is what
    # wakes it.

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


def ensure_project(
    api: _WorkspaceApi,
    project_id: str,
    *,
    min_cu: float = _DEFAULT_MIN_CU,
    max_cu: float = _DEFAULT_MAX_CU,
    suspend_seconds: int = _DEFAULT_SUSPEND_SECONDS,
    deadline: float | None = None,
) -> dict[str, Any]:
    # Docstring: Create the project if absent and return the endpoint's connection facts.
    #
    # ``default_endpoint_settings`` is supplied at creation because that is the
    # only point at which ``suspend_timeout_duration`` reaches the auto-created
    # ``primary`` endpoint (see module docstring). An existing project is left
    # exactly as it is: adjusting a live endpoint's capacity is an operator
    # decision, not something a reader should do behind the operator's back.

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
        # Every flow's process provisions on its own, so others purge the same tombstone at
        # once: one that is gone already (404) or being purged (a conflict) is waited out,
        # and a project another process made since is taken as found.
        try:
            purged = api.request(
                "DELETE",
                f"/api/2.0/postgres/projects/{project_id}?purge=true",
                tolerate_missing=True,
                **deadline_kwargs,
            )
        except LakebaseStateError:
            purged = None
        if purged is not None:
            if deadline is None:
                api.await_operation(purged)
            else:
                api.await_operation(purged, deadline=deadline)
        existing = _purged_project(api, project_id, deadline)
        if existing is None:
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
        except LakebaseStateError:
            # A concurrent worker may have created it between our GET and POST.
            # Re-reading is the check that matters; only a still-absent project
            # (or a deleted one's tombstone) is a real failure.
            found = api.request(
                "GET",
                f"/api/2.0/postgres/projects/{project_id}",
                tolerate_missing=True,
                **deadline_kwargs,
            )
            if found is None or found.get("delete_time"):
                raise
        else:
            # The creation this worker started failing is its own error, not another
            # worker's creation: a failed project can still be visible, and taking it for
            # one left the endpoint wait to time out in place of the error.
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
    return _endpoint_facts(api, project_id, deadline=deadline)


def generate_credential(api: _WorkspaceApi, endpoint: str) -> str:
    # Docstring: Mint a short-lived Postgres OAuth token for ``endpoint``.
    #
    # Sends ``endpoint`` alone: the documented ``claims`` array is rejected by this
    # workspace's API (see module docstring), and the caller's own identity already
    # scopes the credential.

    response = api.request("POST", "/api/2.0/postgres/credentials", {"endpoint": endpoint})
    token = (response or {}).get("token")
    if not token:
        raise LakebaseStateError(f"no Postgres credential returned for {endpoint}")
    return token


class StagingTokenUnavailable(RuntimeError):
    # Docstring: Personal access token creation is disabled or forbidden for this identity.
    #
    # Raised so the connector can fall back to the FUSE staging transport instead
    # of failing the flow: the Files-API transport cannot authenticate without a
    # token, and many workspaces disable PAT creation by policy.
    pass


def staging_token_comment(pipeline_id: str, update_id: str = "") -> str:
    # Docstring: The comment a staging token carries: its pipeline, and its update when known.

    comment = f"{STAGING_TOKEN_COMMENT_PREFIX}{pipeline_id}"
    return f"{comment}:{update_id}" if update_id else comment


# The API scopes of a staging bearer, and of the minting token beside it, which only this
# process holds (see StagingCredentialProvider).
_STAGING_TOKEN_SCOPES = ("files",)
_STAGING_MINTER_SCOPES = ("authentication",)


def mint_staging_token(
    api: _WorkspaceApi,
    pipeline_id: str,
    lifetime_seconds: int,
    update_id: str = "",
    scopes: tuple[str, ...] = (),
) -> tuple[str, str, float | None]:
    # Docstring: Mint a personal access token for Files-API staging, tagged per pipeline update.
    #
    # Scoped to ``scopes``, the File Management APIs by default: leaked, such a token reads
    # and writes the files its identity may, and calls no other workspace API, a token API
    # included. A provider's minting token takes the token APIs' scope instead.
    #
    # Returns ``(token_value, token_id, granted_seconds)``: the lifetime the workspace
    # granted, from the token's creation and expiry times, which a maximum-lifetime
    # policy can make shorter than the one requested; ``None`` when it didn't say.
    # Raises :class:`StagingTokenUnavailable` when the workspace forbids token creation
    # (HTTP 403) or rejects it as unsupported (HTTP 400), so the caller can fall back
    # to FUSE rather than fail.

    comment = staging_token_comment(pipeline_id, update_id)
    try:
        response = (
            api.request(
                "POST",
                "/api/2.0/token/create",
                {
                    "comment": comment,
                    "lifetime_seconds": int(lifetime_seconds),
                    "scopes": list(scopes or _STAGING_TOKEN_SCOPES),
                },
            )
            or {}
        )
    except WorkspaceHttpError as error:
        if error.status_code in (400, 403):
            raise StagingTokenUnavailable(
                "workspace forbids personal access token creation; snapshot staging "
                "cannot use the Files API transport"
            ) from error
        raise
    token_value = str(response.get("token_value") or "")
    info = response.get("token_info") or {}
    token_id = str(info.get("token_id") or "")
    if not token_value:
        raise StagingTokenUnavailable("token creation returned no token value")
    created, expires = int(info.get("creation_time") or 0), int(info.get("expiry_time") or 0)
    granted = (expires - created) / 1000.0 if expires > created > 0 else None
    return token_value, token_id, granted


def revoke_stale_staging_tokens(
    api: _WorkspaceApi, pipeline_id: str, update_id: str, predecessor_id: str = ""
) -> None:
    # Docstring: Delete the pipeline's staging tokens from earlier updates, and the caller's own
    # predecessor.
    #
    # Every flow process of an update mints its own token under that update's comment
    # and stages with it, so those are left alone: revoking them failed the siblings'
    # next Files-API call with a 401. Without an update id the update's tokens can't be
    # told from earlier ones, so only the predecessor goes.
    #
    # Best-effort: listing or deleting may fail (e.g. the identity cannot manage
    # tokens it did not create), and an orphaned token expires on its own, so a
    # failure here is logged and swallowed rather than blocking startup.

    if not update_id:
        # A refresh, with only its predecessor to go, whose id it knows: no listing of
        # every token in the workspace to find it.
        if predecessor_id:
            try:
                api.request("POST", "/api/2.0/token/delete", {"token_id": predecessor_id})
            except LakebaseStateError:
                _LOGGER.debug(
                    "Could not revoke stale staging token %s", predecessor_id, exc_info=True
                )
        return
    pipeline_comment = staging_token_comment(pipeline_id)
    current = staging_token_comment(pipeline_id, update_id)
    try:
        listing = api.request("GET", "/api/2.0/token/list") or {}
    except LakebaseStateError:
        _LOGGER.debug("Could not list personal access tokens for staging cleanup", exc_info=True)
        return
    for info in listing.get("token_infos") or []:
        token_id = str(info.get("token_id") or "")
        comment = str(info.get("comment") or "")
        earlier = (
            bool(update_id)
            and comment != current
            and (comment == pipeline_comment or comment.startswith(f"{pipeline_comment}:"))
        )
        if token_id and (earlier or token_id == predecessor_id):
            try:
                api.request("POST", "/api/2.0/token/delete", {"token_id": token_id})
            except LakebaseStateError:
                _LOGGER.debug("Could not revoke stale staging token %s", token_id, exc_info=True)


# How long a staging token refresh that failed waits before it is tried again; the token
# it would have replaced is still valid then (refreshed at half its lifetime).
_STAGING_TOKEN_REFRESH_RETRY_SECONDS = 60.0


class StagingCredentialProvider:
    # Docstring: Supplies a Files-API bearer token, refreshing it before it can expire.
    #
    # Two tokens. The bearer is scoped to the File Management APIs: it is what every Files
    # API call sends and what split descriptors carry to executors, and leaked it calls no
    # other workspace API. It can't mint its successor, so beside it the provider holds a
    # minting token, scoped to the token APIs, which mints the next pair, its own successor
    # included, and revokes the tokens they replace and the pipeline's earlier updates'
    # (see revoke_stale_staging_tokens). The minting token never leaves this process: no
    # descriptor carries it, and neither token is written anywhere, a workspace secret
    # included. The first pair is minted with ``bootstrap_token`` -- whatever credential
    # the reader could carry from the driver (the captured ambient PAT), the latest
    # one a reader gave (take_bootstrap) -- which lasts about two days; the chain
    # needs no ambient identity after that, however long the reader runs. Refresh is
    # lazy -- checked on each ``bearer()`` call -- so there is no background thread to
    # manage; page writes call ``bearer()`` often enough to refresh well within the
    # half-life.
    #
    # A first-mint :class:`StagingTokenUnavailable` propagates so the caller falls
    # back to FUSE; a later *refresh* failure keeps the still-valid current token
    # rather than dropping a working credential. The half-life is that of the lifetime
    # the workspace granted, when shorter than the one requested, and a token the
    # Files API rejects anyway is replaced through :meth:`refresh`.

    def __init__(
        self,
        host: str,
        bootstrap_token: str,
        pipeline_id: str,
        *,
        lifetime_seconds: int = _DEFAULT_STAGING_TOKEN_LIFETIME_SECONDS,
        clock: Any = time.monotonic,
        update_id: str = "",
        sweeps_earlier_updates: Any = None,
    ) -> None:
        self._host = host.rstrip("/")
        self._bootstrap_token = bootstrap_token
        self._pipeline_id = pipeline_id
        self._update_id = update_id
        self._lifetime_seconds = max(_STAGING_TOKEN_MIN_LIFETIME_SECONDS, int(lifetime_seconds))
        self._refresh_after = self._lifetime_seconds * _STAGING_TOKEN_REFRESH_FRACTION
        self._clock = clock
        # Asked once, at the first mint: whether this process is the one to revoke the
        # tokens of the pipeline's earlier updates. Every flow process of an update
        # starts at once, and without it each listed and deleted every one of them.
        self._sweeps_earlier_updates = sweeps_earlier_updates
        self._lock = threading.Lock()
        self._token: str | None = None
        self._token_id = ""
        # The minting token, and its id: in this process alone (see the class comment).
        self._minter = ""
        self._minter_id = ""
        # The token a rotation replaced, revoked at the next one (see _mint).
        self._retiring_id = ""
        self._minted_at = 0.0
        # When a refresh that failed may be tried again (clock time); 0 until one fails.
        self._refresh_retry_at = 0.0
        # Set when a token was rejected and nothing could mint a successor, so an
        # ``auto`` staging transport can fall back to FUSE; cleared by the next mint.
        self.rejected = False

    def take_bootstrap(self, bootstrap_token: str) -> None:
        # Docstring: Take up a newer bootstrap credential for :meth:`refresh`: a reader's,
        # captured since this provider was made.
        with self._lock:
            self._bootstrap_token = bootstrap_token

    def sweep_claim(self, sweeps_earlier_updates: Any) -> None:
        # Docstring: Install how this provider asks whether its process is the one to
        # revoke earlier updates' tokens: each reader that takes the provider up gives
        # its own, so the provider asks through one in use. Under the lock a mint holds,
        # so a mint asks through one or the other, never a half-installed one.
        with self._lock:
            self._sweeps_earlier_updates = sweeps_earlier_updates

    def bearer(self) -> str:
        with self._lock:
            now = self._clock()
            if self._token is None:
                # First mint uses the carried bootstrap credential; a failure here is
                # fatal for REST and surfaces as StagingTokenUnavailable to the caller.
                self._mint(self._bootstrap_token)
            elif now - self._minted_at >= self._refresh_after and now >= self._refresh_retry_at:
                try:
                    self._mint(self._minter or self._bootstrap_token)
                except Exception:  # noqa: BLE001 - keep the still-valid token on refresh failure
                    # Tried again a minute on, not on the next call: every Files API
                    # request calls here, each 4 MiB chunk of a read included, and each
                    # failed refresh is a token request retried up to four times.
                    self._refresh_retry_at = now + _STAGING_TOKEN_REFRESH_RETRY_SECONDS
                    _LOGGER.warning(
                        "Snapshot-staging token refresh failed; reusing the current token and "
                        "trying again in %g s",
                        _STAGING_TOKEN_REFRESH_RETRY_SECONDS,
                        exc_info=True,
                    )
            return self._token or ""

    def refresh(self, rejected: str) -> str:
        # Docstring: Replace a token the Files API rejected (HTTP 401); returns the new bearer.
        #
        # The minting token mints the successor; failing that (revoked under it), the
        # carried bootstrap credential, and a failure of that too propagates. Only the
        # token that was rejected is replaced: another thread may already have. A failure
        # also drops the rejected token, so a later ``bearer()`` mints afresh instead of
        # resending it, and marks the credential ``rejected``.

        with self._lock:
            if self._token == rejected:
                try:
                    try:
                        self._mint(self._minter or self._bootstrap_token, rejected=True)
                    except Exception:  # pylint: disable=broad-except
                        if not self._minter:
                            raise
                        self._mint(self._bootstrap_token, rejected=True)
                except BaseException:
                    self._token = None
                    self.rejected = True
                    raise
            return self._token or ""

    def _mint(self, minting_token: str, *, rejected: bool = False) -> None:
        # Docstring: Mint the next pair through ``minting_token``: the minting token, then
        # the bearer through it.

        minter, minter_id, minter_granted = mint_staging_token(
            _WorkspaceApi(self._host, minting_token),
            self._pipeline_id,
            self._lifetime_seconds,
            self._update_id,
            _STAGING_MINTER_SCOPES,
        )
        token, token_id, granted = mint_staging_token(
            _WorkspaceApi(self._host, minter),
            self._pipeline_id,
            self._lifetime_seconds,
            self._update_id,
        )
        predecessor_id, replaced_minter_id = self._token_id, self._minter_id
        self._token, self._token_id = token, token_id
        self._minter, self._minter_id = minter, minter_id
        self._minted_at = self._clock()
        self.rejected = False
        lifetime = min(
            [self._lifetime_seconds] + [value for value in (granted, minter_granted) if value]
        )
        self._refresh_after = lifetime * _STAGING_TOKEN_REFRESH_FRACTION
        # Rotation, through the new minting token: revoke this provider's bearer before the
        # previous one, the minting token replaced, and, when this process is the one to,
        # earlier updates' tokens; best-effort inside the helper. The previous bearer goes
        # at the next rotation, unless it was rejected: split descriptors carry it to
        # executors, whose ranged reads it may still serve.
        earlier = not predecessor_id
        if not rejected:
            predecessor_id, self._retiring_id = self._retiring_id, predecessor_id
        claim = self._sweeps_earlier_updates
        if earlier and claim is not None:
            try:
                earlier = bool(claim())
            except Exception:  # pylint: disable=broad-except
                # Not knowing, sweep: a token left behind lives until it expires.
                _LOGGER.debug("Could not claim the staging token sweep", exc_info=True)
        api = _WorkspaceApi(self._host, minter)
        if earlier or predecessor_id:
            revoke_stale_staging_tokens(
                api, self._pipeline_id, self._update_id if earlier else "", predecessor_id
            )
        if replaced_minter_id:
            # Held in this process alone, so no executor may still be using it.
            revoke_stale_staging_tokens(api, self._pipeline_id, "", replaced_minter_id)


class WorkspaceFilesApi:
    # Docstring: Read/write UC Volume files over the Files REST API instead of the FUSE mount.
    #
    # Only the verbs snapshot staging needs, stdlib-only for the same reason as
    # :class:`_WorkspaceApi`. The bearer token is fetched from the credential
    # provider on every attempt, so a token refreshed between attempts is picked up.

    def __init__(self, host: str, credential: StagingCredentialProvider) -> None:
        self._host = _https_host(host)
        self._credential = credential

    def keep_fresh(self) -> None:
        # Docstring: Refresh the credential's tokens when due, between Files API calls.
        self._credential.bearer()

    def put(self, volume_path: str, data: bytes) -> None:
        # Docstring: Upload ``data`` to ``volume_path`` (a /Volumes/... path), overwriting.

        self._request("PUT", volume_path, data=data, overwrite=True)

    def get(self, volume_path: str) -> bytes:
        # Docstring: Download ``volume_path``; raises on any non-200.

        return self._request("GET", volume_path) or b""

    def get_range(self, volume_path: str, start: int, end: int) -> bytes:
        # Docstring: Download the half-open byte range ``[start, end)`` of ``volume_path``.
        #
        # Issues a ranged GET (``Range: bytes=start-end-1``, inclusive per RFC 9110),
        # which the Files API answers with ``206 Partial Content``. Lets a reader pull
        # one split of a large file straight from object storage without materializing
        # the whole file (the serverless FUSE mount caches the whole file on first
        # access, so it is unsuitable for large files). ``end <= start`` yields ``b""``.

        if end <= start:
            return b""
        return self._request("GET", volume_path, byte_range=(start, end - 1)) or b""

    @property
    def rejected(self) -> bool:
        # Docstring: Whether the credential's token was rejected and couldn't be replaced.

        return getattr(self._credential, "rejected", False) is True

    def _refreshed(self, rejected: str) -> bool:
        # Docstring: Whether the credential replaced a rejected token; False when it can't (an
        # executor's fixed bearer) or the replacement failed, so the 401 stands.

        refresh = getattr(self._credential, "refresh", None)
        if refresh is None:
            return False
        try:
            refresh(rejected)
        except Exception:  # noqa: BLE001 - the 401 is the error to report
            _LOGGER.warning(
                "Snapshot-staging token was rejected and could not be replaced", exc_info=True
            )
            return False
        return True

    def _request(
        self,
        method: str,
        volume_path: str,
        *,
        data: bytes | None = None,
        overwrite: bool = False,
        byte_range: tuple[int, int] | None = None,
    ) -> bytes | None:
        quoted = urllib.parse.quote(volume_path, safe="/")
        url = f"{self._host}/api/2.0/fs/files{quoted}"
        if overwrite:
            url += "?overwrite=true"
        last_error: Exception | None = None
        refreshed = False
        for attempt in range(_FILES_API_ATTEMPTS):
            try:
                bearer = self._credential.bearer()
            except Exception as error:
                if not getattr(self._credential, "rejected", False):
                    raise
                # Another request's refresh failed and dropped the token, and a new one
                # can't be had either (token creation disabled): the 401 that request
                # reported, so the caller falls back as it does, not fails the read.
                raise WorkspaceHttpError(
                    f"{method} files {volume_path}: the staging token was rejected", 401
                ) from error
            headers = {"Authorization": f"Bearer {bearer}"}
            if byte_range is not None:
                # Inclusive, zero-based per RFC 9110; the Files API answers 206.
                headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"
            if data is not None:
                headers["Content-Type"] = "application/octet-stream"
            request = urllib.request.Request(url, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(
                    request, timeout=_FILES_API_TIMEOUT_SECONDS
                ) as response:
                    if byte_range is None:
                        return response.read()
                    # At most the range: a server that ignored it (200, the whole file)
                    # gave every split of a file all of it, or exhausted the memory.
                    size = byte_range[1] - byte_range[0] + 1
                    body = response.read(size + 1)
                    if len(body) > size or (byte_range[0] and response.status != 206):
                        raise LakebaseStateError(
                            f"{method} files {volume_path}: the server did not honour the "
                            f"byte range (HTTP {response.status})"
                        )
                    # Short of its Content-Length: a sized read returns what came before
                    # the connection dropped and raises nothing, as a whole read does, so a
                    # split took the bytes it got for the end of its range. Retried below.
                    if getattr(response, "length", None):
                        raise http.client.IncompleteRead(body, response.length)
                    return body
            except urllib.error.HTTPError as error:
                # An expired or revoked token: replace it once and retry at once.
                if error.code == 401 and not refreshed:
                    refreshed = True
                    if self._refreshed(bearer):
                        continue
                if error.code < 500 and error.code != 429:
                    # A bounded read: a proxy's error page can be any size.
                    detail = error.read(4096).decode("utf-8", "replace")[:400]
                    raise WorkspaceHttpError(
                        f"{method} files {volume_path} failed with HTTP {error.code}: {detail}",
                        error.code,
                    ) from error
                last_error = error
            except _HTTP_TRANSIENT_ERRORS as error:
                # IncompleteRead: the server sent a valid response (for a ranged GET,
                # a 206 with the full Content-Length) but the body transfer was cut
                # short mid-stream. It is not a URLError/HTTPError, so without this it
                # would escape the retry loop and kill the read. The request is
                # deterministic and idempotent -- a ranged GET returns the same bytes
                # every time -- so re-issuing it recovers the truncated transfer.
                last_error = error
            if attempt + 1 < _FILES_API_ATTEMPTS:
                time.sleep(min(1.5 * (2**attempt), 8.0))
        raise LakebaseStateError(
            f"{method} files {volume_path} failed after {_FILES_API_ATTEMPTS} attempts"
        ) from last_error


def _quoted_identifier(connection: Any, name: str) -> str:
    # Docstring: Return ``name`` safely quoted for use as a Postgres identifier.
    #
    # A role name arrives from a connection option and lands in DDL, where it
    # cannot be a bind parameter. ``quote_ident`` is the server's own quoting, so
    # the escaping matches exactly what the server will parse.

    with connection.cursor() as cursor:
        cursor.execute("SELECT quote_ident(%s)", (name,))
        row = cursor.fetchone()
    if not row or not row[0]:
        raise LakebaseStateError(f"could not quote Lakebase role name {name!r}")
    return str(row[0])


def _quoted_literal(connection: Any, value: str) -> str:
    # Docstring: Return ``value`` quoted by the server as a Postgres string literal.
    #
    # For the role password: CREATE/ALTER ROLE is a utility statement, which takes no
    # bind parameters under psycopg 3's server-side binding (psycopg2 interpolates on
    # the client, so it never noticed). The error never echoes the value.

    with connection.cursor() as cursor:
        cursor.execute("SELECT quote_literal(%s)", (value,))
        row = cursor.fetchone()
    if not row or not row[0]:
        raise LakebaseStateError("could not quote the Lakebase role password")
    return str(row[0])


def ensure_login_role(admin_connection: Any, database: str, user: str, password: str) -> str:
    # Docstring: Create the password login role, or reset its password if it exists.
    #
    # Runs as the OAuth-authenticated workspace identity, which is the only caller
    # able to do this. Returns ``"created"`` or ``"reset"`` to say which happened.
    #
    # Reset rather than drop-and-recreate, deliberately. Once the role owns the
    # state tables -- which it does from its first run onwards -- ``DROP ROLE``
    # fails with ``DependentObjectsStillExist``, and the forced route
    # (``REASSIGN OWNED``/``DROP OWNED``) fails with ``InsufficientPrivilege``
    # because this identity holds CREATEROLE rather than superuser. Dropping would
    # also be the wrong outcome: the role owns ``conn_slots``, ``backlog_hints``
    # and ``state_records``, so removing it would discard the shared CDC state and
    # force every table to resnapshot. ``ALTER ROLE ... PASSWORD`` achieves the
    # same end -- the previous password stops working immediately -- while leaving
    # the data and its ownership untouched (all verified against a live endpoint).

    identifier = _quoted_identifier(admin_connection, user)
    database_identifier = _quoted_identifier(admin_connection, database)
    password_literal = _quoted_literal(admin_connection, password)
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
            cursor.execute(f"{verb} ROLE {identifier} WITH LOGIN PASSWORD {password_literal}")
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
    # rank live waiters for each slot (see _ACQUIRE_SLOT). ``waiter_class`` tags each
    # ticket with its requester class so acquisition can rotate service across classes
    # rather than serve strictly by arrival; it defaults to 0 (the consumer class) so an
    # un-tagged ticket behaves as before.
    """
    CREATE TABLE IF NOT EXISTS slot_waiters (
        namespace    text        NOT NULL,
        ticket_id    bigserial   NOT NULL,
        owner        text        NOT NULL,
        floor        integer     NOT NULL,
        ceiling      integer     NOT NULL,
        waiter_class smallint    NOT NULL DEFAULT 0,
        heartbeat_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, ticket_id)
    )
    """,
    # Add the class column in place for a slot_waiters table created by an older
    # deployment, so a mixed-version rollout is safe: tickets written before the
    # upgrade default to class 0. Guarded by an existence check rather than a bare
    # ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS``: that form still takes an
    # ACCESS EXCLUSIVE lock on slot_waiters *every* time ensure_schema runs (even when
    # the column already exists), which deadlocks against the concurrent acquire_slot
    # transactions that read slot_waiters and write conn_slots/slot_rr_cursor. The DO
    # block only issues the ALTER (and takes the lock) on the one run that actually
    # migrates; once the column exists it is a lock-free catalog read.
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'slot_waiters'
              AND column_name = 'waiter_class'
        ) THEN
            ALTER TABLE slot_waiters ADD COLUMN waiter_class smallint NOT NULL DEFAULT 0;
        END IF;
    END $$
    """,
    # Ordering scans walk a namespace's tickets oldest-first; the reaper filters by age.
    """
    CREATE INDEX IF NOT EXISTS slot_waiters_order_idx
        ON slot_waiters (namespace, ticket_id)
    """,
    # Round-robin rotation cursor: the requester class served most recently in a namespace.
    # The round-robin acquire reads it to rotate service to the next class; the winner
    # stamps its own class here. One row per namespace; -1 (the read-time default) means
    # nothing served yet, so the rotation begins at class 0.
    """
    CREATE TABLE IF NOT EXISTS slot_rr_cursor (
        namespace  text     NOT NULL,
        last_class smallint NOT NULL,
        PRIMARY KEY (namespace)
    )
    """,
    # reserved_deletes held a delete-channel slot reservation the connector no longer
    # has. Older connector versions sharing a namespace still write it, so it stays;
    # this one always writes 0.
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
    # A checkpoint-handoff token, parked by a source flow so a *different* flow --
    # one with a fresh Spark checkpoint after an SCD1->append switch or a destination
    # relocation -- can resume the CDC stream at the exact offset the old flow stopped
    # at, instead of re-snapshotting. Keyed by (namespace, table_key, channel), all
    # derived from the source only, so it is visible to any flow reading that source
    # table regardless of destination or flow type. Mutable (re-capture overwrites) and
    # deletable (consumed once the new flow has committed the seed), unlike the
    # write-once state_records. Populated by the table.migration option.
    """
    CREATE TABLE IF NOT EXISTS cdc_handoff (
        namespace   text        NOT NULL,
        table_key   text        NOT NULL,
        channel     smallint    NOT NULL DEFAULT 0,
        offset_json text        NOT NULL,
        schema_fp   text        NOT NULL,
        created_at  timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (namespace, table_key, channel)
    )
    """,
)


def ensure_schema(connection: Any) -> None:
    # Docstring: Create the state tables if they do not already exist.
    #
    # Serialized on a transaction-scoped advisory lock: ``CREATE TABLE IF NOT EXISTS``
    # is *not* concurrency-safe on first creation -- when a Lakeflow pipeline starts one
    # Python worker per flow and they all create a brand-new table at once (the first run
    # after a table is added), the existence check passes for all of them and every worker
    # but one then fails inserting the catalog row with ``duplicate key ... pg_type_typname
    # _nsp_index``. The lock makes the losers wait, so they find the objects already present.
    # It needs no connector schema and is released by the commit below, so it is safe during
    # first provisioning; the key is distinct from the role-provisioning lock's.

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            ("lakeflow-informix-ensure-schema",),
        )
        for statement in _SCHEMA_STATEMENTS:
            cursor.execute(statement)
    connection.commit()


def seed_slots(connection: Any, namespace: str, slot_count: int) -> None:
    # Docstring: Ensure ``slot_count`` rows exist for ``namespace``.
    #
    # Capacity is the row count, so seeding *is* configuring the semaphore. Rows
    # are only ever added: removing them while a holder is live would silently
    # revoke a lease, so a reduced limit is enforced by the acquire query's
    # ``slot_id`` bound instead.

    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO conn_slots (namespace, slot_id)"
            " SELECT %s, generated.slot_id FROM generate_series(0, %s - 1) AS generated(slot_id)"
            " ON CONFLICT (namespace, slot_id) DO NOTHING",
            (namespace, slot_count),
        )
    connection.commit()


def _stale_provisioning_error(error: BaseException) -> bool:
    # Docstring: Whether a failed state connect says the provisioning it used no longer holds:
    # a rejected password (Postgres words an absent role the same way) or a host that no
    # longer resolves.

    text = str(error).lower()
    return any(
        part in text
        for part in (
            "password authentication failed",
            "could not translate host name",
            "name or service not known",
            "nodename nor servname provided",
        )
    )
