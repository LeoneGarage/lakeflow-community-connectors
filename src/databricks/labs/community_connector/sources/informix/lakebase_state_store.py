"""The connector's Lakebase state store: its connection, connection slots and their wait queue."""

from __future__ import annotations

import contextlib
import hashlib
import math
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    _AUTO_PROJECT_PREFIX,
    _CONNECT_TIMEOUT_SECONDS,
    _CREDENTIAL_REFRESH_SECONDS,
    _DEFAULT_DATABASE,
    _DEFAULT_MAX_CU,
    _DEFAULT_MIN_CU,
    _DEFAULT_PROVISION_TIMEOUT_SECONDS,
    _DEFAULT_SUSPEND_SECONDS,
    _DEFAULT_WAITER_TTL_SECONDS,
    _LOGGER,
    _NO_CLIENT_CERTIFICATE,
    STATE_ROLE,
    LakebaseStateError,
    _endpoint_facts,
    _stale_provisioning_error,
    _workspace_credentials,
    _WorkspaceApi,
    ensure_login_role,
    ensure_project,
    ensure_schema,
    generate_credential,
    project_id_for_connection,
)


def validate_lakebase_options(options: dict[str, str]) -> None:
    # Docstring: Fail on a lakebase.project.id or lakebase.provision.timeout.seconds the
    # state could not use. The connector checks them as it starts: they were checked only
    # as the state was first opened, deep inside a read.

    project_id = options.get("lakebase.project.id", "").strip()
    # The id goes into workspace API paths and queries as it is. Lakebase's rule: a
    # leading digit or a trailing hyphen passed here and failed at the project API.
    if project_id and re.fullmatch(r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?", project_id) is None:
        raise LakebaseStateError(
            "lakebase.project.id must be 1 to 63 lowercase letters, digits or hyphens, "
            "starting with a letter and ending with a letter or digit"
        )
    raw = options.get("lakebase.provision.timeout.seconds", _DEFAULT_PROVISION_TIMEOUT_SECONDS)
    try:
        timeout = float(raw)
    except (TypeError, ValueError) as error:
        raise ValueError("Option 'lakebase.provision.timeout.seconds' must be a number") from error
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Option 'lakebase.provision.timeout.seconds' must be > 0")


def _current_user_from_captured_credential(options: dict[str, str]) -> str:
    # Docstring: Return the calling identity's user name, or "" if it cannot be resolved.
    #
    # A service principal has no ``userName``, so ``id`` is accepted as the
    # fallback: for one, that is the client id the Postgres role is named after.

    try:
        host, token = _workspace_credentials(options)
        response = _WorkspaceApi(host, token).request(
            "GET", "/api/2.0/preview/scim/v2/Me", tolerate_missing=True
        )
    except Exception:
        _LOGGER.debug("Could not resolve the calling identity from the workspace", exc_info=True)
        return ""
    body = response or {}
    # A service principal has no userName: its OAuth role in Lakebase is its application
    # id, not the numeric id SCIM gives it.
    return str(body.get("userName") or body.get("applicationId") or body.get("id") or "").strip()


def _default_user(options: dict[str, str]) -> str:
    # Docstring: Resolve the provisioning role name, which is the caller's own identity.
    #
    # The last two resolutions exist because the process that needs this name has
    # no ambient identity: ``WorkspaceClient()`` cannot be constructed there, so
    # asking the SDK who we are is not an option. Instead the driver's captured
    # credential is used to *ask the control plane*, which works from anywhere,
    # with the SDK kept only for the non-pipeline case.

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


class LakebaseState:
    # Docstring: Connection factory and provisioning entry point for Lakebase state.
    #
    # One instance per reader process. Provisioning runs at most once per project
    # per process behind a lock, so the 60-odd flows a pipeline starts converge on
    # a single set of control-plane calls instead of each issuing their own.

    # Keyed by project, database and password (see _provision_key).
    _provisioned: dict[tuple[str, str, str], dict[str, Any]] = {}
    # Held in a dict and built on first use rather than at class creation. Spark
    # cloudpickles the reader class, and the merged single-file deployment puts this
    # class in the same namespace the reader closes over, so a lock alive at import
    # would travel with it and make serialization fail outright ("cannot pickle
    # '_thread.lock' object"). Mutating a dict also avoids rebinding a name, which
    # the merged form -- where module globals become function locals -- cannot do.
    _provision_locks: dict[str, threading.Lock] = {}

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
        validate_lakebase_options({**options, "lakebase.project.id": self._project_id})
        self._min_cu = float(options.get("lakebase.autoscale.min.cu", _DEFAULT_MIN_CU))
        self._max_cu = float(options.get("lakebase.autoscale.max.cu", _DEFAULT_MAX_CU))
        self._suspend_seconds = int(
            options.get("lakebase.suspend.timeout.seconds", _DEFAULT_SUSPEND_SECONDS)
        )
        self._provision_timeout_seconds = float(
            options.get("lakebase.provision.timeout.seconds", _DEFAULT_PROVISION_TIMEOUT_SECONDS)
        )
        self._database = options.get("lakebase.database", "").strip() or _DEFAULT_DATABASE
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
        # As given, as the Informix password is: it signs in an admin role set up outside,
        # whose password can begin or end with a blank (only "" is unset, see _options_set).
        self._admin_password = options.get("lakebase.admin_password", "")
        self._facts: dict[str, Any] | None = None
        self._token: str | None = None
        self._token_minted_at = 0.0

    @property
    def project_id(self) -> str:
        return self._project_id

    def _provision_key(self) -> tuple[str, str, str]:
        # Docstring: What one provisioning in this process covers: the project, and the
        # database and state-role password it made ready. Keyed by the project alone, a
        # second connection on the project with another lakebase.database, or a changed
        # lakebase.password in a warm process, skipped the role repair and schema setup and
        # failed every connect. The password goes in as a digest, not as itself.

        digest = hashlib.sha256(self._password.encode("utf-8")).hexdigest()
        return (self._project_id, self._database, digest)

    def _api_client(self) -> _WorkspaceApi:
        # Made for each use: the ambient PAT lasts about two days, an OAuth token an hour,
        # and the one taken up here is captured again (_driver_registration) or renewed by
        # the SDK. Kept, an instance's credential rotation, re-provisioning and project GC
        # failed with it.
        return _WorkspaceApi(*_workspace_credentials(self._options))

    def provision(self) -> dict[str, Any]:
        # Docstring: Idempotently ensure the project, endpoint, and schema all exist.

        key = self._provision_key()
        cached = LakebaseState._provisioned.get(key)
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
            cached = LakebaseState._provisioned.get(key)
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
            # connect() below reads them; until the schema is set up they are provisional.
            self._facts = facts
            try:
                self._ensure_password_login(facts)
                connection = self.connect()
                try:
                    ensure_schema(connection)
                finally:
                    connection.close()
            except BaseException:
                # Kept, a later connect() took them as provisioned and reached a database
                # whose role or schema setup had failed (UndefinedTable on every query):
                # the next one provisions again.
                self._facts = None
                raise
            LakebaseState._provisioned[key] = facts
            return facts
        except LakebaseStateError as error:
            raise LakebaseStateError(
                f"Lakebase provisioning failed for project {self._project_id}: {error}"
            ) from error
        finally:
            lock.release()

    def _password_login_works(self, facts: dict[str, Any]) -> bool:
        # Docstring: Return whether the configured user_id/password can actually log in.

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
        # Docstring: Validate the state role's login, provisioning it if it fails.

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

    def connect(self, timeout: float | None = None) -> Any:
        # Docstring: Open a Postgres connection, waking a suspended endpoint if needed.
        #
        # ``psycopg2`` is preinstalled on the Databricks runtime; ``psycopg`` (v3)
        # is accepted as an alternative so the module also works where only that
        # is present. ``timeout`` shortens the connect timeout, for a caller with less
        # time than that left (a slot lease's renewal).

        facts = self._facts or self.provision()
        # Always the state role: its password needs no control-plane call and
        # cannot expire mid-stream. The admin identity is reached only through
        # ``_connect_as_admin``, during provisioning.
        try:
            return self._open(
                facts,
                self._user_id,
                self._password,
                **({} if timeout is None else {"timeout": timeout}),
            )
        except Exception as error:
            if _stale_provisioning_error(error):
                # The project was re-created under another host since this process
                # provisioned it, or the role's password changed under it: provisioned
                # once per process, every later connect failed. Forget it, so the next
                # connect provisions again and repairs the role. Not for other failures:
                # provisioning needs the workspace API, whose credentials a long-running
                # reader may no longer hold, where a plain retry would have worked.
                key = self._provision_key()
                if LakebaseState._provisioned.get(key) is facts:
                    LakebaseState._provisioned.pop(key, None)
                self._facts = None
            raise

    def _admin_user(self) -> str:
        # Docstring: Return the role name used for provisioning.
        #
        # ``_default_user`` already honours ``lakebase.admin_user`` first, so this
        # does not re-check it: two copies of the same precedence rule can disagree.

        return _default_user(self._options)

    def _connect_as_admin(self, facts: dict[str, Any]) -> Any:
        # Docstring: Connect as the provisioning identity.
        #
        # Prefers ``lakebase.admin_password`` when set, because a password works
        # even where a control-plane credential cannot be minted. Otherwise mints a
        # short-lived OAuth credential for the workspace identity, which is what
        # makes the zero-configuration case work.

        if self._admin_password:
            return self._open(facts, self._admin_user(), self._admin_password)
        return self._open(facts, self._admin_user(), self._credential())

    def _open(
        self,
        facts: dict[str, Any],
        user: str,
        password: str,
        database: str | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        # Docstring: Connect as ``user`` to ``database`` (lakebase.database by default),
        # waking a suspended endpoint if needed.

        parameters = {
            "host": facts["host"],
            "dbname": database or self._database,
            "user": user,
            "password": password,
            "sslmode": "require",
            # libpq takes whole seconds, and reads 0 as no limit.
            "connect_timeout": max(
                1, int(min(timeout or _CONNECT_TIMEOUT_SECONDS, _CONNECT_TIMEOUT_SECONDS))
            ),
            # connect_timeout bounds the connect alone. On an open connection whose path
            # drops packets, a slot heartbeat blocked for the OS retransmission time (about
            # 15 minutes), long past its 120 s lease. Unacknowledged data and a silent peer
            # now end it in about 30 s; a server-side wait is acknowledged, so not cut short.
            # Where the platform lacks these socket options libpq ignores them.
            "keepalives": 1,
            "keepalives_idle": 15,
            "keepalives_interval": 5,
            "keepalives_count": 3,
            "tcp_user_timeout": 30000,
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


def _state_database(
    state: "LakebaseState", facts: dict[str, Any], token: str, database: str
) -> Any:
    # Docstring: A connection to ``database`` if it holds connector state, or its catalog
    # does not say (the state role, taken next, decides as before); else None.

    connection = state._open(facts, state._admin_user(), token, database=database)
    try:
        # The catalog, which any role may read; the records are read as the state role
        # (see unused_projects).
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'state_records'"
            )
            found = bool(cursor.fetchone()[0])
        connection.commit()
    except Exception:  # pylint: disable=broad-except
        try:
            connection.rollback()
        except Exception:
            # A connection that dropped mid-query: closed, not left open.
            with contextlib.suppress(Exception):
                connection.close()
            raise
        return connection
    if found:
        return connection
    connection.close()
    return None


def _open_state_database(
    state: "LakebaseState", facts: dict[str, Any], token: str
) -> tuple[Any, str | None]:
    # Docstring: A connection, as the provisioning identity, to the database of another
    # connector project that holds its state, and that database's name: this
    # connection's lakebase.database, else the default one. (None, None) when neither
    # does.
    #
    # The project was named by another connection, which may have set another
    # lakebase.database: with this connection's alone, a project on the default one
    # was never swept. One on a third name is left alone, as unreadable.

    failure: Exception | None = None
    read = False
    for database in dict.fromkeys((state._database, _DEFAULT_DATABASE)):
        try:
            connection = _state_database(state, facts, token, database)
        except Exception as error:  # pylint: disable=broad-except
            failure = error
            continue
        if connection is not None:
            return connection, database
        read = True
    # A failure counts only where no database could be read: one read and found to hold
    # no state (a project of another kind, its state in a third) is no error, and
    # raised, warned on every sweep.
    if failure is not None and not read:
        raise failure
    return None, None


def _other_state_database(
    state: "LakebaseState", facts: dict[str, Any], token: str, connection: Any, database: str | None
) -> bool:
    # Docstring: Whether a database of the project other than ``database`` holds
    # connector state, or can't be told not to.

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT datname FROM pg_catalog.pg_database WHERE NOT datistemplate AND datallowconn"
        )
        names = [row[0] for row in cursor.fetchall() or ()]
    connection.commit()
    for name in names:
        if name != database:
            try:
                other = _state_database(state, facts, token, name)
            except Exception:  # pylint: disable=broad-except
                return True
            if other is not None:
                other.close()
                return True
    return False


def _sql(text: str) -> str:
    # Docstring: ``text`` with each line's indentation dropped: the statements ride in the
    # pickled source, which Spark ships within a size limit (see the merged source).

    return "\n".join(line.strip() for line in text.strip().splitlines())


# A candidate project's sweep: its connector activity, then anything else recent.
_PROJECT_ACTIVITY = _sql(
    """
SELECT
  COUNT(*) FILTER (WHERE record_type = 'state-gc'),
  COUNT(*) FILTER (WHERE record_type = 'table-activity'),
  MAX((record->>'last_used_at')::double precision)
    FILTER (WHERE record_type = 'table-activity')
FROM state_records
"""
)
_PROJECT_RECENT_USE = _sql(
    """
SELECT
  EXISTS (SELECT 1 FROM conn_slots WHERE renewed_at > now() - %s * interval '1 day'),
  EXISTS (SELECT 1 FROM slot_waiters WHERE heartbeat_at > now() - %s * interval '1 day'),
  EXISTS (SELECT 1 FROM backlog_hints WHERE updated_at > now() - %s * interval '1 day'),
  EXISTS (SELECT 1 FROM state_records WHERE created_at > now() - %s * interval '1 day'),
  EXISTS (SELECT 1 FROM cdc_handoff WHERE created_at > now() - %s * interval '1 day')
"""
)


# --- unused projects: an offline sweep -------------------------------------
#
# Run by the Lakebase project cleanup tool, never on a pipeline's reads: it lists every
# project in the workspace and connects to each auto-named connector project, waking its
# endpoint, which a reader had no business doing for other servers' projects.


# pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
def unused_projects(state: "LakebaseState", retention_days: int, *, delete: bool = False) -> list:
    # Docstring: The auto-named connector projects of the workspace, other than ``state``'s
    # own, whose state shows no use within ``retention_days``; deleted too with ``delete``.
    #
    # A project is judged by its connector state, read as the state role, which the
    # provisioning identity takes where it may: another identity's project, one with no
    # state or with state in another database, and one used within the retention are left
    # alone, and so is every project when the connection names its own.

    if state._options.get("lakebase.project.id", "").strip():
        return []
    projects: list = []
    api = state._api_client()
    path = "/api/2.0/postgres/projects"
    # Every page of the listing: a workspace with many projects has more than one.
    for _page in range(1000):
        response = api.request("GET", path) or {}
        if isinstance(response, list):
            projects += response
            break
        projects += response.get("projects") or []
        token = response.get("next_page_token")
        if not token:
            break
        path = "/api/2.0/postgres/projects?page_token=" + urllib.parse.quote(str(token), safe="")
    found = []
    for project in projects:
        name = str((project or {}).get("name") or (project or {}).get("id") or "")
        project_id = name.rsplit("/", 1)[-1]
        if not project_id.startswith(_AUTO_PROJECT_PREFIX) or project_id == state._project_id:
            continue
        if (project or {}).get("delete_time"):
            # A deleted project lingers in the list with no endpoint to wait for.
            continue
        connection = None
        requested = False
        try:
            facts = _endpoint_facts(api, project_id, deadline=time.monotonic() + 60)
            # As _connect_as_admin: the admin role's password where one is set, which an
            # OAuth token minted beside it failed to stand in for.
            token = state._admin_password or generate_credential(api, facts["endpoint"])
            connection, database = _open_state_database(state, facts, token)
            if connection is None:
                _LOGGER.debug(
                    "Leaving Lakebase project %s: no connector state found in it", project_id
                )
                continue
            # The records belong to the state role, and the provisioning identity holds
            # CREATEROLE, not superuser: it reads them as that role where it may take it,
            # granting itself the role it created first if need be. Where it may not
            # (another identity's project) the read below fails and it is left alone.
            for statements in (
                (f"SET ROLE {STATE_ROLE}",),
                (f"GRANT {STATE_ROLE} TO CURRENT_USER", f"SET ROLE {STATE_ROLE}"),
            ):
                try:
                    with connection.cursor() as cursor:
                        for statement in statements:
                            cursor.execute(statement)
                    # Kept: a role set in a transaction that is rolled back is unset
                    # again, and so is a grant.
                    connection.commit()
                    break
                except Exception:  # pylint: disable=broad-except
                    connection.rollback()
            else:
                _LOGGER.debug(
                    "Leaving Lakebase project %s: its state role cannot be taken", project_id
                )
                continue
            with connection.cursor() as cursor:
                cursor.execute(_PROJECT_ACTIVITY)
                gc_rows, activity_rows, last_used = cursor.fetchone()
            if not gc_rows or not activity_rows or last_used is None:
                continue
            if float(last_used) >= time.time() - retention_days * 86400:
                continue
            # Table activity alone said a project was idle while a reader used it: a stream
            # outside a pipeline records none, and a pipeline resumed after a long stop
            # waits for a slot before it records any. Anything else that moved within the
            # retention keeps it: a lease renewed, a reader waiting, a backlog hint, a
            # state record written, a stream's position parked for a migration.
            with connection.cursor() as cursor:
                cursor.execute(_PROJECT_RECENT_USE, (float(retention_days),) * 5)
                recent = cursor.fetchone()
            if any(recent):
                continue
            # The project is named by the server alone, so a connection that moved to
            # another lakebase.database kept it, its old database's records left behind:
            # judged by that database, the project went, with the state the other holds.
            if _other_state_database(state, facts, token, connection, database):
                _LOGGER.debug("Leaving Lakebase project %s: another database's state", project_id)
                continue
            connection.close()
            connection = None
            if delete:
                deleting = api.request("DELETE", f"/api/2.0/postgres/projects/{project_id}")
                requested = True
                api.await_operation(deleting, deadline=time.monotonic() + 600)
        except Exception:
            if not requested:
                _LOGGER.warning("Skipping Lakebase project %s", project_id, exc_info=True)
                continue
            # Asked for and going on at the server past the wait.
            _LOGGER.info("Lakebase project %s is still being deleted", project_id, exc_info=True)
        finally:
            if connection is not None:
                connection.close()
        found.append(project_id)
        if delete:
            for key in [key for key in LakebaseState._provisioned if key[0] == project_id]:
                LakebaseState._provisioned.pop(key, None)
    return found


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
#
# Every claim goes through the fair waiter queue: a slot is eligible only when no live
# waiter ranked ahead of this one is also eligible for it, so no waiter is lapped
# indefinitely under contention. Ordering is per slot rather than global: the band
# predicate (w.floor <= slot < w.ceiling) means an older waiter reserved to high slots does
# not block a younger one from a low slot it could never use, so there is no cross-band
# head-of-line blocking.
#
# Waiters are ranked by requester *class* in rotation, then by age. A per-namespace cursor
# (slot_rr_cursor) records the class served last; ``rot`` ranks each live waiter's class by
# cyclic distance *after* that cursor -- MOD(class - last - 1, N) -- so the class next in
# rotation has rot 0 and is served first, then the next, wrapping around. One class per
# grant, in rotation, so a class gets a turn every N grants no matter how many tickets
# another class holds. That is what serving by arrival cannot do: because a served waiter
# dequeues, the next same-class ticket would inherit the head and the high-volume class
# would keep winning; the cursor rotates past a class once it is served, so a burst of
# consumer tickets cannot crowd out the few CDC-daemon tickets. Within the chosen class the
# oldest ticket wins (ticket_id tiebreak); empty classes are skipped for free since only
# live waiters appear in ``live``. The ``live`` CTE drops tickets that stopped heartbeating
# so a dead ticket cannot block a slot even between reaper sweeps, and a caller whose own
# ticket is stale (``me`` empty) wins nothing until it heartbeats again. The ordering is
# advisory: FOR UPDATE SKIP LOCKED + the epoch CAS serialise the actual grant, so a stale
# cursor or a read race costs at most one extra sweep, never an over-issued slot. The
# winner advances the cursor to its own class (see _ADVANCE_RR_CURSOR) in the same
# transaction as the claim.
#
# ``num_classes`` is the rotation modulus (the count of distinct classes); ``1`` makes rot
# uniformly 0, so waiters are served strictly by ticket age.
_ACQUIRE_SLOT = _sql(
    """
WITH cur AS (
    SELECT COALESCE(
        (SELECT last_class FROM slot_rr_cursor WHERE namespace = %(namespace)s), -1
    ) AS last_class
),
live AS (
    SELECT ticket_id, floor, ceiling,
           MOD(waiter_class - (SELECT last_class FROM cur) - 1 + %(num_classes)s,
               %(num_classes)s) AS rot
    FROM slot_waiters
    WHERE namespace = %(namespace)s
      AND heartbeat_at >= now() - make_interval(secs => %(waiter_ttl)s)
),
me AS (
    SELECT rot, ticket_id FROM live WHERE ticket_id = %(ticket_id)s
)
UPDATE conn_slots SET owner = %(owner)s, epoch = epoch + 1,
                      scope = %(scope)s, renewed_at = now()
WHERE (namespace, slot_id) = (
    SELECT s.namespace, s.slot_id FROM conn_slots s
    CROSS JOIN me
    WHERE s.namespace = %(namespace)s
      AND s.slot_id >= %(floor)s
      AND s.slot_id < %(ceiling)s
      AND (s.owner IS NULL OR s.renewed_at < now() - make_interval(secs => %(lease)s))
      AND NOT EXISTS (
          SELECT 1 FROM live w
          WHERE w.floor <= s.slot_id AND s.slot_id < w.ceiling
            AND (w.rot, w.ticket_id) < (me.rot, me.ticket_id)
      )
    ORDER BY s.slot_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING slot_id, epoch
"""
)

# Renewal is guarded on (owner, epoch): a zombie's heartbeat is rejected rather
# than silently extending a lease its successor now owns. A caller that gets no
# row has provably lost the slot and must stop using its connection.
_HEARTBEAT_SLOT = _sql(
    """
UPDATE conn_slots SET renewed_at = now()
WHERE namespace = %(namespace)s AND slot_id = %(slot_id)s
  AND owner = %(owner)s AND epoch = %(epoch)s
RETURNING slot_id
"""
)

# Release carries the same guard, for a sharper reason: without it a zombie's
# late release would free a slot its successor legitimately holds, over-issuing
# capacity. On the Volume this hazard is what the mutating- marker guarded.
#
# renewed_at keeps the time of the release: a free slot is claimed by its NULL owner
# alone, and the unused-project sweep reads renewed_at as activity. Set to NULL, every
# slot of a stream outside a pipeline, which records no table activity, read as unused
# between its microbatches, and its project could be dropped under it.
_RELEASE_SLOT = _sql(
    """
UPDATE conn_slots SET owner = NULL, scope = NULL, renewed_at = now()
WHERE namespace = %(namespace)s AND slot_id = %(slot_id)s
  AND owner = %(owner)s AND epoch = %(epoch)s
RETURNING slot_id
"""
)

# The winner stamps the cursor with its own class so the next grant rotates to the class
# after it. Upsert because the row may not exist yet on the first grant in a namespace.
_ADVANCE_RR_CURSOR = _sql(
    """
INSERT INTO slot_rr_cursor (namespace, last_class) VALUES (%(namespace)s, %(waiter_class)s)
ON CONFLICT (namespace) DO UPDATE SET last_class = EXCLUDED.last_class
"""
)

# A waiter enqueues one ticket for the whole time it blocks, heartbeats it each sweep,
# and deletes it the instant it wins a slot (or gives up). The heartbeat doubles as the
# reaper -- it drops any ticket whose owner stopped heartbeating -- so a crashed or
# timed-out waiter cannot wedge the queue and the table needs no separate GC.
_ENQUEUE_WAITER = _sql(
    """
INSERT INTO slot_waiters (namespace, owner, floor, ceiling, waiter_class)
VALUES (%(namespace)s, %(owner)s, %(floor)s, %(ceiling)s, %(waiter_class)s)
RETURNING ticket_id
"""
)
_HEARTBEAT_WAITER = _sql(
    """
UPDATE slot_waiters SET heartbeat_at = now()
WHERE namespace = %(namespace)s AND ticket_id = %(ticket_id)s
RETURNING ticket_id
"""
)
_REAP_WAITERS = _sql(
    """
DELETE FROM slot_waiters
WHERE namespace = %(namespace)s
  AND heartbeat_at < now() - make_interval(secs => %(waiter_ttl)s)
"""
)
_DEQUEUE_WAITER = _sql(
    """
DELETE FROM slot_waiters WHERE namespace = %(namespace)s AND ticket_id = %(ticket_id)s
"""
)


class ConnectionSlot:
    # Docstring: A held connection-capacity lease.
    #
    # ``epoch`` is the fencing token; it is required to renew or release, so a
    # lease that expired while this process was stalled cannot be resurrected.

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
    ticket_id: int,
    floor: int = 0,
    scope: str | None = None,
    lease_seconds: float = 120.0,
    waiter_ttl: float = _DEFAULT_WAITER_TTL_SECONDS,
    waiter_class: int = 0,
    num_classes: int = 1,
) -> ConnectionSlot | None:
    # Docstring: Claim one slot, or return ``None`` when every eligible slot is busy.
    #
    # ``floor`` keeps a reader off the low slots: the CDC daemon and the snapshot drain
    # pass their reservations so they can never claim a slot kept for consumer reads,
    # which is what makes a reservation a guarantee rather than a preference.
    # ``slot_count`` bounds the ceiling, so lowering the configured limit takes effect
    # immediately even though surplus rows are deliberately left in place.
    #
    # ``ticket_id`` is this caller's place in the fair queue: the id returned by
    # ``enqueue_waiter``, heartbeated with the same ``waiter_ttl``. The claim yields any
    # slot a waiter ranked ahead of it is also eligible for, so this caller waits its turn.
    # ``waiter_class`` is this caller's class (matching the ticket) and ``num_classes`` the
    # rotation modulus; on a win the cursor is advanced to ``waiter_class`` in the same
    # transaction as the claim.

    parameters = {
        "owner": owner,
        "scope": scope,
        "namespace": namespace,
        "floor": max(0, floor),
        "ceiling": slot_count,
        "lease": float(lease_seconds),
        "ticket_id": int(ticket_id),
        "waiter_ttl": float(waiter_ttl),
        "num_classes": max(1, int(num_classes)),
    }
    with connection.cursor() as cursor:
        cursor.execute(_ACQUIRE_SLOT, parameters)
        row = cursor.fetchone()
        if row is not None:
            # Advance the rotation cursor to this class inside the same transaction as the
            # claim, so the next grant rotates past it. Best-effort ordering only -- a lost
            # advance under a race just repeats a class, never over-issues a slot.
            cursor.execute(
                _ADVANCE_RR_CURSOR,
                {"namespace": namespace, "waiter_class": int(waiter_class)},
            )
    connection.commit()
    if row is None:
        return None
    return ConnectionSlot(namespace, int(row[0]), int(row[1]), owner)


def enqueue_waiter(
    connection: Any,
    namespace: str,
    owner: str,
    *,
    floor: int,
    ceiling: int,
    waiter_class: int = 0,
) -> int:
    # Docstring: Register this caller as a slot waiter and return its queue ticket.
    #
    # The ticket records the eligible band [floor, ceiling); acquisition consults it
    # so a waiter ranked ahead in the same band is served first. ``waiter_class`` tags
    # the ticket with its requester class for the rotation; it defaults to 0, the
    # consumer class. Commit it before waiting so other waiters' connections can see it.

    with connection.cursor() as cursor:
        cursor.execute(
            _ENQUEUE_WAITER,
            {
                "namespace": namespace,
                "owner": owner,
                "floor": max(0, floor),
                "ceiling": int(ceiling),
                "waiter_class": int(waiter_class),
            },
        )
        row = cursor.fetchone()
    connection.commit()
    return int(row[0])


def heartbeat_waiter(connection: Any, namespace: str, ticket_id: int, *, waiter_ttl: float) -> bool:
    # Docstring: Renew this waiter's ticket and reap any whose owner stopped heartbeating.
    #
    # Folding the reap into the heartbeat keeps the queue self-cleaning: a crashed or
    # timed-out waiter's ticket ages out and is dropped by the next live heartbeat, so
    # it never blocks the queue and no separate GC pass is needed. Returns False when
    # the ticket no longer exists (it was reaped after a stall longer than
    # ``waiter_ttl``): acquisition only ever grants to a live ticket, so the caller must
    # enqueue a fresh one.

    with connection.cursor() as cursor:
        cursor.execute(
            _HEARTBEAT_WAITER,
            {"namespace": namespace, "ticket_id": int(ticket_id)},
        )
        renewed = cursor.fetchone() is not None
        cursor.execute(
            _REAP_WAITERS,
            {"namespace": namespace, "waiter_ttl": float(waiter_ttl)},
        )
    connection.commit()
    return renewed


def dequeue_waiter(connection: Any, namespace: str, ticket_id: int) -> None:
    # Docstring: Remove this waiter's ticket -- on winning a slot or on giving up.

    with connection.cursor() as cursor:
        cursor.execute(
            _DEQUEUE_WAITER,
            {"namespace": namespace, "ticket_id": int(ticket_id)},
        )
    connection.commit()


_PUT_CDC_HANDOFF = _sql(
    """
INSERT INTO cdc_handoff (namespace, table_key, channel, offset_json, schema_fp)
VALUES (%(namespace)s, %(table_key)s, %(channel)s, %(offset_json)s, %(schema_fp)s)
ON CONFLICT (namespace, table_key, channel)
DO UPDATE SET offset_json = EXCLUDED.offset_json,
              schema_fp = EXCLUDED.schema_fp,
              created_at = now()
"""
)

_GET_CDC_HANDOFF = _sql(
    """
SELECT offset_json, schema_fp FROM cdc_handoff
WHERE namespace = %(namespace)s AND table_key = %(table_key)s AND channel = %(channel)s
"""
)

_DELETE_CDC_HANDOFF = _sql(
    """
DELETE FROM cdc_handoff
WHERE namespace = %(namespace)s AND table_key = %(table_key)s AND channel = %(channel)s
"""
)


def put_cdc_handoff(
    connection: Any,
    namespace: str,
    table_key: str,
    channel: int,
    offset_json: str,
    schema_fp: str,
) -> None:
    # Docstring: Park (or overwrite) a table's checkpoint-handoff token. Idempotent re-capture.

    with connection.cursor() as cursor:
        cursor.execute(
            _PUT_CDC_HANDOFF,
            {
                "namespace": namespace,
                "table_key": table_key,
                "channel": int(channel),
                "offset_json": offset_json,
                "schema_fp": schema_fp,
            },
        )
    connection.commit()


def get_cdc_handoff(
    connection: Any, namespace: str, table_key: str, channel: int
) -> dict[str, str] | None:
    # Docstring: Return the parked ``{offset_json, schema_fp}`` token, or ``None`` when absent.

    with connection.cursor() as cursor:
        cursor.execute(
            _GET_CDC_HANDOFF,
            {"namespace": namespace, "table_key": table_key, "channel": int(channel)},
        )
        row = cursor.fetchone()
    connection.commit()
    if row is None:
        return None
    return {"offset_json": row[0], "schema_fp": row[1]}


def delete_cdc_handoff(connection: Any, namespace: str, table_key: str, channel: int) -> None:
    # Docstring: Remove a consumed handoff token. Idempotent (a missing row is a no-op).

    with connection.cursor() as cursor:
        cursor.execute(
            _DELETE_CDC_HANDOFF,
            {"namespace": namespace, "table_key": table_key, "channel": int(channel)},
        )
    connection.commit()


def heartbeat_slot(connection: Any, slot: ConnectionSlot, progressed: bool = False) -> bool:
    # Docstring: Renew a lease. ``False`` means the lease was lost and must not be reused.
    #
    # ``progressed``: the holder did work since its last renewal, counted as the pool's
    # progress (see _POOL_PROGRESS_TOKEN) in the same transaction, only if the lease was
    # still its own.

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
        if renewed and progressed:
            cursor.execute(_SLOT_PROGRESS, {"namespace": slot.namespace, "slot_id": slot.slot_id})
    connection.commit()
    return renewed


def release_slot(connection: Any, slot: ConnectionSlot) -> bool:
    # Docstring: Free a lease. ``False`` means it had already been reclaimed.

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


# A monotonic "the pool is doing work" token. ``epoch`` increments on every claim
# (see ``_ACQUIRE_SLOT``: ``epoch = epoch + 1``), and a holder that did work since its
# last lease renewal counts it in ``slot_progress`` at the next one. A caller that cannot
# win a slot polls this between sweeps to tell "the pool is busy -- keep waiting" from
# "the pool is wedged -- give up", without needing to see which slot moved. Counting
# claims alone, a pool whose every slot a long snapshot drain held looked wedged: a
# drain holds its slot for its whole scan, which can outlast the waiter's window. Only a
# drain counts its work (staged pages, change polls): a CDC shard daemon polls for as
# long as it runs, so its polls would make no pool ever look wedged, and a slot merely
# held, renewed with no work done, does not advance the token either.
_POOL_PROGRESS_TOKEN = _sql(
    """
SELECT COALESCE((SELECT SUM(epoch) FROM conn_slots WHERE namespace = %(namespace)s), 0)
     + COALESCE((SELECT SUM(progress) FROM slot_progress WHERE namespace = %(namespace)s), 0)
"""
)

# The work a slot's holder did since its last renewal, counted with that renewal.
_SLOT_PROGRESS = _sql(
    """
INSERT INTO slot_progress (namespace, slot_id, progress)
VALUES (%(namespace)s, %(slot_id)s, 1)
ON CONFLICT (namespace, slot_id) DO UPDATE SET progress = slot_progress.progress + 1
"""
)


def pool_progress_token(connection: Any, namespace: str) -> int:
    # Docstring: Return the pool's turnover token (see ``_POOL_PROGRESS_TOKEN``).

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
_WRITE_HINT = _sql(
    """
INSERT INTO backlog_hints (namespace, table_name, lsn, updated_at)
VALUES (%(namespace)s, %(table_name)s, %(lsn)s, now())
ON CONFLICT (namespace, table_name) DO UPDATE
   SET lsn = GREATEST(backlog_hints.lsn, EXCLUDED.lsn), updated_at = now()
"""
)

_READ_HINTS = _sql(
    """
SELECT table_name, lsn FROM backlog_hints
WHERE namespace = %(namespace)s
  AND updated_at > now() - make_interval(secs => %(max_age)s)
"""
)


def publish_backlog_hint(connection: Any, namespace: str, table_name: str, lsn: int) -> None:
    # Docstring: Record a table's log position, never regressing an existing value.

    with connection.cursor() as cursor:
        cursor.execute(
            _WRITE_HINT, {"namespace": namespace, "table_name": table_name, "lsn": int(lsn)}
        )
    connection.commit()


def read_backlog_hints(
    connection: Any, namespace: str, *, max_age_seconds: float = 45.0
) -> dict[str, int]:
    # Docstring: Return every fresh hint for a namespace in one round trip.
    #
    # Stale rows are filtered by age rather than deleted: a hint is advisory, and
    # leaving it costs one row per table instead of a reaper.

    with connection.cursor() as cursor:
        cursor.execute(_READ_HINTS, {"namespace": namespace, "max_age": float(max_age_seconds)})
        rows = cursor.fetchall()
    # End the read-only transaction so the connection is never left idle-in-transaction.
    connection.rollback()
    return {str(name): int(lsn) for name, lsn in rows}


# --- connection limit configuration ---------------------------------------

_WRITE_LIMIT = _sql(
    """
INSERT INTO conn_limits (namespace, max_connections, reserved_deletes, version, updated_at)
VALUES (%(namespace)s, %(max_connections)s, 0, %(version)s, now())
ON CONFLICT (namespace) DO UPDATE
   SET max_connections = EXCLUDED.max_connections,
       reserved_deletes = 0,
       version = EXCLUDED.version,
       updated_at = now()
"""
)


def publish_connection_limit(
    connection: Any,
    namespace: str,
    max_connections: int,
    version: int,
) -> None:
    # Docstring: Publish the agreed capacity configuration for a namespace.

    with connection.cursor() as cursor:
        cursor.execute(
            _WRITE_LIMIT,
            {
                "namespace": namespace,
                "max_connections": int(max_connections),
                "version": int(version),
            },
        )
    connection.commit()


def read_connection_limit(connection: Any, namespace: str) -> dict[str, int] | None:
    # Docstring: Return the published capacity configuration, or ``None`` if unset.

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT max_connections, version
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
        "version": int(row[1]),
    }


# --- immutable state records ----------------------------------------------
#
# ``INSERT ... ON CONFLICT DO NOTHING RETURNING`` is the head election. Exactly
# one writer gets a row back; every other writer gets nothing and re-reads the
# winner. That single statement replaces writing a candidate file, fsyncing it,
# and renaming it into an exclusive ``head`` -- along with the candidate sweeper
# that had to clean up whichever candidates lost.
_INSERT_RECORD = _sql(
    """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (%(namespace)s, %(record_key)s, %(record)s::jsonb, %(record_type)s)
ON CONFLICT (namespace, record_key) DO NOTHING
RETURNING record
"""
)

_SELECT_RECORD = _sql(
    """
SELECT record FROM state_records
WHERE namespace = %(namespace)s AND record_key = %(record_key)s
"""
)

_DELETE_OBSOLETE_SCOPED_RECORDS = _sql(
    """
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
)

_TOUCH_TABLE_ACTIVITY = _sql(
    """
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
)

_CLAIM_STATE_GC = _sql(
    """
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
)

# A pipeline's records for a table it no longer reads: its updates', and the two of its own
# that no update's cleanup takes (keyed by the pipeline, not scoped to an update), which
# outlived it while another pipeline read the table, and stayed when its others went.
_DELETE_STALE_PIPELINE_STATE = _sql(
    """
DELETE FROM state_records AS target
USING state_records AS activity
WHERE target.namespace = %(namespace)s
  AND activity.namespace = target.namespace
  AND activity.record_type = 'table-activity'
  AND (activity.record->>'last_used_at')::double precision < %(cutoff_epoch)s
  AND target.record_key LIKE activity.record->>'table_prefix' || '/%%'
  AND (target.record->>'scope' LIKE activity.record->>'pipeline_id' || '_@_%%'
    OR target.record_key IN (
      activity.record->>'table_prefix' || '/delete-floor/' || (activity.record->>'pipeline_id'),
      activity.record->>'table_prefix' || '/own-start/' || (activity.record->>'pipeline_id')))
RETURNING target.record_key
"""
)

_DELETE_UNUSED_TABLE_STATE = _sql(
    """
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
)

_DELETE_STALE_ACTIVITY = _sql(
    """
DELETE FROM state_records
WHERE namespace = %(namespace)s
  AND record_type = 'table-activity'
  AND (record->>'last_used_at')::double precision < %(cutoff_epoch)s
RETURNING record_key
"""
)

_PUBLISH_MINIMUM_LSN_RECORD = _sql(
    """
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
)


# Counterpart of the minimum publisher, for a record shared across every update scope
# for the table: the delete-bootstrap snapshot floor. Publishers of one update converge on
# the *highest* start_lsn; a record from another update replaces it whatever its
# start_lsn. That update took a snapshot of its own, the one the destination now holds,
# and its boundary can be lower: a full refresh after the source reinitialized its logical
# log, or from an extract whose boundary predates the last snapshot. Kept at the higher,
# older floor, a delete flow bootstrapping later started past deletes it had to apply. A
# record without a snapshot_scope keeps the highest only. Not "scope": records carrying
# that are a finished update's, and its cleanup deletes them.
_PUBLISH_MAXIMUM_LSN_RECORD = _sql(
    """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (%(namespace)s, %(record_key)s, %(record)s::jsonb, %(record_type)s)
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = EXCLUDED.record
WHERE state_records.record_type = EXCLUDED.record_type
  AND state_records.record->>'format_version' = EXCLUDED.record->>'format_version'
  AND state_records.record->>'table' = EXCLUDED.record->>'table'
  AND ((state_records.record->>'start_lsn')::numeric
         < (EXCLUDED.record->>'start_lsn')::numeric
       OR (EXCLUDED.record ? 'snapshot_scope'
           AND state_records.record->>'snapshot_scope'
               IS DISTINCT FROM EXCLUDED.record->>'snapshot_scope'))
RETURNING record
"""
)

# A full refresh's own start, which a delete reader took (own_start), and the lowest start
# an upsert reader published after it (start_lsn and its schema): another own start
# replaces the record, and a start lowers the one it holds (written alone where there is
# no record), so a delete reader sent back from that own start knows a start it never saw,
# of an update in between. No "scope", so the obsolete-update cleanup, which removes those
# updates' starts, leaves it.
_PUBLISH_OWN_START_RECORD = _sql(
    """
INSERT INTO state_records (namespace, record_key, record, record_type)
VALUES (%(namespace)s, %(record_key)s, %(record)s::jsonb, %(record_type)s)
ON CONFLICT (namespace, record_key) DO UPDATE
SET record = CASE WHEN EXCLUDED.record ? 'own_start' THEN EXCLUDED.record
  ELSE state_records.record || EXCLUDED.record END
WHERE state_records.record_type = EXCLUDED.record_type
  AND state_records.record->>'table' = EXCLUDED.record->>'table'
  AND CASE WHEN EXCLUDED.record ? 'own_start'
    THEN state_records.record->>'own_start' IS DISTINCT FROM EXCLUDED.record->>'own_start'
    ELSE NOT state_records.record ? 'start_lsn'
      OR (state_records.record->>'start_lsn')::numeric
         > (EXCLUDED.record->>'start_lsn')::numeric
  END
RETURNING record
"""
)


# Resolved once and cached, but in a dict mutated in place -- never a bare module
# global rebound via ``global``. The single-file merged deployment nests this whole
# module inside a function, turning module globals into that function's locals, so a
# ``global`` rebinding helper would address a name nothing ever assigns and raise
# ``NameError`` at runtime (mirrors _LAKEBASE_WAITER_LOCK in informix.py). Mutating a
# dict needs no rebinding, so it survives both the standalone and merged forms.
_CONNECTION_LOST_ERROR_TYPES: dict[str, tuple[type[BaseException], ...]] = {}
