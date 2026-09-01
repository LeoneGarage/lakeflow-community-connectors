"""Offline regressions for the Lakebase-backed slot and state implementation.

Runs against an in-process fake Postgres, so simulate mode needs no credentials,
no network, and no provisioned endpoint. The fake implements only the statements
this connector issues, but it implements the parts that carry the invariants --
``FOR UPDATE SKIP LOCKED`` row selection, the ``(owner, epoch)`` guards,
``GREATEST`` on hint upserts, and ``ON CONFLICT DO NOTHING RETURNING`` -- because
those are exactly what these tests exercise.

Every behaviour asserted here was first confirmed against a live Lakebase
endpoint; this module is the offline guard that keeps it from regressing.

Importing ``lakeflow_test`` first installs that module's PySpark stubs, which the
connector's imports require and which there is no reason to duplicate.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import threading
import time
import unittest
from unittest import mock
from unittest import mock

from databricks.labs.community_connector.sources.informix import lakebase_state
from databricks.labs.community_connector.sources.informix import (  # noqa: F401
    lakeflow_test as _pyspark_stubs,
)


class _FakeCursor:
    """Executes the connector's statements against dictionaries.

    Dispatch is by distinctive substring rather than by parsing SQL: the point is
    to model the *semantics* the real statements rely on, and a parser would add
    surface without adding fidelity. An unrecognised statement raises so a new
    query cannot silently pass its tests.
    """

    def __init__(self, database: "_FakeDatabase", connection: "_FakeConnection") -> None:
        self._database = database
        self._connection = connection
        self._result: list[tuple] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def close(self) -> None:
        return None

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)

    def execute(self, statement: str, parameters: dict | tuple | None = None) -> None:
        text = " ".join(statement.split())
        args = parameters or {}
        self._result = []
        if text.startswith("CREATE TABLE") or text.startswith("CREATE INDEX"):
            return
        if "INSERT INTO conn_slots" in text:
            self._seed_slots(args)
            return
        if text.startswith("UPDATE conn_slots SET owner = %(owner)s"):
            self._acquire(args, text)
            return
        if "UPDATE conn_slots SET renewed_at = now()" in text:
            self._heartbeat(args, text)
            return
        if "UPDATE conn_slots SET owner = NULL" in text:
            self._release(args, text)
            return
        if "INSERT INTO slot_waiters" in text:
            self._enqueue_waiter(args)
            return
        if "UPDATE slot_waiters SET heartbeat_at = now()" in text:
            self._heartbeat_waiter(args)
            return
        if "DELETE FROM slot_waiters" in text:
            self._delete_waiter(args, text)
            return
        if "INSERT INTO backlog_hints" in text:
            self._write_hint(args, text)
            return
        if "SELECT table_name, lsn FROM backlog_hints" in text:
            self._read_hints(args, text)
            return
        if "INSERT INTO conn_limits" in text:
            self._write_limit(args)
            return
        if "FROM conn_limits" in text:
            self._read_limit(args)
            return
        if "'table-activity'" in text and "INSERT INTO state_records" in text:
            self._touch_activity(args)
            return
        if "'state-gc'" in text and "INSERT INTO state_records" in text:
            self._claim_state_gc(args)
            return
        if text.startswith("SELECT extract(epoch FROM clock_timestamp())"):
            self._result = [(self._database.now(),)]
            return
        if "INSERT INTO state_records" in text:
            self._insert_record(args, text)
            return
        if "SELECT record FROM state_records" in text:
            self._select_record(args)
            return
        if text.startswith("DELETE FROM"):
            self._result = []
            return
        if text.startswith("SELECT quote_ident"):
            # Model the server's quoting well enough to prove the connector routes
            # identifiers through it: quote and double any embedded quote.
            name = args[0] if isinstance(args, tuple) else args["name"]
            self._result = [('"' + str(name).replace('"', '""') + '"',)]
            return
        if text.startswith("SELECT pg_advisory_xact_lock"):
            self._connection.acquire_role_advisory_lock()
            return
        if text.startswith("SELECT 1 FROM pg_roles WHERE rolname"):
            name = args[0] if isinstance(args, tuple) else args["name"]
            self._result = [(1,)] if name in self._database.roles else []
            return
        if text.startswith("CREATE ROLE") or text.startswith("ALTER ROLE"):
            self._role_password(text, args)
            return
        if text.startswith("GRANT "):
            # Both the role and the database reach this statement as identifiers,
            # and both come from connection options, so both must be quoted.
            for identifier in text.replace(" TO ", " ON ").split(" ON ")[1:]:
                word = identifier.replace("DATABASE ", "").replace("SCHEMA ", "").strip()
                if word in ("public",) or word.startswith('"'):
                    continue
                raise AssertionError(
                    f"GRANT identifier was not quoted by the server: {word!r}. "
                    "Route it through quote_ident rather than interpolating it."
                )
            return
        raise AssertionError(f"fake Postgres received an unmodelled statement: {text[:120]}")

    def _role_password(self, text: str, args: dict | tuple) -> None:
        """Apply CREATE/ALTER ROLE, recording the password and login flag."""

        # The role name is an identifier in the statement text, not a parameter,
        # so recover it the same way the server would -- by unquoting it. An
        # unquoted identifier is rejected rather than tolerated: a real server
        # folds it to lower case and fails outright on a name containing a quote
        # or a hyphen, so accepting it here would hide exactly the injection and
        # case-folding bugs that routing through ``quote_ident`` prevents.
        head = text.split(" ROLE ", 1)[1].split(" WITH ", 1)[0].strip()
        if not (head.startswith('"') and head.endswith('"')):
            raise AssertionError(
                f"role identifier was not quoted by the server: {head!r}. "
                "Route it through quote_ident rather than interpolating it."
            )
        name = head[1:-1].replace('""', '"')
        password = args[0] if isinstance(args, tuple) else args["password"]
        creating = text.startswith("CREATE ROLE")
        with self._database.lock:
            if creating and name in self._database.roles:
                raise AssertionError(f"CREATE ROLE for existing role {name}")
            if not creating and name not in self._database.roles:
                raise AssertionError(f"ALTER ROLE for absent role {name}")
            self._database.roles[name] = {
                "password": password,
                "can_login": "LOGIN" in text,
            }

    # -- conn_slots ---------------------------------------------------------

    def _seed_slots(self, args: dict) -> None:
        namespace, count = args if isinstance(args, tuple) else (args["ns"], args["n"])
        with self._database.lock:
            for slot_id in range(int(count)):
                self._database.slots.setdefault(
                    (namespace, slot_id),
                    {"owner": None, "epoch": 0, "scope": None, "renewed_at": None},
                )

    def _acquire(self, args: dict, text: str) -> None:
        # Honour the statement's own predicates rather than assuming them, so a
        # dropped clause changes behaviour here exactly as it would in Postgres.
        bounded_below = "slot_id >= %(floor)s" in text
        bounded_above = "slot_id < %(ceiling)s" in text
        expires = "renewed_at < now() - make_interval(secs => %(lease)s)" in text
        # The fair variant carries the NOT EXISTS over slot_waiters: a slot is
        # eligible only when no older, still-heartbeating waiter is also eligible for
        # it. Model that ordering so a dropped clause changes behaviour here as it
        # would in Postgres.
        fair = "slot_waiters" in text
        now = self._database.now()
        with self._database.lock:
            eligible = sorted(
                slot_id
                for (namespace, slot_id), row in self._database.slots.items()
                if namespace == args["namespace"]
                and (not bounded_below or slot_id >= args["floor"])
                and (not bounded_above or slot_id < args["ceiling"])
                and (
                    row["owner"] is None
                    or (
                        expires
                        and (row["renewed_at"] is None or now - row["renewed_at"] > args["lease"])
                    )
                )
            )
            if fair:
                ttl = float(args["waiter_ttl"])
                eligible = [
                    slot_id
                    for slot_id in eligible
                    if not any(
                        ns == args["namespace"]
                        and ticket < args["ticket_id"]
                        and waiter["floor"] <= slot_id < waiter["ceiling"]
                        and now - waiter["heartbeat_at"] <= ttl
                        for (ns, ticket), waiter in self._database.waiters.items()
                    )
                ]
            if not eligible:
                return
            # SKIP LOCKED semantics: the lock is held for the whole claim, so a
            # concurrent acquirer never observes this row mid-update and moves on
            # to the next eligible one instead of blocking.
            slot_id = eligible[0]
            row = self._database.slots[(args["namespace"], slot_id)]
            row["owner"] = args["owner"]
            if "epoch = epoch + 1" in text:
                row["epoch"] += 1
            row["scope"] = args["scope"]
            row["renewed_at"] = now
            self._result = [(slot_id, row["epoch"])]

    def _guarded(self, args: dict, text: str) -> dict | None:
        # Apply only the guards the statement actually carries: dropping
        # "epoch = %(epoch)s" must let a stale caller through here, just as it
        # would in Postgres.
        row = self._database.slots.get((args["namespace"], args["slot_id"]))
        if row is None:
            return None
        if "owner = %(owner)s" in text and row["owner"] != args["owner"]:
            return None
        if "epoch = %(epoch)s" in text and row["epoch"] != args["epoch"]:
            return None
        return row

    def _heartbeat(self, args: dict, text: str) -> None:
        with self._database.lock:
            row = self._guarded(args, text)
            if row is None:
                return
            row["renewed_at"] = self._database.now()
            self._result = [(args["slot_id"],)]

    def _release(self, args: dict, text: str) -> None:
        with self._database.lock:
            row = self._guarded(args, text)
            if row is None:
                return
            row["owner"] = None
            row["scope"] = None
            row["renewed_at"] = None
            self._result = [(args["slot_id"],)]

    # -- slot_waiters -------------------------------------------------------

    def _enqueue_waiter(self, args: dict) -> None:
        with self._database.lock:
            # bigserial: a single monotonic sequence, so a later enqueue always
            # gets a strictly larger ticket than an earlier one.
            self._database.waiter_seq += 1
            ticket_id = self._database.waiter_seq
            self._database.waiters[(args["namespace"], ticket_id)] = {
                "owner": args["owner"],
                "floor": int(args["floor"]),
                "ceiling": int(args["ceiling"]),
                "heartbeat_at": self._database.now(),
            }
            self._result = [(ticket_id,)]

    def _heartbeat_waiter(self, args: dict) -> None:
        with self._database.lock:
            waiter = self._database.waiters.get((args["namespace"], args["ticket_id"]))
            if waiter is None:
                return
            waiter["heartbeat_at"] = self._database.now()
            self._result = [(args["ticket_id"],)]

    def _delete_waiter(self, args: dict, text: str) -> None:
        now = self._database.now()
        with self._database.lock:
            if "heartbeat_at <" in text:
                # Reap: drop every ticket in the namespace whose owner stopped
                # heartbeating past the TTL.
                ttl = float(args["waiter_ttl"])
                stale = [
                    key
                    for key, waiter in self._database.waiters.items()
                    if key[0] == args["namespace"] and now - waiter["heartbeat_at"] > ttl
                ]
                for key in stale:
                    del self._database.waiters[key]
            else:
                # Dequeue one ticket by id.
                self._database.waiters.pop((args["namespace"], args["ticket_id"]), None)

    # -- backlog_hints ------------------------------------------------------

    def _write_hint(self, args: dict, text: str) -> None:
        key = (args["namespace"], args["table_name"])
        monotonic = "GREATEST(backlog_hints.lsn, EXCLUDED.lsn)" in text
        with self._database.lock:
            existing = self._database.hints.get(key)
            lsn = int(args["lsn"])
            if monotonic and existing is not None:
                lsn = max(int(existing["lsn"]), lsn)
            self._database.hints[key] = {"lsn": lsn, "updated_at": self._database.now()}

    def _read_hints(self, args: dict, text: str) -> None:
        fresh_only = "updated_at > now() - make_interval(secs => %(max_age)s)" in text
        now = self._database.now()
        with self._database.lock:
            self._result = [
                (table_name, row["lsn"])
                for (namespace, table_name), row in self._database.hints.items()
                if namespace == args["namespace"]
                and (not fresh_only or now - row["updated_at"] < float(args["max_age"]))
            ]

    # -- conn_limits --------------------------------------------------------

    def _write_limit(self, args: dict) -> None:
        with self._database.lock:
            self._database.limits[args["namespace"]] = {
                "max_connections": int(args["max_connections"]),
                "reserved_deletes": int(args["reserved"]),
                "version": int(args["version"]),
            }

    def _read_limit(self, args: dict) -> None:
        with self._database.lock:
            row = self._database.limits.get(args["namespace"])
            if row is not None:
                self._result = [(row["max_connections"], row["reserved_deletes"], row["version"])]

    # -- state_records ------------------------------------------------------

    def _insert_record(self, args: dict, text: str) -> None:
        key = (args["namespace"], args["record_key"])
        immutable = "DO NOTHING" in text
        minimum_lsn = "EXCLUDED.record->>'start_lsn'" in text
        with self._database.lock:
            if key in self._database.records and immutable:
                return  # ON CONFLICT DO NOTHING: no row returned
            if key in self._database.records and minimum_lsn:
                existing = json.loads(self._database.records[key])
                candidate = json.loads(args["record"])
                compatible = all(
                    existing.get(field) == candidate.get(field)
                    for field in (
                        "format_version",
                        "scope",
                        "table",
                    )
                )
                if not compatible or int(existing["start_lsn"]) <= int(candidate["start_lsn"]):
                    return
            self._database.records[key] = args["record"]
            self._result = [(args["record"],)]

    def _touch_activity(self, args: dict) -> None:
        key = (args["namespace"], args["record_key"])
        now = self._database.now()
        with self._database.lock:
            existing = self._database.records.get(key)
            if existing is not None:
                last_used = float(json.loads(existing)["last_used_at"])
                if last_used > now - float(args["touch_interval_seconds"]):
                    return
            record = {
                "format_version": 1,
                "pipeline_id": args["pipeline_id"],
                "table_prefix": args["table_prefix"],
                "last_used_at": now,
            }
            raw = json.dumps(record)
            self._database.records[key] = raw
            self._result = [(raw,)]

    def _claim_state_gc(self, args: dict) -> None:
        key = (args["namespace"], args["record_key"])
        now = self._database.now()
        with self._database.lock:
            existing = self._database.records.get(key)
            if existing is not None and float(json.loads(existing)["next_run_at"]) > now:
                return
            record = {
                "format_version": 1,
                "last_run_at": now,
                "next_run_at": now + float(args["gc_interval_seconds"]),
            }
            raw = json.dumps(record)
            self._database.records[key] = raw
            self._result = [(raw,)]

    def _select_record(self, args: dict) -> None:
        key = (args["namespace"], args["record_key"])
        with self._database.lock:
            stored = self._database.records.get(key)
            if stored is not None:
                self._result = [(stored,)]

    def _delete_scoped_records(self, args: dict) -> None:
        key_prefix = args["record_key_pattern"][:-1]
        scope_prefix = args["pipeline_scope_pattern"][:-1]
        with self._database.lock:
            deleted = []
            for key, raw in list(self._database.records.items()):
                namespace, record_key = key
                record = json.loads(raw)
                scope = record.get("scope")
                if (
                    namespace == args["namespace"]
                    and record_key.startswith(key_prefix)
                    and isinstance(scope, str)
                    and scope.startswith(scope_prefix)
                    and scope != args["current_scope"]
                    and (args["retained_scope"] is None or scope != args["retained_scope"])
                ):
                    deleted.append((record_key,))
                    del self._database.records[key]
            self._result = deleted

    def _delete_removed_table_records(self, args: dict) -> None:
        scope_prefix = args["pipeline_scope_pattern"][:-1]
        active = set(args["active_table_keys"])
        with self._database.lock:
            deleted = []
            for key, raw in list(self._database.records.items()):
                namespace, record_key = key
                parts = record_key.split("/")
                scope = json.loads(raw).get("scope")
                if (
                    namespace == args["namespace"]
                    and len(parts) > 1
                    and parts[1] not in active
                    and isinstance(scope, str)
                    and scope.startswith(scope_prefix)
                ):
                    deleted.append((record_key,))
                    del self._database.records[key]
            self._result = deleted


class _FakeDatabase:
    """Shared state behind every fake connection, with a controllable clock."""

    def __init__(self) -> None:
        self.slots: dict[tuple[str, int], dict] = {}
        # Fair-queue tickets, keyed (namespace, ticket_id); waiter_seq is the
        # bigserial sequence behind ticket_id.
        self.waiters: dict[tuple[str, int], dict] = {}
        self.waiter_seq: int = 0
        self.hints: dict[tuple[str, str], dict] = {}
        self.limits: dict[str, dict] = {}
        self.records: dict[tuple[str, str], str] = {}
        # Postgres roles, so password provisioning and validation are testable
        # without a live endpoint.
        self.roles: dict[str, dict] = {}
        self.role_advisory_lock = threading.Lock()
        self.lock = threading.RLock()
        self.offset = 0.0

    def now(self) -> float:
        return time.monotonic() + self.offset

    def advance(self, seconds: float) -> None:
        """Move the clock forward so lease expiry is testable without sleeping."""

        self.offset += seconds


class _FakeConnection:
    def __init__(self, database: _FakeDatabase) -> None:
        self._database = database
        self.closed = False
        self.commits = 0
        self.rollbacks = 0
        self._holds_role_advisory_lock = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._database, self)

    def acquire_role_advisory_lock(self) -> None:
        if not self._holds_role_advisory_lock:
            self._database.role_advisory_lock.acquire()
            self._holds_role_advisory_lock = True

    def _release_role_advisory_lock(self) -> None:
        if self._holds_role_advisory_lock:
            self._holds_role_advisory_lock = False
            self._database.role_advisory_lock.release()

    def commit(self) -> None:
        self.commits += 1
        self._release_role_advisory_lock()

    def rollback(self) -> None:
        self.rollbacks += 1
        self._release_role_advisory_lock()

    def close(self) -> None:
        self._release_role_advisory_lock()
        self.closed = True


class LakebaseSlotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.connection = _FakeConnection(self.database)
        lakebase_state.ensure_schema(self.connection)
        lakebase_state.seed_slots(self.connection, "ns", 16)

    def acquire(self, owner: str, **kwargs):
        parameters = {"slot_count": 16, "lease_seconds": 120.0}
        parameters.update(kwargs)
        return lakebase_state.acquire_slot(self.connection, "ns", owner, **parameters)

    def test_schema_indexes_state_gc_access_paths(self):
        schema = "\n".join(lakebase_state._SCHEMA_STATEMENTS)

        self.assertIn(
            "state_records_type_idx\n        ON state_records (namespace, record_type)",
            schema,
        )
        self.assertIn("state_records_activity_age_idx", schema)
        self.assertIn("(record->>'last_used_at')::double precision", schema)
        self.assertIn("WHERE record_type = 'table-activity'", schema)

    def test_capacity_is_never_exceeded(self):
        held = [self.acquire(f"o{index}") for index in range(20)]
        issued = [slot for slot in held if slot is not None]

        self.assertEqual(len(issued), 16)
        self.assertEqual(len({slot.slot_id for slot in issued}), 16)

    def test_seeding_is_idempotent(self):
        lakebase_state.seed_slots(self.connection, "ns", 16)

        self.assertEqual(len([key for key in self.database.slots if key[0] == "ns"]), 16)

    def test_expired_lease_is_reclaimed_by_a_new_owner(self):
        original = self.acquire("first")
        self.database.advance(200.0)

        successor = self.acquire("second")

        self.assertEqual(successor.slot_id, original.slot_id)
        self.assertEqual(successor.epoch, original.epoch + 1)

    def test_fresh_lease_is_not_reclaimed(self):
        self.acquire("first")
        self.database.advance(60.0)  # inside the 120s lease

        # Every other slot is free, so a reclaim would show up as a *reused* id.
        second = self.acquire("second")

        self.assertNotEqual(second.slot_id, 0)

    def test_zombie_heartbeat_is_rejected_after_reclaim(self):
        stale = self.acquire("zombie")
        self.database.advance(200.0)
        self.acquire("successor")

        self.assertFalse(lakebase_state.heartbeat_slot(self.connection, stale))

    def test_zombie_release_cannot_free_a_successors_slot(self):
        # The sharpest invariant: a late release from a dead owner must not free
        # the slot its successor now holds, which would over-issue capacity.
        stale = self.acquire("zombie")
        self.database.advance(200.0)
        successor = self.acquire("successor")

        self.assertFalse(lakebase_state.release_slot(self.connection, stale))
        row = self.database.slots[("ns", successor.slot_id)]
        self.assertEqual(row["owner"], "successor")

    def test_stale_handle_is_fenced_when_the_same_owner_reacquires(self):
        # ABA: the owner guard alone cannot catch this, because the owner string
        # matches. Only the epoch distinguishes the stale handle from the live
        # claim, so this is what makes the epoch load-bearing rather than
        # decorative. Reusing an owner is realistic on retry paths and after a
        # process re-registers under a recycled identity.
        first = self.acquire("same-owner")
        self.database.advance(200.0)
        second = self.acquire("same-owner")

        self.assertEqual(second.slot_id, first.slot_id)
        self.assertNotEqual(second.epoch, first.epoch)
        self.assertFalse(lakebase_state.heartbeat_slot(self.connection, first))
        self.assertFalse(lakebase_state.release_slot(self.connection, first))
        # The live claim must survive both stale calls untouched.
        self.assertEqual(self.database.slots[("ns", second.slot_id)]["owner"], "same-owner")
        self.assertEqual(self.database.slots[("ns", second.slot_id)]["epoch"], second.epoch)
        self.assertIsNotNone(self.database.slots[("ns", second.slot_id)]["renewed_at"])

    def test_holder_can_renew_and_release(self):
        slot = self.acquire("holder")

        self.assertTrue(lakebase_state.heartbeat_slot(self.connection, slot))
        self.assertTrue(lakebase_state.release_slot(self.connection, slot))
        self.assertIsNone(self.database.slots[("ns", slot.slot_id)]["owner"])

    def test_release_is_not_repeatable(self):
        slot = self.acquire("holder")
        lakebase_state.release_slot(self.connection, slot)

        self.assertFalse(lakebase_state.release_slot(self.connection, slot))

    def test_reservation_floor_excludes_low_slots(self):
        issued = [self.acquire(f"d{index}", floor=4) for index in range(16)]
        got = [slot for slot in issued if slot is not None]

        self.assertEqual(len(got), 12)
        self.assertTrue(all(slot.slot_id >= 4 for slot in got))

    def test_lowered_ceiling_takes_effect_without_removing_rows(self):
        issued = [self.acquire(f"f{index}", slot_count=3) for index in range(6)]
        got = [slot for slot in issued if slot is not None]

        self.assertEqual(len(got), 3)
        self.assertTrue(all(slot.slot_id < 3 for slot in got))

    def test_concurrent_acquirers_never_double_issue(self):
        held: set[int] = set()
        peak = [0]
        violations: list[str] = []
        guard = threading.Lock()

        def worker(index: int) -> None:
            for _ in range(8):
                slot = self.acquire(f"w{index}")
                if slot is None:
                    continue
                with guard:
                    if slot.slot_id in held:
                        violations.append(f"double-issue slot {slot.slot_id}")
                    held.add(slot.slot_id)
                    peak[0] = max(peak[0], len(held))
                with guard:
                    held.discard(slot.slot_id)
                lakebase_state.release_slot(self.connection, slot)

        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as pool:
            list(pool.map(worker, range(40)))

        self.assertEqual(violations, [])
        self.assertLessEqual(peak[0], 16)

    def test_scope_is_recorded_with_the_claim(self):
        self.acquire("holder", scope="pipe_@_update")

        self.assertEqual(self.database.slots[("ns", 0)]["scope"], "pipe_@_update")


class LakebaseHintTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.connection = _FakeConnection(self.database)
        lakebase_state.ensure_schema(self.connection)

    def test_hint_never_regresses(self):
        lakebase_state.publish_backlog_hint(self.connection, "ns", "tw316", 500)
        lakebase_state.publish_backlog_hint(self.connection, "ns", "tw316", 900)
        lakebase_state.publish_backlog_hint(self.connection, "ns", "tw316", 700)

        hints = lakebase_state.read_backlog_hints(self.connection, "ns")

        self.assertEqual(hints["tw316"], 900)

    def test_every_table_is_read_in_one_query(self):
        lakebase_state.publish_backlog_hint(self.connection, "ns", "tw316", 1)
        lakebase_state.publish_backlog_hint(self.connection, "ns", "tw221", 2)

        self.assertEqual(
            lakebase_state.read_backlog_hints(self.connection, "ns"),
            {"tw316": 1, "tw221": 2},
        )

    def test_stale_hints_are_filtered_by_age(self):
        lakebase_state.publish_backlog_hint(self.connection, "ns", "tw316", 1)
        self.database.advance(60.0)

        self.assertEqual(
            lakebase_state.read_backlog_hints(self.connection, "ns", max_age_seconds=45.0),
            {},
        )

    def test_namespaces_are_isolated(self):
        lakebase_state.publish_backlog_hint(self.connection, "a", "tw316", 1)
        lakebase_state.publish_backlog_hint(self.connection, "b", "tw316", 2)

        self.assertEqual(lakebase_state.read_backlog_hints(self.connection, "a"), {"tw316": 1})


class LakebaseLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.connection = _FakeConnection(self.database)
        lakebase_state.ensure_schema(self.connection)

    def test_limit_round_trips(self):
        lakebase_state.publish_connection_limit(self.connection, "ns", 16, 4, 1)

        self.assertEqual(
            lakebase_state.read_connection_limit(self.connection, "ns"),
            {"max_connections": 16, "reserved_deletes": 4, "version": 1},
        )

    def test_absent_limit_is_none(self):
        self.assertIsNone(lakebase_state.read_connection_limit(self.connection, "ns"))

    def test_republished_limit_replaces_the_previous_value(self):
        lakebase_state.publish_connection_limit(self.connection, "ns", 16, 4, 1)
        lakebase_state.publish_connection_limit(self.connection, "ns", 8, 2, 1)

        self.assertEqual(
            lakebase_state.read_connection_limit(self.connection, "ns"),
            {"max_connections": 8, "reserved_deletes": 2, "version": 1},
        )


class LakebaseStateRecordTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.connection = _FakeConnection(self.database)
        lakebase_state.ensure_schema(self.connection)

    def test_absent_record_is_none(self):
        self.assertIsNone(lakebase_state.read_state_record(self.connection, "ns", "key"))

    def test_record_round_trips(self):
        lakebase_state.publish_state_record(self.connection, "ns", "key", {"start_lsn": 7})

        self.assertEqual(
            lakebase_state.read_state_record(self.connection, "ns", "key"), {"start_lsn": 7}
        )

    def test_read_ends_its_transaction(self):
        # Regression: a bare SELECT must not leave the connection idle-in-transaction.
        # Lakebase reaps such connections after idle_in_transaction_session_timeout, which
        # killed the snapshot-drain worker between its scan and its boundary write.
        before = self.connection.rollbacks
        lakebase_state.read_state_record(self.connection, "ns", "absent")
        self.assertEqual(self.connection.rollbacks, before + 1)

        lakebase_state.publish_state_record(self.connection, "ns", "key", {"start_lsn": 7})
        before = self.connection.rollbacks
        lakebase_state.read_state_record(self.connection, "ns", "key")
        self.assertEqual(self.connection.rollbacks, before + 1)

    def test_second_writer_adopts_the_elected_record(self):
        first = lakebase_state.publish_state_record(self.connection, "ns", "key", {"v": 1})
        second = lakebase_state.publish_state_record(self.connection, "ns", "key", {"v": 2})

        self.assertEqual(first, {"v": 1})
        self.assertEqual(second, {"v": 1})

    def test_concurrent_writers_elect_exactly_one_record(self):
        winners: list[int] = []
        guard = threading.Lock()

        def worker(index: int) -> None:
            elected = lakebase_state.publish_state_record(
                self.connection, "ns", "contended", {"writer": index}
            )
            with guard:
                winners.append(elected["writer"])

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as pool:
            list(pool.map(worker, range(25)))

        self.assertEqual(len(winners), 25)
        self.assertEqual(len(set(winners)), 1)

    @staticmethod
    def _channel_record(lsn: int, *, schema_id: str = "schema") -> dict:
        return {
            "fingerprint": "fingerprint",
            "format_version": 1,
            "record_type": "channel-start",
            "schema_id": schema_id,
            "scope": "update",
            "start_lsn": str(lsn),
            "table": "demo:app.orders",
        }

    def test_minimum_lsn_record_can_only_move_backwards(self):
        publish = lakebase_state.publish_minimum_lsn_state_record

        self.assertEqual(
            publish(
                self.connection,
                "ns",
                "minimum",
                self._channel_record(120),
                record_type="channel-start",
            )["start_lsn"],
            "120",
        )
        self.assertEqual(
            publish(
                self.connection,
                "ns",
                "minimum",
                self._channel_record(90),
                record_type="channel-start",
            )["start_lsn"],
            "90",
        )
        self.assertEqual(
            publish(
                self.connection,
                "ns",
                "minimum",
                self._channel_record(150),
                record_type="channel-start",
            )["start_lsn"],
            "90",
        )

    def test_concurrent_minimum_lsn_publishers_converge_on_lowest(self):
        values = [150, 90, 120, 80, 110] * 5

        def publish(lsn: int) -> None:
            lakebase_state.publish_minimum_lsn_state_record(
                self.connection,
                "ns",
                "contended-minimum",
                self._channel_record(lsn),
                record_type="channel-start",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as pool:
            list(pool.map(publish, values))

        self.assertEqual(
            lakebase_state.read_state_record(self.connection, "ns", "contended-minimum")[
                "start_lsn"
            ],
            "80",
        )

    def test_minimum_lsn_record_carries_schema_of_lower_boundary(self):
        publish = lakebase_state.publish_minimum_lsn_state_record
        publish(
            self.connection,
            "ns",
            "schema-minimum",
            self._channel_record(120),
            record_type="channel-start",
        )

        winner = publish(
            self.connection,
            "ns",
            "schema-minimum",
            self._channel_record(90, schema_id="other"),
            record_type="channel-start",
        )

        self.assertEqual(winner, self._channel_record(90, schema_id="other"))

    def test_vanished_record_raises_rather_than_returning_an_unelected_value(self):
        # Returning the caller's own value here would let two readers disagree
        # about shared truth, which is worse than failing the read.
        lakebase_state.publish_state_record(self.connection, "ns", "key", {"v": 1})
        self.database.records.clear()
        original = _FakeCursor._insert_record

        def insert_then_vanish(cursor, args, text):
            cursor._result = []  # conflict: no row returned
            self.database.records.clear()  # and then removed externally

        _FakeCursor._insert_record = insert_then_vanish
        try:
            with self.assertRaises(lakebase_state.LakebaseStateError):
                lakebase_state.publish_state_record(self.connection, "ns", "key", {"v": 2})
        finally:
            _FakeCursor._insert_record = original

    def test_jsonb_text_and_dict_shapes_both_decode(self):
        # Drivers differ: psycopg2 returns jsonb as str, psycopg 3 as dict.
        self.assertEqual(lakebase_state._as_dict({"a": 1}), {"a": 1})
        self.assertEqual(lakebase_state._as_dict(json.dumps({"a": 1})), {"a": 1})
        with self.assertRaises(lakebase_state.LakebaseStateError):
            lakebase_state._as_dict("[1, 2]")

    def test_table_activity_touch_is_an_atomic_conditional_upsert(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ({"last_used_at": 1},)

        touched = lakebase_state.touch_table_activity(
            connection, "ns", "table/activity/pipeline", "table", "pipeline", 86400
        )

        self.assertTrue(touched)
        self.assertIn("ON CONFLICT", cursor.execute.call_args.args[0])
        self.assertEqual(cursor.execute.call_args.args[1]["touch_interval_seconds"], 86400)
        connection.commit.assert_called_once()

    def test_state_gc_loser_does_no_deletion(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None

        deleted = lakebase_state.collect_stale_table_state(
            connection, "ns", "state-gc/table-activity", 30, 24
        )

        self.assertEqual(deleted, 0)
        self.assertEqual(cursor.execute.call_count, 1)
        connection.commit.assert_called_once()

    def test_state_gc_winner_uses_thirty_day_cutoff_and_three_cleanup_steps(self):
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [({"next_run_at": 1},), (3_000_000.0,)]
        cursor.fetchall.side_effect = [[("a",)], [("b",), ("c",)], [("d",)]]

        deleted = lakebase_state.collect_stale_table_state(
            connection, "ns", "state-gc/table-activity", 30, 24
        )

        self.assertEqual(deleted, 4)
        self.assertEqual(cursor.execute.call_count, 5)
        cleanup_args = cursor.execute.call_args_list[2].args[1]
        self.assertEqual(cleanup_args["cutoff_epoch"], 3_000_000 - 30 * 86400)

    def test_obsolete_scope_delete_casts_nullable_retained_scope(self):
        # Regression: retained_scope defaults to None, which psycopg sends with an
        # unknown type OID. Without an explicit cast, `%(retained_scope)s IS NULL`
        # left Postgres unable to infer the parameter type and raised
        # AmbiguousParameter ("could not determine data type of parameter $5").
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [("channels/old",)]

        deleted = lakebase_state.delete_obsolete_scoped_state_records(
            connection, "ns", "channels", "pipe_@_", "pipe_@_new", retained_scope=None
        )

        self.assertEqual(deleted, 1)
        sql, params = cursor.execute.call_args.args
        normalized = " ".join(sql.split())
        self.assertIn("%(retained_scope)s::text IS NULL", normalized)
        self.assertNotIn("%(retained_scope)s IS NULL", normalized)
        self.assertIsNone(params["retained_scope"])
        connection.commit.assert_called_once()

    def test_touch_activity_casts_jsonb_value_parameters(self):
        # Regression: params used as jsonb_build_object values sit in "any"-typed
        # positions, so without an explicit cast the server cannot determine their
        # type when the statement is prepared (IndeterminateDatatype: parameter $3).
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ({"record": 1},)

        touched = lakebase_state.touch_table_activity(
            connection, "ns", "state-gc/table-activity", "prefix", "pipe", 30
        )

        self.assertTrue(touched)
        normalized = " ".join(cursor.execute.call_args.args[0].split())
        self.assertIn("%(pipeline_id)s::text", normalized)
        self.assertIn("%(table_prefix)s::text", normalized)
        connection.commit.assert_called_once()


class LakebaseProjectNamingTests(unittest.TestCase):
    def test_project_id_is_stable_and_dns_compliant(self):
        first = lakebase_state.project_id_for_connection("host\x009088\x00server")
        second = lakebase_state.project_id_for_connection("host\x009088\x00server")

        self.assertEqual(first, second)
        self.assertRegex(first, r"^[a-z][a-z0-9-]{0,62}$")

    def test_different_connections_get_different_projects(self):
        self.assertNotEqual(
            lakebase_state.project_id_for_connection("host-a\x009088\x00s"),
            lakebase_state.project_id_for_connection("host-b\x009088\x00s"),
        )

    def test_provisioning_lock_wait_is_bounded(self):
        state = lakebase_state.LakebaseState(
            {"lakebase.provision.timeout.seconds": "0.25"}, "identity"
        )
        lock = mock.MagicMock()
        lock.acquire.return_value = False

        with mock.patch.object(lakebase_state.LakebaseState, "_lock", return_value=lock):
            with self.assertRaisesRegex(
                lakebase_state.LakebaseStateError,
                "timed out after 0.25s waiting for the provisioning lock",
            ):
                state.provision()

        lock.acquire.assert_called_once_with(timeout=0.25)
        lock.release.assert_not_called()

    def test_provisioning_timeout_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "lakebase.provision.timeout.seconds"):
            lakebase_state.LakebaseState({"lakebase.provision.timeout.seconds": "0"}, "identity")

    def test_explicit_project_is_never_auto_deleted(self):
        state = lakebase_state.LakebaseState({"lakebase.project.id": "shared-project"}, "identity")
        state._api_client = mock.Mock(side_effect=AssertionError("must not list projects"))

        self.assertEqual(state.collect_unused_projects(90, 24), 0)

    def test_owned_inactive_project_is_deleted_after_retention(self):
        lakebase_state.LakebaseState._project_gc_checked.clear()
        state = lakebase_state.LakebaseState({}, "current")
        api = mock.MagicMock()
        api.request.side_effect = [
            {"projects": [{"name": "projects/informix-state-old"}]},
            {"name": "operations/delete"},
        ]
        state._api_client = mock.Mock(return_value=api)
        connection = mock.MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (1, 1, 1.0)
        state._open = mock.Mock(return_value=connection)
        state._admin_user = mock.Mock(return_value="admin")

        with (
            mock.patch.object(
                lakebase_state,
                "_endpoint_facts",
                return_value={"endpoint": "endpoint", "host": "host"},
            ),
            mock.patch.object(lakebase_state, "generate_credential", return_value="token"),
            mock.patch.object(lakebase_state.time, "time", return_value=100_000_000),
        ):
            deleted = state.collect_unused_projects(90, 24)

        self.assertEqual(deleted, 1)
        api.request.assert_any_call("DELETE", "/api/2.0/postgres/projects/informix-state-old")
        connection.close.assert_called_once()


class LakebaseLoginRoleTests(unittest.TestCase):
    """Provisioning of the static password role.

    ``ensure_login_role`` resets rather than drops-and-recreates because a drop is
    impossible once the role owns the state tables: plain ``DROP ROLE`` fails with
    ``DependentObjectsStillExist`` and the forced route needs superuser. These
    tests pin the resulting contract.
    """

    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.connection = _FakeConnection(self.database)

    def test_absent_role_is_created_with_login_and_password(self):
        action = lakebase_state.ensure_login_role(
            self.connection, "databricks_postgres", lakebase_state.STATE_ROLE, "pw-1"
        )

        self.assertEqual(action, "created")
        self.assertEqual(self.database.roles[lakebase_state.STATE_ROLE]["password"], "pw-1")
        self.assertTrue(self.database.roles[lakebase_state.STATE_ROLE]["can_login"])

    def test_existing_role_has_its_password_reset_and_is_never_dropped(self):
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "stale", "can_login": True}

        action = lakebase_state.ensure_login_role(
            self.connection, "databricks_postgres", lakebase_state.STATE_ROLE, "pw-2"
        )

        self.assertEqual(action, "reset")
        self.assertEqual(self.database.roles[lakebase_state.STATE_ROLE]["password"], "pw-2")
        # Still present: dropping it would discard the CDC state it owns.
        self.assertIn(lakebase_state.STATE_ROLE, self.database.roles)

    def test_existing_role_without_login_regains_it(self):
        # A role that cannot log in fails validation with an error that looks
        # exactly like a wrong password, so repair must restore LOGIN too.
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "pw", "can_login": False}

        lakebase_state.ensure_login_role(
            self.connection, "databricks_postgres", lakebase_state.STATE_ROLE, "pw"
        )
        self.assertTrue(self.database.roles[lakebase_state.STATE_ROLE]["can_login"])

    def test_concurrent_role_creation_converges_with_alter(self):
        class DuplicateRoleError(Exception):
            sqlstate = "23505"
            diag = type("Diag", (), {"constraint_name": "pg_authid_rolname_index"})()

        connection = mock.MagicMock()
        initial = mock.MagicMock()
        grants = mock.MagicMock()
        connection.cursor.side_effect = [
            initial,
            grants,
        ]
        initial.__enter__.return_value.fetchone.return_value = None
        initial.__enter__.return_value.execute.side_effect = [
            None,
            None,
            DuplicateRoleError(),
        ]

        with mock.patch.object(
            lakebase_state, "_quoted_identifier", side_effect=['"informix_state_user"', '"db"']
        ):
            action = lakebase_state.ensure_login_role(connection, "db", "informix_state_user", "pw")

        self.assertEqual(action, "reset")
        self.assertIn(
            "pg_advisory_xact_lock",
            initial.__enter__.return_value.execute.call_args_list[0].args[0],
        )
        connection.rollback.assert_called_once_with()
        self.assertEqual(grants.__enter__.return_value.execute.call_count, 2)
        connection.commit.assert_called_once_with()

    def test_advisory_lock_serializes_role_provisioning_across_connections(self):
        def provision(_: int) -> str:
            return lakebase_state.ensure_login_role(
                _FakeConnection(self.database),
                "databricks_postgres",
                lakebase_state.STATE_ROLE,
                "shared-password",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            actions = list(pool.map(provision, range(40)))

        self.assertEqual(actions.count("created"), 1)
        self.assertEqual(actions.count("reset"), 39)
        self.assertEqual(
            self.database.roles[lakebase_state.STATE_ROLE]["password"],
            "shared-password",
        )

    def test_role_name_is_quoted_by_the_server_not_interpolated(self):
        lakebase_state.ensure_login_role(self.connection, "databricks_postgres", 'weird"name', "pw")

        self.assertIn('weird"name', self.database.roles)

    def test_provisioning_commits_so_the_password_survives_the_connection(self):
        lakebase_state.ensure_login_role(
            self.connection, "databricks_postgres", lakebase_state.STATE_ROLE, "pw"
        )

        self.assertEqual(self.connection.commits, 1)


class LakebasePasswordValidationTests(unittest.TestCase):
    """The validate-then-repair path in ``LakebaseState``."""

    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.facts = {"endpoint": "projects/p/branches/b/endpoints/e", "host": "h"}
        self.admin_password = "admin-pw"

    def _state(self, **options):
        merged = {"lakebase.admin_user": "admin@example.com", **options}
        state = lakebase_state.LakebaseState(merged, "identity")
        state._facts = self.facts
        # Stand in for the network: record who connected with which credential and
        # honour the fake role table when deciding whether a login succeeds.
        self.attempts: list[tuple[str, str]] = []

        def open_connection(_facts, user, password):
            self.attempts.append((user, password))
            if user == "admin@example.com":
                # The admin may authenticate with an OAuth credential or with its
                # own configured password, and nothing else.
                if password not in ("oauth-token", self.admin_password):
                    raise RuntimeError(f"password authentication failed for user '{user}'")
                return _FakeConnection(self.database)
            role = self.database.roles.get(user)
            if not role or not role["can_login"] or role["password"] != password:
                raise RuntimeError(f"password authentication failed for user '{user}'")
            return _FakeConnection(self.database)

        state._open = open_connection  # type: ignore[method-assign]
        state._credential = lambda: "oauth-token"  # type: ignore[method-assign]
        return state

    def test_valid_password_is_accepted_without_touching_oauth(self):
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "pw", "can_login": True}
        state = self._state(**{"lakebase.password": "pw"})

        state._ensure_password_login(self.facts)

        # Exactly one attempt, as the configured role. No admin connection, so no
        # control-plane credential was needed for an already-working login.
        self.assertEqual(self.attempts, [(lakebase_state.STATE_ROLE, "pw")])

    def test_absent_role_is_provisioned_then_revalidated(self):
        state = self._state(**{"lakebase.password": "pw"})

        state._ensure_password_login(self.facts)

        self.assertEqual(self.database.roles[lakebase_state.STATE_ROLE]["password"], "pw")
        # Probe, admin repair, then a re-validation probe that must now succeed.
        self.assertEqual(
            self.attempts,
            [
                (lakebase_state.STATE_ROLE, "pw"),
                ("admin@example.com", "oauth-token"),
                (lakebase_state.STATE_ROLE, "pw"),
            ],
        )

    def test_wrong_password_is_repaired_to_match_the_configured_value(self):
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "stale", "can_login": True}
        state = self._state(**{"lakebase.password": "fresh"})

        state._ensure_password_login(self.facts)

        self.assertEqual(self.database.roles[lakebase_state.STATE_ROLE]["password"], "fresh")

    def test_repair_that_does_not_take_effect_raises_rather_than_proceeding(self):
        state = self._state(**{"lakebase.password": "pw"})
        # Simulate a repair that reports success but leaves login broken, which is
        # what a project without native login enabled would look like.
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "pw", "can_login": False}
        original = _FakeCursor._role_password
        _FakeCursor._role_password = lambda *args, **kwargs: None
        try:
            with self.assertRaises(lakebase_state.LakebaseStateError) as caught:
                state._ensure_password_login(self.facts)
        finally:
            _FakeCursor._role_password = original

        self.assertIn("native Postgres login", str(caught.exception))

    def test_missing_password_is_refused_rather_than_provisioning_without_one(self):
        # There is no OAuth fallback for state access any more, so a missing
        # password must fail loudly instead of creating a passwordless role.
        state = self._state()

        with self.assertRaises(lakebase_state.LakebaseStateError) as caught:
            state._ensure_password_login(self.facts)

        self.assertIn("lakebase.password", str(caught.exception))
        self.assertEqual(self.attempts, [])
        self.assertEqual(self.database.roles, {})

    def test_state_role_name_is_fixed_and_not_taken_from_options(self):
        # An option that used to name the role must no longer have any effect.
        state = self._state(**{"lakebase.password": "pw", "lakebase.user_id": "attacker"})

        state._ensure_password_login(self.facts)

        self.assertIn(lakebase_state.STATE_ROLE, self.database.roles)
        self.assertNotIn("attacker", self.database.roles)
        self.assertEqual(lakebase_state.STATE_ROLE, "informix_state_user")

    def test_connect_always_uses_the_state_role_never_the_admin(self):
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "pw", "can_login": True}
        state = self._state(**{"lakebase.password": "pw"})

        state.connect()

        self.assertEqual(self.attempts, [(lakebase_state.STATE_ROLE, "pw")])

    def test_admin_password_is_used_for_provisioning_when_configured(self):
        state = self._state(
            **{
                "lakebase.password": "data-pw",
                "lakebase.admin_password": self.admin_password,
            }
        )

        state._ensure_password_login(self.facts)

        # The admin connects with its own password, never the data role's, and
        # never a minted token.
        self.assertEqual(
            self.attempts,
            [
                (lakebase_state.STATE_ROLE, "data-pw"),
                ("admin@example.com", self.admin_password),
                (lakebase_state.STATE_ROLE, "data-pw"),
            ],
        )

    def test_admin_password_is_not_reused_as_the_data_password(self):
        state = self._state(
            **{
                "lakebase.password": "data-pw",
                "lakebase.admin_password": self.admin_password,
            }
        )

        state._ensure_password_login(self.facts)

        # The provisioned role must carry the data password, not the admin one:
        # conflating the two would hand provisioning rights to every reader.
        self.assertEqual(self.database.roles[lakebase_state.STATE_ROLE]["password"], "data-pw")

    def test_data_connections_never_use_the_admin_credential(self):
        self.database.roles[lakebase_state.STATE_ROLE] = {"password": "data-pw", "can_login": True}
        state = self._state(
            **{
                "lakebase.password": "data-pw",
                "lakebase.admin_password": self.admin_password,
            }
        )

        state.connect()

        self.assertEqual(self.attempts, [(lakebase_state.STATE_ROLE, "data-pw")])

    def test_oauth_is_still_used_when_only_the_data_password_is_configured(self):
        state = self._state(**{"lakebase.password": "data-pw"})

        state._ensure_password_login(self.facts)

        self.assertIn(("admin@example.com", "oauth-token"), self.attempts)

    def test_admin_prefers_its_configured_password_over_minting_a_token(self):
        state = self._state(**{"lakebase.admin_password": self.admin_password})

        state._connect_as_admin(self.facts)

        self.assertEqual(self.attempts, [("admin@example.com", self.admin_password)])

    def test_admin_mints_a_token_when_no_admin_password_is_configured(self):
        state = self._state()

        state._connect_as_admin(self.facts)

        self.assertEqual(self.attempts, [("admin@example.com", "oauth-token")])


class LakebaseConnectionParameterTests(unittest.TestCase):
    """What ``_open`` passes to the driver.

    Every pipeline flow failed with ``could not open certificate file
    "/root/.postgresql/postgresql.crt": Permission denied``. libpq probes for a
    default client certificate and treats EACCES on it as fatal, and the reader
    processes run with an untraversable ``HOME=/root``.
    """

    def setUp(self) -> None:
        self.state = lakebase_state.LakebaseState({"lakebase.password": "pw"}, "identity")
        self.captured: dict = {}
        module = type(sys)("psycopg2")
        module.connect = lambda **kwargs: self.captured.update(kwargs) or object()
        patch = mock.patch.dict(sys.modules, {"psycopg2": module})
        patch.start()
        self.addCleanup(patch.stop)

    def _open(self):
        self.state._open({"host": "ep.example"}, "someone", "secret")
        return self.captured

    def test_client_certificate_lookup_is_pointed_at_a_missing_path(self):
        parameters = self._open()

        # Must be absent, not empty: an empty sslcert falls back to the default
        # location and fails again, and /dev/null is read and rejected.
        for key in ("sslcert", "sslkey"):
            self.assertTrue(parameters[key].startswith("/nonexistent/"), parameters[key])
            self.assertFalse(os.path.exists(parameters[key]))

    def test_tls_is_still_required(self):
        # The certificate override must not weaken transport security.
        self.assertEqual(self._open()["sslmode"], "require")

    def test_credentials_and_target_are_passed_through(self):
        parameters = self._open()

        self.assertEqual(parameters["host"], "ep.example")
        self.assertEqual(parameters["user"], "someone")
        self.assertEqual(parameters["password"], "secret")
        self.assertEqual(parameters["dbname"], "databricks_postgres")

    def test_connect_timeout_allows_a_suspended_endpoint_to_wake(self):
        # Resuming from IDLE measured ~3.2s; the bound must be well clear of that.
        self.assertGreaterEqual(self._open()["connect_timeout"], 60)


class _FakeApi:
    """Records control-plane requests and serves canned project/endpoint reads."""

    def __init__(self, existing: bool = False) -> None:
        self.requests: list[tuple[str, str, dict | None]] = []
        self._existing = existing

    def request(self, method, path, body=None, tolerate_missing=False):
        self.requests.append((method, path, body))
        if path.endswith("/endpoints/primary"):
            return {
                "status": {
                    "hosts": {"host": "ep.example"},
                    "current_state": "ACTIVE",
                    "autoscaling_limit_min_cu": 0.5,
                    "autoscaling_limit_max_cu": 2,
                    "suspend_timeout_duration": "60s",
                }
            }
        if method == "GET":
            return {"project_id": "p"} if self._existing else None
        return {"done": True}

    def await_operation(self, response):
        return response

    def created_body(self):
        for method, path, body in self.requests:
            if method == "POST" and "/projects?project_id=" in path:
                return body
        return None


class LakebaseProjectCreationTests(unittest.TestCase):
    """The project-creation request body.

    Every setting here is verified to be honoured *only* at creation: the API
    accepts these fields nested elsewhere with HTTP 200 and silently ignores them,
    so the exact shape is the contract.
    """

    def test_creation_requests_autoscaling_suspend_and_native_login_together(self):
        api = _FakeApi(existing=False)

        lakebase_state.ensure_project(api, "proj", min_cu=0.5, max_cu=2, suspend_seconds=60)

        body = api.created_body()
        self.assertIsNotNone(body, "no project creation request was issued")
        spec = body["spec"]
        self.assertEqual(
            spec["default_endpoint_settings"],
            {
                "autoscaling_limit_min_cu": 0.5,
                "autoscaling_limit_max_cu": 2,
                "suspend_timeout_duration": "60s",
            },
        )
        # Without this, SCRAM password logins are refused outright.
        self.assertIs(spec["enable_pg_native_login"], True)

    def test_settings_are_sent_under_spec_not_at_the_top_level(self):
        # Sending them anywhere else is accepted and ignored, leaving the endpoint
        # at the 1/1 CU, 86400s defaults, so this pins the nesting.
        api = _FakeApi(existing=False)

        lakebase_state.ensure_project(api, "proj")

        body = api.created_body()
        self.assertNotIn("default_endpoint_settings", body)
        self.assertNotIn("enable_pg_native_login", body)
        self.assertNotIn("status", body)

    def test_existing_project_is_not_reconfigured(self):
        api = _FakeApi(existing=True)

        lakebase_state.ensure_project(api, "proj")

        self.assertIsNone(api.created_body())

    def test_deleted_project_is_purged_before_recreation(self):
        api = mock.MagicMock()
        api.request.side_effect = [
            {"name": "projects/proj", "delete_time": "2026-08-10T06:51:32Z"},
            {"name": "operations/purge"},
            None,
            {"done": True},
        ]

        with mock.patch.object(
            lakebase_state,
            "_endpoint_facts",
            return_value={"endpoint": "endpoint", "host": "host"},
        ):
            lakebase_state.ensure_project(api, "proj")

        self.assertEqual(
            api.request.call_args_list,
            [
                mock.call(
                    "GET",
                    "/api/2.0/postgres/projects/proj",
                    tolerate_missing=True,
                ),
                mock.call(
                    "DELETE",
                    "/api/2.0/postgres/projects/proj?purge=true",
                ),
                mock.call(
                    "GET",
                    "/api/2.0/postgres/projects/proj",
                    tolerate_missing=True,
                ),
                mock.call(
                    "POST",
                    "/api/2.0/postgres/projects?project_id=proj",
                    mock.ANY,
                ),
            ],
        )
        api.await_operation.assert_has_calls(
            [mock.call({"name": "operations/purge"}), mock.call({"done": True})]
        )

    def test_deleted_project_must_disappear_after_purge(self):
        api = mock.MagicMock()
        tombstone = {"name": "projects/proj", "delete_time": "2026-08-10T06:51:32Z"}
        api.request.side_effect = [tombstone, {"done": True}, tombstone]

        with self.assertRaisesRegex(lakebase_state.LakebaseStateError, "is still visible"):
            lakebase_state.ensure_project(api, "proj")


class LakebaseAdminIdentityTests(unittest.TestCase):
    """Resolving the provisioning role without any configuration.

    The process that needs this name cannot construct a ``WorkspaceClient``, so
    resolution goes through the driver-captured credential instead. Without this,
    a first run on a fresh project fails before it can create the login role,
    which would make ``lakebase.admin_user`` mandatory in practice.
    """

    def setUp(self) -> None:
        lakebase_state._CAPTURED_WORKSPACE.clear()
        self.addCleanup(lakebase_state._CAPTURED_WORKSPACE.clear)
        lakebase_state._CAPTURED_WORKSPACE.update(
            {"host": "https://ws.example", "token": "driver-token"}
        )
        self.responses: list[dict | None] = [{"userName": "user@example.com"}]
        self.paths: list[str] = []
        original = lakebase_state._WorkspaceApi.request

        def fake_request(_self, method, path, body=None, tolerate_missing=False):
            self.paths.append(path)
            return self.responses.pop(0) if self.responses else None

        lakebase_state._WorkspaceApi.request = fake_request
        self.addCleanup(setattr, lakebase_state._WorkspaceApi, "request", original)

    def _state(self, **options):
        return lakebase_state.LakebaseState(dict(options), "identity")

    def test_identity_is_resolved_from_the_captured_credential(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(self._state()._admin_user(), "user@example.com")
        self.assertEqual(self.paths, ["/api/2.0/preview/scim/v2/Me"])

    def test_service_principal_without_a_user_name_falls_back_to_its_id(self):
        self.responses = [{"id": "sp-client-id"}]
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(self._state()._admin_user(), "sp-client-id")

    def test_explicit_option_wins_and_makes_no_request(self):
        # The workspace would answer with a different name, so returning "chosen"
        # is only possible if the option short-circuits the lookup entirely.
        self.responses = [{"userName": "someone-else@example.com"}]

        self.assertEqual(self._state(**{"lakebase.admin_user": "chosen"})._admin_user(), "chosen")
        self.assertEqual(self.paths, [])

    def test_client_id_environment_wins_over_a_workspace_lookup(self):
        with mock.patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "env-sp"}, clear=True):
            self.assertEqual(self._state()._admin_user(), "env-sp")
        self.assertEqual(self.paths, [])

    def test_unresolvable_identity_names_the_option_that_fixes_it(self):
        self.responses = [None]
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch.dict(sys.modules, {"databricks.sdk": None}):
                with self.assertRaises(lakebase_state.LakebaseStateError) as caught:
                    self._state()._admin_user()

        self.assertIn("lakebase.admin_user", str(caught.exception))

    def test_first_run_reaches_repair_without_admin_user_configured(self):
        # The regression: resolving the admin role used to raise here, so a fresh
        # project could never have its login role created.
        state = self._state(**{"lakebase.password": "pw"})
        facts = {"host": "h", "endpoint": "e"}
        state._facts = facts
        attempted: list[str] = []

        class Connection:
            def cursor(self):
                raise AssertionError("reached the admin connection")

            def close(self):
                return None

        def opener(_facts, user, _password):
            attempted.append(user)
            if user == lakebase_state.STATE_ROLE:
                raise RuntimeError("password authentication failed")
            return Connection()

        state._open = opener  # type: ignore[method-assign]
        state._credential = lambda: "token"  # type: ignore[method-assign]
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(AssertionError):
                state._ensure_password_login(facts)

        self.assertEqual(attempted, [lakebase_state.STATE_ROLE, "user@example.com"])


class LakebaseCredentialCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        lakebase_state._CAPTURED_WORKSPACE.clear()
        self.addCleanup(lakebase_state._CAPTURED_WORKSPACE.clear)

    def test_capture_is_skipped_outside_a_databricks_runtime(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(lakebase_state.capture_workspace_credentials(), {})

    def test_captured_credentials_are_preferred_over_ambient_resolution(self):
        lakebase_state._CAPTURED_WORKSPACE.update(
            {"host": "https://captured.example", "token": "captured-token"}
        )

        host, token = lakebase_state._workspace_credentials({})

        self.assertEqual(host, "https://captured.example")
        self.assertEqual(token, "captured-token")

    def test_explicit_options_still_win_over_a_captured_credential(self):
        lakebase_state._CAPTURED_WORKSPACE.update({"host": "https://c", "token": "ct"})

        host, token = lakebase_state._workspace_credentials(
            {
                "lakebase.workspace.host": "explicit.example",
                "lakebase.workspace.token": "explicit-token",
            }
        )

        self.assertEqual(host, "https://explicit.example")
        self.assertEqual(token, "explicit-token")


class LakebaseWaiterQueueTests(unittest.TestCase):
    """The fair slot queue: per-slot FIFO layered over the raw acquire race."""

    def setUp(self) -> None:
        self.database = _FakeDatabase()
        self.connection = _FakeConnection(self.database)
        lakebase_state.ensure_schema(self.connection)

    def _seed(self, namespace: str, count: int) -> None:
        lakebase_state.seed_slots(self.connection, namespace, count)

    def _occupy(self, namespace: str, count: int, *, floor: int = 0, slot_count: int) -> None:
        for index in range(count):
            slot = lakebase_state.acquire_slot(
                self.connection, namespace, f"filler{index}", slot_count=slot_count, floor=floor
            )
            self.assertIsNotNone(slot)

    def _enqueue(self, namespace: str, owner: str, *, floor: int, ceiling: int) -> int:
        return lakebase_state.enqueue_waiter(
            self.connection, namespace, owner, floor=floor, ceiling=ceiling
        )

    def _fair_acquire(self, namespace: str, owner: str, ticket_id: int, *, slot_count, floor=0):
        return lakebase_state.acquire_slot(
            self.connection,
            namespace,
            owner,
            slot_count=slot_count,
            floor=floor,
            ticket_id=ticket_id,
        )

    def test_enqueue_returns_monotonic_tickets(self):
        first = self._enqueue("ns", "a", floor=0, ceiling=4)
        second = self._enqueue("ns", "b", floor=0, ceiling=4)

        self.assertLess(first, second)

    def test_older_waiter_wins_the_free_slot_in_its_band(self):
        # One free slot, two same-band waiters: the younger must not jump ahead of
        # the older while the older is still queued and live.
        self._seed("ns", 1)
        older = self._enqueue("ns", "older", floor=0, ceiling=1)
        younger = self._enqueue("ns", "younger", floor=0, ceiling=1)

        self.assertIsNone(self._fair_acquire("ns", "younger", younger, slot_count=1))
        self.assertIsNotNone(self._fair_acquire("ns", "older", older, slot_count=1))

    def test_younger_waiter_takes_a_low_slot_the_older_cannot_use(self):
        # Cross-band: an older delete-channel waiter reserved to slot >= 2 must not
        # block a younger consumer from the only free low slot (0) it could use.
        self._seed("ns", 3)
        self._occupy("ns", 2, floor=1, slot_count=3)  # take slots 1 and 2, leave 0 free
        older_high = self._enqueue("ns", "delete", floor=2, ceiling=3)
        younger_low = self._enqueue("ns", "consumer", floor=0, ceiling=3)

        self.assertIsNone(self._fair_acquire("ns", "delete", older_high, slot_count=3, floor=2))
        slot = self._fair_acquire("ns", "consumer", younger_low, slot_count=3)
        self.assertIsNotNone(slot)
        self.assertEqual(slot.slot_id, 0)

    def test_stale_waiter_ticket_stops_blocking(self):
        # An older waiter that stopped heartbeating past the TTL is ignored, so a
        # live younger waiter is not starved by a crashed predecessor.
        self._seed("ns", 1)
        self._enqueue("ns", "dead", floor=0, ceiling=1)
        younger = self._enqueue("ns", "live", floor=0, ceiling=1)
        self.database.advance(lakebase_state._DEFAULT_WAITER_TTL_SECONDS + 1.0)

        self.assertIsNotNone(self._fair_acquire("ns", "live", younger, slot_count=1))

    def test_heartbeat_reaps_stale_tickets(self):
        self._seed("ns", 1)
        stale = self._enqueue("ns", "dead", floor=0, ceiling=1)
        live = self._enqueue("ns", "live", floor=0, ceiling=1)
        self.database.advance(lakebase_state._DEFAULT_WAITER_TTL_SECONDS + 1.0)

        lakebase_state.heartbeat_waiter(
            self.connection, "ns", live, waiter_ttl=lakebase_state._DEFAULT_WAITER_TTL_SECONDS
        )

        self.assertNotIn(("ns", stale), self.database.waiters)
        self.assertIn(("ns", live), self.database.waiters)

    def test_dequeue_unblocks_a_younger_waiter(self):
        # Once the older waiter gives up (dequeues), the younger may claim the slot.
        self._seed("ns", 1)
        older = self._enqueue("ns", "older", floor=0, ceiling=1)
        younger = self._enqueue("ns", "younger", floor=0, ceiling=1)
        self.assertIsNone(self._fair_acquire("ns", "younger", younger, slot_count=1))

        lakebase_state.dequeue_waiter(self.connection, "ns", older)

        self.assertIsNotNone(self._fair_acquire("ns", "younger", younger, slot_count=1))

    def test_capacity_is_never_exceeded_with_tickets(self):
        # Fairness must not weaken the capacity guarantee: N ticketed acquirers on an
        # N-slot pool get N distinct slots and no more.
        self._seed("ns", 4)
        held = []
        for index in range(6):
            ticket = self._enqueue("ns", f"o{index}", floor=0, ceiling=4)
            slot = self._fair_acquire("ns", f"o{index}", ticket, slot_count=4)
            held.append(slot)
            if slot is not None:
                # A winner dequeues its ticket, exactly as the connector does, so it
                # does not linger and block the next acquirer.
                lakebase_state.dequeue_waiter(self.connection, "ns", ticket)
        issued = [slot for slot in held if slot is not None]

        self.assertEqual(len(issued), 4)
        self.assertEqual(len({slot.slot_id for slot in issued}), 4)


if __name__ == "__main__":
    unittest.main()
