"""Pure-Python, serverless-capable Informix snapshot and CDC connector.

The SQLI, CDC framing, codec, transaction, and Lakeflow paths are covered by
source-local regression tests. Informix 15 and serverless Lakeflow pipelines
have validated authentication, queries, discovery, snapshots, and CDC.
"""

from __future__ import annotations

import errno
import fnmatch
import hashlib
import importlib
import json
import logging
import math
import os
import re
import secrets
import stat
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Iterator, Protocol, Sequence

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    CdcFrameParser,
    ColumnDescriptor,
    OpenTransactionRecords,
    cdc_routine,
    decode_frame,
    metadata_column_names,
    validate_snapshot_arity,
)
from databricks.labs.community_connector.sources.informix.sqli import (
    InformixSqliClient,
    PasswordAuthenticationProvider,
    informix_locale_encoding,
)
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CURSOR = "_informix_change_lsn"
COMMIT_LSN = "_informix_commit_lsn"
TX_ID = "_informix_tx_id"
OP = "_informix_op"
_INTERNAL_COLUMNS = (CURSOR, COMMIT_LSN, TX_ID, OP)
_LSN_DECIMAL_WIDTH = 20
_OFFSET_VERSION = 7
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_DATA_OPS = {"INSERT", "BEFORE_UPDATE", "AFTER_UPDATE", "DELETE", "TRUNCATE"}
_DEFAULT_SNAPSHOT_PAGE_SIZE = 10000
_DEFAULT_MAX_RECORDS_PER_BATCH = 10000
_IMMUTABLE_STATE_VERSION = 1
_SHARED_STATE_WAIT_SECONDS = 300
_MAX_SHARED_STATE_BYTES = 1 << 20
_ARTIFACT_RETENTION_SECONDS = 3600
_HEADLESS_CANDIDATE_RETENTION_SECONDS = 30 * 24 * 60 * 60
_CANDIDATE_CLEANUP_INTERVAL_SECONDS = 24 * 60 * 60
_CANDIDATE_CLEANUP_MARKER_RETENTION_BUCKETS = 7
_MAX_CANDIDATE_CLEANUP_THROTTLES = 128
_VALIDATED_STATE_LOCATIONS: set[str] = set()
_LAST_CANDIDATE_CLEANUP: dict[str, float] = {}

def _informix_available_now_base(base: type) -> type:
    """Wrap the generated reader base without changing the shared adapter source."""

    original_base = getattr(base, "_informix_original_base", base)
    registration_scope = secrets.token_hex(16)

    class InformixAvailableNowBase(original_base):
        _informix_available_now_wrapper = True
        _informix_original_base = original_base

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)
            if cls.__name__ != "LakeflowStreamReader":
                return

            original_init = cls.__init__

            def initialize(reader, *args, **kwargs) -> None:
                original_init(reader, *args, **kwargs)
                setter = getattr(reader.lakeflow_connect, "set_registration_scope", None)
                if setter is not None:
                    setter(registration_scope)
                options = getattr(reader, "options", {})
                table = options.get("tableName", "<unknown>")
                role = "delete" if options.get("isDeleteFlow") == "true" else "upsert"
                logging.getLogger(__name__).info(
                    "Informix CDC reader initialized: scope=%s table=%s role=%s",
                    registration_scope,
                    table,
                    role,
                )

            def prepare_for_trigger(reader) -> None:
                prepare = getattr(
                    reader.lakeflow_connect, "prepare_for_trigger_available_now", None
                )
                if prepare is not None:
                    prepare()

            cls.prepareForTriggerAvailableNow = prepare_for_trigger
            cls.__init__ = initialize

    return InformixAvailableNowBase


# In the deployable merged module the shared trigger base has already been
# defined and the shared reader is defined after this connector. Shadow its base
# with an Informix-owned wrapper that installs the callback at class creation.
# In normal package imports that global is absent, so this block is inert.
try:
    # This name is local to register_lakeflow_source() after merging; globals()
    # cannot see it. In a normal package import it is intentionally undefined.
    _generated_trigger_base = SupportsTriggerAvailableNow  # type: ignore[name-defined]  # noqa: F821
except NameError:
    _generated_trigger_base = None
else:
    # Avoid assigning this name in register_lakeflow_source(): doing so would
    # make the imported PySpark base an uninitialized local throughout that
    # function. Replace the generated module global instead.
    globals()["SupportsTriggerAvailableNow"] = _informix_available_now_base(
        _generated_trigger_base
    )


class InformixError(RuntimeError):
    """Base error raised by this connector."""


class LogRetentionError(InformixError):
    """The requested restart LSN is no longer retained by Informix."""


class UnsupportedChangeError(InformixError):
    """A source operation cannot be represented by the Lakeflow interface."""


class InformixBridge(Protocol):
    """Injectable boundary around pure-Python SQLI metadata, snapshots, and CDC."""

    def list_tables(self) -> list[dict[str, Any]]: ...

    def get_table(self, identity: str) -> dict[str, Any]: ...

    def current_lsn(self) -> int: ...

    def minimum_lsn(self) -> int: ...

    def prepare_initial_capture(self, identities: Sequence[str]) -> int: ...

    def validate_initial_lsn(self, capture: dict[str, Any], start_lsn: int) -> None: ...

    def snapshot_page(
        self,
        identity: str,
        columns: Sequence[str],
        primary_keys: Sequence[str],
        after: Sequence[Any] | None,
        limit: int,
        max_bytes: int | None = None,
    ) -> list[dict[str, Any]]: ...

    def consistent_snapshot(
        self,
        identity: str,
        columns: Sequence[str],
        primary_keys: Sequence[str],
        page_size: int,
        max_rows: int,
        max_bytes: int,
    ) -> tuple[int, list[dict[str, Any]]]: ...

    def read_changes(
        self,
        tables: Sequence[dict[str, Any]],
        start_lsn: int,
        timeout_seconds: int,
        max_records: int,
    ) -> list[dict[str, Any]]: ...


class PurePythonInformixBridge:
    """Pure-Python SQLI/CDC bridge validated against disposable Informix 15.

    Tests can inject ``transport.factory=module:callable`` without changing
    Lakeflow or CDC decoding code.
    """

    def __init__(self, options: dict[str, str]) -> None:
        self.options = options
        authentication = options.get("authentication.mode", "password").lower()
        if authentication not in {"password", "pam"}:
            raise InformixError(
                f"Authentication mode {authentication!r} is unsupported; use password or pam."
            )
        self.config = _bridge_config(options)
        factory_path = options.get("transport.factory")
        if factory_path:
            self.transport = _load_factory(factory_path)(options)
        else:
            provider_path = options.get("authentication.provider.factory")
            provider = (
                _load_factory(provider_path)(options)
                if provider_path
                else PasswordAuthenticationProvider(
                    self.config["password"], options.get("authentication.pam.echo.response")
                )
            )
            self.transport = InformixSqliClient(
                self.config["hostname"],
                self.config["port"],
                self.config["database"],
                self.config["user"],
                self.config["password"],
                server_name=self.config["server"],
                db_locale=self.config["db_locale"],
                client_locale=self.config["client_locale"],
                tls=self.config["tls"],
                ca_file=self.config["ca_file"],
                pad_varchar=self.config["pad_varchar"],
                authentication_mode=authentication,
                authentication_provider=provider,
                pam_max_rounds=int(options.get("authentication.pam.max.rounds", "16")),
                login_timeout=float(options.get("authentication.login.timeout", "30")),
                redirect_enabled=_option_bool(options, "redirect.enabled", False),
                redirect_allowlist=_redirect_allowlist(options.get("redirect.allowlist", "")),
                redirect_max=int(options.get("redirect.max", "3")),
            )
        connect = getattr(self.transport, "connect", None)
        if connect:
            connect()

    def list_tables(self) -> list[dict[str, Any]]:
        maximum = int(self.options.get("metadata.max.bytes", str(64 << 20)))
        rows = self.transport.execute(
            "SELECT owner, tabname FROM systables "
            "WHERE tabtype = 'T' AND owner NOT MATCHES 'sys*' "
            "AND tabname NOT MATCHES 'sys*' ORDER BY owner, tabname",
            max_result_bytes=maximum or None,
        )
        identities = [
            (str(_field(row, "owner", 0)), str(_field(row, "tabname", 1))) for row in rows
        ]
        del rows
        result = []
        retained_bytes = _deep_size(result) + _deep_size(identities) if maximum else 0
        for owner, name in identities:
            table = self._describe_table(owner, name)
            if maximum:
                retained_bytes += _deep_size(table)
            if maximum and retained_bytes > maximum:
                raise InformixError(
                    f"Informix metadata discovery exceeded metadata.max.bytes={maximum}"
                )
            result.append(table)
        return result

    def get_table(self, identity: str) -> dict[str, Any]:
        parts = identity.split(".")
        if len(parts) != 3:
            raise InformixError(f"Invalid logical table identity {identity!r}")
        return self._describe_table(parts[1], parts[2])

    def _assert_capture_layout(self, capture: dict[str, Any], encoding: str) -> None:
        """Fail before decoding rows when catalog metadata changed mid-session."""

        native = str(capture["identity"])
        try:
            database, qualified = native.split(":", 1)
            owner, name = qualified.split(".", 1)
        except ValueError as error:
            raise InformixError(f"Invalid native table identity {native!r}") from error
        refreshed = _capture_descriptor(
            Table.parse(self._describe_table(owner, name), database), encoding
        )

        def layout(value: dict[str, Any]) -> dict[str, tuple[Any, ...]]:
            return {
                str(column["name"]): (
                    str(column["type_name"]),
                    int(column.get("length") or 0),
                    column.get("precision"),
                    column.get("scale"),
                    str(column.get("encoding") or "utf-8"),
                )
                for column in value["descriptors"]
            }

        capture_columns = list(capture["columns"])
        refreshed_columns = list(refreshed["columns"])
        refreshed_layout = layout(refreshed)
        capture_layout = layout(capture)
        prefix_is_unchanged = (
            refreshed_columns[: len(capture_columns)] == capture_columns
            and all(
                refreshed_layout.get(column) == capture_layout[column]
                for column in capture_columns
            )
        )
        if not prefix_is_unchanged:
            raise InformixError(
                f"Informix schema changed for {native!r} during CDC; "
                "run a full refresh before decoding additional records"
            )

    def _describe_table(self, owner: str, name: str) -> dict[str, Any]:
        columns = self.transport.execute(
            "SELECT c.colname, c.coltype, c.collength, c.colno, t.tabid, "
            "c.extended_id, x.name AS extended_name, x.owner AS extended_owner "
            "FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
            "LEFT JOIN sysxtdtypes x ON x.extended_id = c.extended_id "
            "WHERE t.owner = ? AND t.tabname = ? ORDER BY c.colno",
            (owner, name),
            max_result_bytes=(
                int(self.options.get("metadata.max.bytes", str(64 << 20))) or None
            ),
        )
        keys = self.transport.execute(
            "SELECT i.part1,i.part2,i.part3,i.part4,i.part5,i.part6,i.part7,i.part8,"
            "i.part9,i.part10,i.part11,i.part12,i.part13,i.part14,i.part15,i.part16 "
            "FROM systables t JOIN sysconstraints x ON t.tabid=x.tabid "
            "JOIN sysindexes i ON x.idxname=i.idxname AND x.tabid=i.tabid "
            "WHERE x.constrtype='P' AND t.owner=? AND t.tabname=?",
            (owner, name),
            max_result_bytes=(
                int(self.options.get("metadata.max.bytes", str(64 << 20))) or None
            ),
        )
        parsed_columns = [_catalog_column(row) for row in columns]
        for column in parsed_columns:
            if column["type_name"] in {"INT8", "SERIAL8"}:
                column["cdc_supported"] = True
            if column["type_name"] == "DATETIME":
                start, end = (column["length"] >> 8) & 0xF, column["length"] & 0xF
                column["cdc_supported"] = (
                    start in {0, 2, 4, 6, 8, 10}
                    and end in {0, 2, 4, 6, 8, 10, 11, 12, 13, 14, 15}
                    and end >= start
                )
        by_number = {
            int(_field(row, "colno", 3)): str(_field(row, "colname", 0)) for row in columns
        }
        primary_keys = []
        if keys:
            for position in range(16):
                column_number = int(_field(keys[0], f"part{position + 1}", position))
                if column_number > 0:
                    primary_keys.append(by_number[column_number])
        try:
            tabid = int(_field(columns[0], "tabid", 4))
        except (IndexError, KeyError, TypeError, ValueError) as error:
            raise InformixError(
                f"Informix catalog metadata for {owner}.{name} is missing tabid"
            ) from error
        if tabid <= 0:
            raise InformixError(
                f"Informix catalog metadata for {owner}.{name} has invalid tabid {tabid}"
            )
        return {
            "database": self.config["database"],
            "owner": owner,
            "name": name,
            "columns": parsed_columns,
            "primary_keys": primary_keys,
            "incarnation": str(tabid),
        }

    def current_lsn(self) -> int:
        row = self.transport.execute(
            "SELECT uniqid, used FROM sysmaster:syslogs WHERE is_current = 1"
        )[0]
        return (int(_field(row, "uniqid", 0)) << 32) + (int(_field(row, "used", 1)) << 12)

    def minimum_lsn(self) -> int:
        row = self.transport.execute("SELECT MIN(uniqid) AS uniqid FROM sysmaster:syslogs")[0]
        return int(_field(row, "uniqid", 0)) << 32

    def prepare_initial_capture(self, identities: Sequence[str]) -> int:
        """Enable full-row logging for every table, then capture one shared LSN."""

        if not identities:
            raise InformixError("Initial CDC preparation requires at least one table")
        enabled = []
        try:
            for identity in identities:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_set_fullrowlogging')}(?, 1)",
                        (identity,),
                    ),
                    f"cdc_set_fullrowlogging({identity})",
                )
                enabled.append(identity)
        except Exception as error:
            raise InformixError(
                "Initial CDC preparation was partially applied; full-row logging remains "
                f"enabled for {enabled!r}. Correct the failure and rerun preparation."
            ) from error
        return self.current_lsn()

    def validate_initial_lsn(self, capture: dict[str, Any], start_lsn: int) -> None:
        """Validate CDC registration/activation without reading LODATA records."""

        server_row = self.transport.execute(
            "SELECT env_value FROM sysmaster:sysenv WHERE env_name='INFORMIXSERVER'"
        )[0]
        server = str(_field(server_row, "env_value", 0))
        session_row = self.transport.execute(
            f"EXECUTE FUNCTION {cdc_routine('cdc_opensess')}(?, 0, 1, 1, 1, 1)",
            (server,),
        )[0]
        session = int(_field(session_row, "session_id", 0))
        if session < 0:
            raise InformixError(f"cdc_opensess failed with Informix error {session}")
        native = capture["identity"]
        started = False
        primary_error: BaseException | None = None
        try:
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_startcapture')}(?, 0, ?, ?, ?)",
                    (session, native, ",".join(capture["columns"]), 1),
                ),
                "cdc_startcapture",
            )
            started = True
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_activatesess')}(?, ?)",
                    (session, start_lsn),
                ),
                "cdc_activatesess",
            )
        except BaseException as error:
            primary_error = error
            raise
        finally:
            cleanup_errors = []
            if started:
                try:
                    _expect_zero(
                        self.transport.execute(
                            f"EXECUTE FUNCTION {cdc_routine('cdc_endcapture')}(?, 0, ?)",
                            (session, native),
                        ),
                        "cdc_endcapture",
                    )
                except Exception as error:
                    cleanup_errors.append(error)
            try:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)", (session,)
                    ),
                    "cdc_closesess",
                )
            except Exception as error:
                cleanup_errors.append(error)
            if cleanup_errors and primary_error is None:
                raise InformixError("Initial CDC validation cleanup failed") from cleanup_errors[0]
            if cleanup_errors and primary_error is not None and hasattr(primary_error, "add_note"):
                for error in cleanup_errors:
                    primary_error.add_note(
                        f"Initial Informix CDC validation cleanup also failed: {error}"
                    )

    def snapshot_page(
        self, identity, columns, primary_keys, after, limit, max_bytes=None
    ):
        database, owner, name = identity.split(".")
        for identifier in (database, owner, name, *columns, *primary_keys):
            if not _IDENTIFIER.fullmatch(identifier):
                raise InformixError(f"Unsafe snapshot identifier {identifier!r}")
        sql = f"SELECT FIRST {int(limit)} {','.join(columns)} FROM {database}:{owner}.{name}"
        parameters: list[Any] = []
        if after is not None:
            validate_snapshot_arity(after, primary_keys)
            clauses = []
            for index, key in enumerate(primary_keys):
                prefix = " AND ".join(f"{previous} = ?" for previous in primary_keys[:index])
                clauses.append(f"({prefix + ' AND ' if prefix else ''}{key} > ?)")
                parameters.extend(after[: index + 1])
            sql += " WHERE " + " OR ".join(clauses)
        if primary_keys:
            sql += " ORDER BY " + ",".join(primary_keys)
        rows = self.transport.execute(
            sql,
            tuple(parameters),
            max_result_bytes=(
                (max_bytes or None)
                if max_bytes is not None
                else (int(self.options.get("snapshot.max.bytes", str(256 << 20))) or None)
            ),
        )
        return [
            dict(row) if isinstance(row, dict) else dict(zip(columns, row, strict=True))
            for row in rows
        ]

    def consistent_snapshot(
        self, identity, columns, primary_keys, page_size, max_rows, max_bytes
    ):
        """Read one bounded point-in-time snapshot in a repeatable-read transaction."""

        execute_command = getattr(self.transport, "execute_command", self.transport.execute)
        ansi_rows = self.transport.execute(
            "SELECT is_ansi FROM sysmaster:sysdatabases WHERE name = ?",
            (self.config["database"],),
        )
        if len(ansi_rows) != 1:
            raise InformixError(
                f"Could not determine transaction mode for database {self.config['database']!r}"
            )
        is_ansi = bool(int(_field(ansi_rows[0], "is_ansi", 0)))
        if is_ansi:
            # The catalog SELECT starts an implicit transaction in an ANSI
            # database. End it before establishing the snapshot isolation.
            execute_command("COMMIT WORK")
        execute_command("SET ISOLATION TO REPEATABLE READ")
        if not is_ansi:
            execute_command("BEGIN WORK")
        try:
            snapshot_lsn = self.current_lsn()
            rows: list[dict[str, Any]] = []
            retained_bytes = _deep_size(rows) if max_bytes else 0
            after = None
            while True:
                remaining_rows = max_rows - len(rows)
                page_capacity = min(page_size, remaining_rows)
                remaining_bytes = max_bytes - retained_bytes if max_bytes else None
                if remaining_bytes is not None and remaining_bytes <= 0:
                    raise InformixError(
                        f"Initial snapshot exceeds snapshot.max.bytes={max_bytes}"
                    )
                page = self.snapshot_page(
                    identity,
                    columns,
                    primary_keys,
                    after,
                    page_capacity + 1,
                    remaining_bytes,
                )
                has_more = len(page) > page_capacity
                if has_more and remaining_rows == 0:
                    raise InformixError(
                        f"Initial snapshot exceeds snapshot.max.rows={max_rows}"
                    )
                accepted = page[:page_capacity]
                if max_bytes:
                    retained_bytes += _deep_size(accepted)
                if max_bytes and retained_bytes > max_bytes:
                    raise InformixError(
                        f"Initial snapshot exceeds snapshot.max.bytes={max_bytes}"
                    )
                rows.extend(accepted)
                if not has_more:
                    break
                after = [rows[-1][key] for key in primary_keys]
            execute_command("COMMIT WORK")
            return snapshot_lsn, rows
        except BaseException as primary_error:
            try:
                execute_command("ROLLBACK WORK")
            except Exception as cleanup_error:
                if hasattr(primary_error, "add_note"):
                    primary_error.add_note(
                        f"Informix snapshot rollback also failed: {cleanup_error}"
                    )
            raise

    def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
        set_socket_timeout = getattr(self.transport, "set_socket_timeout", None)
        previous_socket_timeout = getattr(self.transport, "socket_timeout", None)
        server_row = self.transport.execute(
            "SELECT env_value FROM sysmaster:sysenv WHERE env_name='INFORMIXSERVER'"
        )[0]
        server = str(_field(server_row, "env_value", 0))
        session_row = self.transport.execute(
            f"EXECUTE FUNCTION {cdc_routine('cdc_opensess')}(?, 0, ?, ?, 1, 1)",
            (server, timeout_seconds, max_records),
        )[0]
        session = int(_field(session_row, "session_id", 0))
        if session < 0:
            raise InformixError(f"cdc_opensess failed with Informix error {session}")
        labels: dict[int, tuple[ColumnDescriptor, ...]] = {}
        captures = []
        try:
            if set_socket_timeout is not None:
                set_socket_timeout(
                    max(float(previous_socket_timeout or 30), timeout_seconds + 5.0)
                )
            for label, capture in enumerate(tables, 1):
                native = capture["identity"]
                columns = tuple(capture["columns"])
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_set_fullrowlogging')}(?, 1)",
                        (native,),
                    ),
                    "cdc_set_fullrowlogging",
                )
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_startcapture')}(?, 0, ?, ?, ?)",
                        (session, native, ",".join(columns), label),
                    ),
                    "cdc_startcapture",
                )
                captures.append(native)
                labels[label] = tuple(_column_descriptor(c) for c in capture["descriptors"])
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_activatesess')}(?, ?)",
                    (session, start_lsn),
                ),
                "cdc_activatesess",
            )
            parser = CdcFrameParser(int(self.options.get("cdc.max.frame.bytes", str(16 << 20))))
            records = []
            budget_records = 0
            open_transactions = OpenTransactionRecords()
            max_transaction_records = int(self.options.get("cdc.max.transaction.records", "100000"))
            max_poll_records = int(self.options.get("cdc.max.poll.records", "200000"))
            max_poll_bytes = int(self.options.get("cdc.max.poll.bytes", "0"))
            requested = int(self.options.get("cdc.read.bytes", "32000"))
            empty_reads = 0
            timed_out = False
            retained_bytes = 0
            metadata_labels: set[int] = set()
            # cdc.max.records is a soft boundary: once crossed, finish every
            # transaction already observed so a transaction larger than the
            # native boundary can make progress. An idle open transaction is
            # still bounded by Informix's TIMEOUT control frame.
            while (
                (budget_records < max_records or open_transactions)
                and empty_reads < 1
                and not timed_out
            ):
                chunk = self.transport.read_lodata(session, requested)
                if not chunk:
                    empty_reads += 1
                    continue
                empty_reads = 0
                for frame in parser.feed(chunk):
                    record = decode_frame(frame, labels)
                    if max_poll_bytes:
                        retained_bytes += len(frame) + _deep_size(record)
                    if max_poll_bytes and retained_bytes > max_poll_bytes:
                        raise InformixError(
                            "A CDC poll exceeded "
                            f"cdc.max.poll.bytes={max_poll_bytes} while completing "
                            "interleaved transactions"
                        )
                    if record["op"] == "TIMEOUT":
                        # Informix represents an idle CDC timeout as a real,
                        # non-empty protocol frame. Treat it as the terminal
                        # condition for this finite poll instead of waiting for
                        # max_records timeout frames.
                        timed_out = True
                    label = record.get("label", record.get("capture_label"))
                    if record["op"] == "METADATA" and label in labels:
                        if label in metadata_labels:
                            raise InformixError(
                                f"Informix emitted a second CDC metadata layout for capture "
                                f"label {label}; run a full refresh before continuing"
                            )
                        descriptors = {column.name: column for column in labels[label]}
                        encoding = next(iter(descriptors.values())).encoding
                        names = metadata_column_names(record["metadata"], encoding)
                        if set(names) != set(descriptors):
                            raise InformixError(
                                f"CDC metadata for capture label {label} does not match "
                                "the requested columns"
                            )
                        self._assert_capture_layout(tables[label - 1], encoding)
                        labels[label] = tuple(descriptors[name] for name in names)
                        metadata_labels.add(label)
                    if label and 1 <= label <= len(tables):
                        record["table"] = tables[label - 1]["logical_identity"]
                    records.append(record)
                    if record["op"] not in {"METADATA", "TIMEOUT"}:
                        budget_records += 1
                    if len(records) > max_poll_records:
                        raise InformixError(
                            "A CDC poll exceeded "
                            f"cdc.max.poll.records={max_poll_records} while completing "
                            "interleaved transactions"
                        )
                    if record["op"] == "BEGIN":
                        open_transactions.begin(int(record["tx_id"]))
                    elif record["op"] in _DATA_OPS:
                        open_transactions.append(int(record["tx_id"]), int(record["lsn"]))
                    elif record["op"] == "DISCARD":
                        open_transactions.discard(int(record["tx_id"]), int(record["lsn"]))
                    elif record["op"] in {"COMMIT", "ROLLBACK"}:
                        open_transactions.finish(int(record["tx_id"]))
                    if open_transactions.buffered > max_transaction_records:
                        raise InformixError(
                            "An open CDC transaction exceeded "
                            f"cdc.max.transaction.records={max_transaction_records}"
                        )
                    if timed_out:
                        # Ignore anything following the terminal timeout in this
                        # session. The checkpoint remains at the last complete
                        # transaction, so a later poll safely replays it.
                        break
            if parser.buffered_bytes and not timed_out:
                raise InformixError("CDC read ended with an incomplete native frame")
            return records
        finally:
            cleanup_errors = []
            for native in captures:
                try:
                    _expect_zero(
                        self.transport.execute(
                            f"EXECUTE FUNCTION {cdc_routine('cdc_endcapture')}(?, 0, ?)",
                            (session, native),
                        ),
                        "cdc_endcapture",
                    )
                except Exception as error:  # preserve an active CDC failure
                    cleanup_errors.append(error)
            try:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)", (session,)
                    ),
                    "cdc_closesess",
                )
            except Exception as error:  # preserve an active CDC failure
                cleanup_errors.append(error)
            if set_socket_timeout is not None and previous_socket_timeout is not None:
                try:
                    set_socket_timeout(float(previous_socket_timeout))
                except Exception as error:
                    cleanup_errors.append(error)
            active_error = sys.exc_info()[1]
            if cleanup_errors:
                if active_error is None:
                    raise InformixError(
                        "Informix CDC session cleanup failed"
                    ) from cleanup_errors[0]
                if hasattr(active_error, "add_note"):
                    for error in cleanup_errors:
                        active_error.add_note(f"Informix CDC cleanup also failed: {error}")


_bridge_factory: Callable[[dict[str, str]], InformixBridge] = PurePythonInformixBridge


def set_bridge_factory(factory: Callable[[dict[str, str]], InformixBridge]) -> None:
    """Set the process-wide bridge factory (primarily for deterministic tests)."""

    global _bridge_factory
    _bridge_factory = factory


def _bridge_config(options: dict[str, str]) -> dict[str, Any]:
    required = ("hostname", "database", "user", "password", "server")
    missing = [name for name in required if not options.get(name)]
    if missing:
        raise ValueError(f"Missing required Informix option(s): {', '.join(missing)}")
    return {
        "hostname": options["hostname"],
        "port": int(options.get("port", "9088")),
        "database": options["database"],
        "user": options["user"],
        "password": options["password"],
        "server": options.get("server"),
        "db_locale": options.get("DB_LOCALE") or options.get("db.locale") or "en_US.819",
        "client_locale": (
            options.get("CLIENT_LOCALE") or options.get("client.locale") or "en_US.utf8"
        ),
        "tls": _option_bool(options, "encrypt", True),
        "ca_file": options.get("ssl.ca.file"),
        "pad_varchar": _option_bool(options, "padVarchar", False),
        "cdc_timeout": int(options.get("cdc.timeout", "5")),
        "cdc_max_records": int(options.get("cdc.max.records", "64")),
        "stop_logging_on_close": False,
    }


def _option_bool(options: dict[str, str], name: str, default: bool) -> bool:
    value = options.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError(
        f"Option '{name}' must be one of: 1, true, yes, 0, false, no"
    )


def _shared_state_location(options: dict[str, str]) -> str:
    location = options.get("cdc.shared.state.location", "").strip()
    if not location:
        raise ValueError("Missing required Informix option: cdc.shared.state.location")
    if not os.path.isabs(location):
        raise ValueError("Option 'cdc.shared.state.location' must be an absolute path")
    normalized = os.path.normpath(location)
    if normalized != location.rstrip("/") or any(
        part in {".", ".."} for part in location.split("/")
    ):
        raise ValueError("Option 'cdc.shared.state.location' must not contain traversal")
    if options.get("hostname"):
        parts = normalized.split("/")
        if len(parts) < 5 or parts[1] != "Volumes" or any(not part for part in parts[2:5]):
            raise ValueError(
                "Option 'cdc.shared.state.location' must be a Unity Catalog Volume path under "
                "/Volumes/<catalog>/<schema>/<volume>"
            )
    return normalized


def _validate_shared_state_filesystem(location: str) -> None:
    """Probe the filesystem primitives required for cross-reader coordination once."""

    if location in _VALIDATED_STATE_LOCATIONS:
        return
    _makedirs_durable(location, location)
    _validate_state_path(location, location)
    _cleanup_probe_artifacts(location)
    probe_name = f".informix-probe-{secrets.token_hex(8)}"
    root_descriptor: int | None = None
    probe_descriptor: int | None = None
    renamed_descriptor: int | None = None
    occupied_descriptor: int | None = None
    try:
        root_descriptor = _open_state_directory(location, location)
        os.mkdir(probe_name, mode=0o700, dir_fd=root_descriptor)
        probe_descriptor = os.open(
            probe_name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=root_descriptor,
        )
        barrier = threading.Barrier(8, timeout=5)
        winners: list[int] = []
        failures: list[BaseException] = []

        def compete(index: int) -> None:
            try:
                barrier.wait()
                os.mkdir("exclusive", mode=0o700, dir_fd=probe_descriptor)
                winners.append(index)
            except FileExistsError:
                return
            except BaseException as error:  # surfaced on the caller thread
                failures.append(error)

        threads = [
            threading.Thread(target=compete, args=(index,), daemon=True)
            for index in range(8)
        ]
        started: list[threading.Thread] = []
        try:
            for thread in threads:
                thread.start()
                started.append(thread)
        except RuntimeError as error:
            barrier.abort()
            failures.append(error)
        for thread in started:
            thread.join(timeout=6)
        if any(thread.is_alive() for thread in started):
            barrier.abort()
            raise InformixError("Informix shared-state filesystem probe timed out")
        if failures or len(winners) != 1:
            raise InformixError(
                "cdc.shared.state.location does not provide exclusive directory creation"
            ) from (failures[0] if failures else None)
        os.rename(
            "exclusive", "renamed",
            src_dir_fd=probe_descriptor, dst_dir_fd=probe_descriptor,
        )
        names = os.listdir(probe_descriptor)
        if "exclusive" in names or "renamed" not in names:
            raise InformixError(
                "cdc.shared.state.location does not provide atomic directory rename"
            )
        renamed_descriptor = os.open(
            "renamed",
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=probe_descriptor,
        )
        record_descriptor = os.open(
            "record.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            mode=0o600, dir_fd=renamed_descriptor,
        )
        with os.fdopen(record_descriptor, "w", encoding="utf-8") as handle:
            handle.write("winner")
            handle.flush()
            os.fsync(handle.fileno())
        with os.scandir(renamed_descriptor) as entries:
            if [entry.name for entry in entries] != ["record.json"]:
                raise InformixError("Descriptor-based directory scan is unavailable")
        os.close(renamed_descriptor)
        renamed_descriptor = None
        os.mkdir("occupied", mode=0o700, dir_fd=probe_descriptor)
        occupied_descriptor = os.open(
            "occupied",
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=probe_descriptor,
        )
        loser_descriptor = os.open(
            "record.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            mode=0o600, dir_fd=occupied_descriptor,
        )
        with os.fdopen(loser_descriptor, "w", encoding="utf-8") as handle:
            handle.write("loser")
            handle.flush()
            os.fsync(handle.fileno())
        os.close(occupied_descriptor)
        occupied_descriptor = None
        try:
            os.rename(
                "occupied", "renamed",
                src_dir_fd=probe_descriptor, dst_dir_fd=probe_descriptor,
            )
        except OSError as error:
            if error.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                raise
        else:
            raise InformixError(
                "cdc.shared.state.location permits replacing a populated immutable head"
            )
        renamed_descriptor = os.open(
            "renamed",
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=probe_descriptor,
        )
        winner_descriptor = os.open("record.json", os.O_RDONLY, dir_fd=renamed_descriptor)
        with os.fdopen(winner_descriptor, encoding="utf-8") as handle:
            if handle.read() != "winner":
                raise InformixError("Immutable-head filesystem probe replaced its winner")
        os.close(renamed_descriptor)
        renamed_descriptor = None
        os.fsync(probe_descriptor)
        # A duplicate concurrent probe is harmless because every probe uses a
        # unique directory. Avoid retaining a process lock in the generated
        # source closure: Spark must pickle the DataSource class for workers.
        _VALIDATED_STATE_LOCATIONS.add(location)
    except OSError as error:
        raise InformixError(
            f"Cannot validate Informix shared-state filesystem at '{location}'"
        ) from error
    finally:
        if renamed_descriptor is not None:
            os.close(renamed_descriptor)
        if occupied_descriptor is not None:
            os.close(occupied_descriptor)
        if probe_descriptor is not None:
            for name in ("renamed", "occupied", "exclusive"):
                try:
                    _remove_candidate_tree_at(probe_descriptor, name)
                    os.rmdir(name, dir_fd=probe_descriptor)
                except OSError:
                    pass
            os.close(probe_descriptor)
        if root_descriptor is not None:
            try:
                os.rmdir(probe_name, dir_fd=root_descriptor)
            except OSError:
                pass
            os.close(root_descriptor)


def _cleanup_probe_artifacts(location: str) -> None:
    cutoff = time.time() - _ARTIFACT_RETENTION_SECONDS
    try:
        descriptor = _open_state_directory(location, location)
    except FileNotFoundError:
        return
    except OSError as error:
        raise InformixError(
            f"Cannot inspect Informix shared-state location '{location}'"
        ) from error
    try:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                try:
                    eligible = (
                        entry.name.startswith(".informix-probe-")
                        and entry.is_dir(follow_symlinks=False)
                        and entry.stat(follow_symlinks=False).st_mtime <= cutoff
                    )
                except FileNotFoundError:
                    continue
                if not eligible:
                    continue
                try:
                    _remove_candidate_tree_at(descriptor, entry.name)
                    os.rmdir(entry.name, dir_fd=descriptor)
                except FileNotFoundError:
                    continue
                except OSError as error:
                    raise InformixError(
                        f"Cannot clean abandoned Informix probe '{entry.name}'"
                    ) from error
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validated_volume_state_location(state_location: str) -> str:
    location = os.path.normpath(state_location)
    if location != state_location.rstrip("/") or not os.path.isabs(location):
        raise ValueError("Shared-state location must be a canonical absolute path")
    parts = location.split("/")
    if len(parts) < 5 or parts[1] != "Volumes" or any(not part for part in parts[2:5]):
        raise ValueError(
            "Shared-state location must be under /Volumes/<catalog>/<schema>/<volume>"
        )
    cursor = "/Volumes"
    for part in parts[2:]:
        cursor = os.path.join(cursor, part)
        try:
            if stat.S_ISLNK(os.lstat(cursor).st_mode):
                raise ValueError(
                    f"Shared-state location must not traverse symlink '{cursor}'"
                )
        except FileNotFoundError:
            break
        except OSError as error:
            raise InformixError(
                f"Cannot inspect shared-state path component '{cursor}'"
            ) from error
    try:
        metadata = os.lstat(location)
    except FileNotFoundError as error:
        raise ValueError(f"Shared-state location does not exist: '{location}'") from error
    except OSError as error:
        raise InformixError(f"Cannot inspect shared-state location '{location}'") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"Shared-state location must be an existing directory: '{location}'")
    return location


def _remove_candidate_tree_at(parent_descriptor: int, name: str) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = os.open(name, flags, dir_fd=parent_descriptor)
    stack: list[tuple[int, os.ScandirIterator, str | None]] = [
        (root, os.scandir(root), None)
    ]
    try:
        while stack:
            descriptor, entries, child_name = stack[-1]
            try:
                entry = next(entries)
            except StopIteration:
                entries.close()
                os.close(descriptor)
                stack.pop()
                if stack and child_name is not None:
                    os.rmdir(child_name, dir_fd=stack[-1][0])
                continue
            metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                child = os.open(entry.name, flags, dir_fd=descriptor)
                stack.append((child, os.scandir(child), entry.name))
            else:
                os.unlink(entry.name, dir_fd=descriptor)
    finally:
        while stack:
            descriptor, entries, _ = stack.pop()
            entries.close()
            os.close(descriptor)


def _committed_head_exists_at(namespace_descriptor: int) -> bool:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        head = os.open("head", flags, dir_fd=namespace_descriptor)
    except OSError as error:
        if error.errno not in {
            errno.ENOENT,
            errno.ENOTDIR,
            errno.ELOOP,
        }:
            raise
        return False
    try:
        try:
            record = os.open(
                "record.json",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=head,
            )
        except OSError as error:
            if error.errno in {errno.ENOENT, errno.ELOOP}:
                return False
            raise
        try:
            return stat.S_ISREG(os.fstat(record).st_mode)
        finally:
            os.close(record)
    finally:
        os.close(head)


def _cleanup_immutable_candidates(
    location: str, *, headless_cutoff: float, strict: bool = False
) -> int:
    _validate_state_path(location, location)
    removed = 0
    first_traversal_error: OSError | None = None
    first_removal_error: BaseException | None = None
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = _open_state_directory(location, location)
    try:
        root_entries = os.scandir(root)
    except BaseException:
        os.close(root)
        raise
    stack: list[tuple[int, os.ScandirIterator, str]] = [(root, root_entries, location)]
    try:
        while stack:
            directory_descriptor, entries, directory = stack[-1]
            try:
                entry = next(entries)
            except StopIteration:
                entries.close()
                os.close(directory_descriptor)
                stack.pop()
                continue
            except OSError as error:
                if first_traversal_error is None:
                    first_traversal_error = error
                logging.getLogger(__name__).warning(
                    "Cannot inspect Informix shared-state path: path=%s", directory
                )
                entries.close()
                os.close(directory_descriptor)
                stack.pop()
                continue
            name = entry.name
            quarantined = bool(re.fullmatch(r"\.candidate-gc-[0-9a-f]{32}", name))
            candidate = quarantined or bool(
                re.fullmatch(r"candidate-[0-9a-f]{16,64}", name)
            )
            try:
                metadata = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            except OSError as error:
                if first_traversal_error is None:
                    first_traversal_error = error
                logging.getLogger(__name__).warning(
                    "Cannot inspect Informix shared-state path: path=%s",
                    os.path.join(directory, name),
                )
                continue
            if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                continue
            if not candidate:
                try:
                    child = os.open(name, flags, dir_fd=directory_descriptor)
                    try:
                        child_entries = os.scandir(child)
                    except BaseException:
                        os.close(child)
                        raise
                    stack.append(
                        (child, child_entries, os.path.join(directory, name))
                    )
                except FileNotFoundError:
                    continue
                except OSError as error:
                    if first_traversal_error is None:
                        first_traversal_error = error
                    logging.getLogger(__name__).warning(
                        "Cannot inspect Informix shared-state path: path=%s",
                        os.path.join(directory, name),
                    )
                continue
            path = os.path.join(directory, name)
            try:
                cleanup_name = name
                if not quarantined:
                    committed = _committed_head_exists_at(directory_descriptor)
                    if not committed and metadata.st_mtime > headless_cutoff:
                        continue
                    cleanup_name = f".candidate-gc-{secrets.token_hex(16)}"
                    try:
                        os.rename(
                            name,
                            cleanup_name,
                            src_dir_fd=directory_descriptor,
                            dst_dir_fd=directory_descriptor,
                        )
                    except FileNotFoundError:
                        continue
                _remove_candidate_tree_at(directory_descriptor, cleanup_name)
                os.rmdir(cleanup_name, dir_fd=directory_descriptor)
                removed += 1
            except FileNotFoundError:
                continue
            except Exception as error:
                if first_removal_error is None:
                    first_removal_error = error
                logging.getLogger(__name__).warning(
                    "Cannot remove abandoned Informix candidate: path=%s",
                    path,
                    exc_info=True,
                )
    finally:
        while stack:
            descriptor, entries, _ = stack.pop()
            entries.close()
            os.close(descriptor)
    if strict and (first_traversal_error is not None or first_removal_error is not None):
        error = first_traversal_error or first_removal_error
        raise InformixError(
            f"Informix candidate cleanup left inaccessible artifacts under '{location}'"
        ) from error
    return removed


def cleanup_abandoned_immutable_candidates(
    state_location: str, *, acknowledge_pipelines_stopped: bool
) -> int:
    """Remove all uncommitted candidates after every pipeline is stopped."""

    if not acknowledge_pipelines_stopped:
        raise ValueError(
            "Candidate cleanup requires acknowledgement that all pipelines are stopped"
        )
    location = _validated_volume_state_location(state_location)
    removed = _cleanup_immutable_candidates(
        location, headless_cutoff=float("inf"), strict=True
    )
    root_descriptor = _open_state_directory(location, location)
    try:
        try:
            _remove_candidate_tree_at(root_descriptor, ".informix-candidate-cleanup")
            os.rmdir(".informix-candidate-cleanup", dir_fd=root_descriptor)
            os.fsync(root_descriptor)
        except FileNotFoundError:
            pass
    finally:
        os.close(root_descriptor)
    return removed


def _candidate_cleanup_completion_path(state_location: str, bucket: int) -> str:
    return os.path.join(
        state_location,
        ".informix-candidate-cleanup",
        str(bucket),
        "head",
        "record.json",
    )


def _candidate_cleanup_election_path(state_location: str, bucket: int) -> str:
    return os.path.join(
        state_location,
        ".informix-candidate-cleanup",
        str(bucket),
        "election",
        "head",
        "record.json",
    )


def _validate_state_path(root: str, path: str) -> None:
    if os.path.commonpath((root, path)) != root:
        raise InformixError(f"Informix shared-state path escapes '{root}': '{path}'")
    cursor = root
    relative = os.path.relpath(path, root)
    parts = () if relative == "." else tuple(relative.split(os.sep))
    for index, part in enumerate(("", *parts)):
        if index:
            cursor = os.path.join(cursor, part)
        try:
            metadata = os.lstat(cursor)
        except FileNotFoundError:
            break
        if stat.S_ISLNK(metadata.st_mode):
            raise InformixError(f"Informix shared-state path traverses symlink '{cursor}'")
        if index < len(parts) and not stat.S_ISDIR(metadata.st_mode):
            raise InformixError(f"Informix shared-state ancestor is not a directory: '{cursor}'")


def _fsync_directory_path(path: str) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as error:
        if error.errno in {errno.EINVAL, errno.ENOTSUP, errno.EACCES}:
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                raise
    finally:
        os.close(descriptor)


def _open_state_directory(root: str, path: str) -> int:
    """Open a directory beneath root without following a replaceable symlink."""

    if os.path.commonpath((root, path)) != root:
        raise InformixError(f"Informix shared-state path escapes '{root}': '{path}'")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(root, flags)
    try:
        relative = os.path.relpath(path, root)
        for part in (() if relative == "." else relative.split(os.sep)):
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except OSError as error:
                if error.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise InformixError(
                        f"Informix shared-state path traverses symlink or non-directory: '{path}'"
                    ) from error
                raise
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_state_file(root: str, path: str) -> int:
    parent = os.path.dirname(path)
    directory = _open_state_directory(root, parent)
    try:
        try:
            return os.open(
                os.path.basename(path),
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory,
            )
        except OSError as error:
            if error.errno == errno.ELOOP:
                raise InformixError(
                    f"Informix shared-state path traverses symlink '{path}'"
                ) from error
            raise
    finally:
        os.close(directory)


def _makedirs_durable(root: str, path: str) -> None:
    if os.path.commonpath((root, path)) != root:
        raise InformixError(f"Informix shared-state path escapes '{root}': '{path}'")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(root, flags)
    except FileNotFoundError:
        parts = root.split(os.sep)
        if len(parts) >= 5 and parts[1] == "Volumes":
            anchor = os.path.join(os.sep, *parts[1:5])
        else:
            anchor = os.path.dirname(root)
            while not os.path.isdir(anchor):
                parent = os.path.dirname(anchor)
                if parent == anchor:
                    raise InformixError(
                        f"Cannot find an existing parent for shared-state root '{root}'"
                    )
                anchor = parent
        descriptor = os.open(anchor, flags)
        root_parts = tuple(
            part for part in os.path.relpath(root, anchor).split(os.sep) if part != "."
        )
    else:
        root_parts = ()
    path_parts = tuple(
        part for part in os.path.relpath(path, root).split(os.sep) if part != "."
    )
    try:
        for part in (*root_parts, *path_parts):
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
                os.fsync(descriptor)
                child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_cleanup_marker(
    state_location: str, path: str, record_type: str, bucket: int
) -> dict[str, object] | None:
    try:
        descriptor = _open_state_file(state_location, path)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            os.close(descriptor)
            raise InformixError(f"Invalid Informix cleanup marker '{path}'")
        if metadata.st_size > _MAX_SHARED_STATE_BYTES:
            os.close(descriptor)
            raise InformixError(f"Informix cleanup marker '{path}' is too large")
        with os.fdopen(descriptor, encoding="utf-8") as handle:
            record = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise InformixError(f"Cannot read Informix cleanup marker '{path}'") from error
    if (
        not isinstance(record, dict)
        or not isinstance(record.get("format_version"), int)
        or isinstance(record.get("format_version"), bool)
        or record.get("format_version") != _IMMUTABLE_STATE_VERSION
        or record.get("record_type") != record_type
        or not isinstance(record.get("bucket"), int)
        or isinstance(record.get("bucket"), bool)
        or record.get("bucket") != bucket
        or isinstance(record.get("created_at"), bool)
        or not isinstance(record.get("created_at"), (int, float))
        or not math.isfinite(float(record["created_at"]))
    ):
        raise InformixError(f"Invalid Informix cleanup marker '{path}'")
    return record


def _publish_cleanup_marker(
    state_location: str, path: str, payload: dict[str, object]
) -> bool:
    namespace = os.path.dirname(os.path.dirname(path))
    _makedirs_durable(state_location, namespace)
    candidate_name = f"candidate-{secrets.token_hex(16)}"
    candidate = os.path.join(namespace, candidate_name)
    namespace_descriptor = _open_state_directory(state_location, namespace)
    candidate_descriptor: int | None = None
    won = False
    try:
        os.mkdir(candidate_name, mode=0o700, dir_fd=namespace_descriptor)
        candidate_descriptor = os.open(
            candidate_name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=namespace_descriptor,
        )
        record_descriptor = os.open(
            "record.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            mode=0o600,
            dir_fd=candidate_descriptor,
        )
        with os.fdopen(record_descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.fsync(candidate_descriptor)
        os.close(candidate_descriptor)
        candidate_descriptor = None
        try:
            os.rename(
                candidate_name,
                "head",
                src_dir_fd=namespace_descriptor,
                dst_dir_fd=namespace_descriptor,
            )
            os.fsync(namespace_descriptor)
            won = True
        except OSError as error:
            if error.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                raise
    finally:
        if candidate_descriptor is not None:
            os.close(candidate_descriptor)
        if not won:
            try:
                os.unlink(os.path.join(candidate_name, "record.json"), dir_fd=namespace_descriptor)
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).warning(
                    "Retained abandoned Informix cleanup candidate: path=%s", candidate
                )
            try:
                os.rmdir(candidate_name, dir_fd=namespace_descriptor)
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).warning(
                    "Retained abandoned Informix cleanup candidate: path=%s", candidate
                )
        os.close(namespace_descriptor)
    return won


def _publish_candidate_cleanup_completion(state_location: str, bucket: int) -> None:
    path = _candidate_cleanup_completion_path(state_location, bucket)
    _publish_cleanup_marker(
        state_location,
        path,
        {"format_version": _IMMUTABLE_STATE_VERSION,
         "record_type": "candidate-cleanup-completion", "bucket": bucket,
         "created_at": time.time()},
    )
    if _read_cleanup_marker(
        state_location, path, "candidate-cleanup-completion", bucket
    ) is None:
        raise InformixError(
            f"Informix cleanup completion election has no readable winner: '{path}'"
        )


def _quarantine_invalid_cleanup_marker(state_location: str, path: str) -> None:
    head = os.path.dirname(path)
    namespace = os.path.dirname(head)
    descriptor = _open_state_directory(state_location, namespace)
    try:
        os.rename(
            "head",
            f".invalid-{secrets.token_hex(16)}",
            src_dir_fd=descriptor,
            dst_dir_fd=descriptor,
        )
        os.fsync(descriptor)
    except FileNotFoundError:
        pass
    finally:
        os.close(descriptor)


def _cancel_cleanup_election(state_location: str, path: str) -> None:
    head = os.path.dirname(path)
    namespace = os.path.dirname(head)
    descriptor = _open_state_directory(state_location, namespace)
    try:
        os.rename(
            "head",
            f".cancelled-{secrets.token_hex(16)}",
            src_dir_fd=descriptor,
            dst_dir_fd=descriptor,
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _prune_candidate_cleanup_markers(state_location: str, bucket: int) -> None:
    root = os.path.join(state_location, ".informix-candidate-cleanup")
    _validate_state_path(state_location, root)
    try:
        descriptor = _open_state_directory(state_location, root)
    except FileNotFoundError:
        return
    try:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                name = entry.name
                other = _cleanup_bucket_number(name)
                invalid = name.startswith(".invalid-bucket-")
                if not invalid and (
                    other is None
                    or other > bucket - _CANDIDATE_CLEANUP_MARKER_RETENTION_BUCKETS
                ):
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                        os.unlink(name, dir_fd=descriptor)
                        continue
                    _remove_candidate_tree_at(descriptor, name)
                    os.rmdir(name, dir_fd=descriptor)
                except OSError as error:
                    if error.errno != errno.ENOENT:
                        raise
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _remember_candidate_cleanup(state_location: str, when: float) -> None:
    _LAST_CANDIDATE_CLEANUP.pop(state_location, None)
    _LAST_CANDIDATE_CLEANUP[state_location] = when
    while len(_LAST_CANDIDATE_CLEANUP) > _MAX_CANDIDATE_CLEANUP_THROTTLES:
        oldest = next(iter(_LAST_CANDIDATE_CLEANUP))
        del _LAST_CANDIDATE_CLEANUP[oldest]


def _cleanup_bucket_number(name: str) -> int | None:
    if re.fullmatch(r"[0-9]{1,20}", name) is None:
        return None
    return int(name)


def _unfinished_cleanup_elections(state_location: str, bucket: int) -> tuple[int, ...]:
    root = os.path.join(state_location, ".informix-candidate-cleanup")
    _validate_state_path(state_location, root)
    try:
        descriptor = _open_state_directory(state_location, root)
    except FileNotFoundError:
        return ()
    earliest_unfinished: int | None = None
    try:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                name = entry.name
                other = _cleanup_bucket_number(name)
                if other is None:
                    if name.isdigit():
                        try:
                            os.rename(
                                name,
                                f".invalid-bucket-{secrets.token_hex(16)}",
                                src_dir_fd=descriptor,
                                dst_dir_fd=descriptor,
                            )
                        except FileNotFoundError:
                            pass
                    continue
                if other == bucket:
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                    os.unlink(name, dir_fd=descriptor)
                    continue
                try:
                    election = _read_cleanup_marker(
                        state_location,
                        _candidate_cleanup_election_path(state_location, other),
                        "candidate-cleanup-election",
                        other,
                    )
                    if election is None:
                        continue
                    completion = _read_cleanup_marker(
                        state_location,
                        _candidate_cleanup_completion_path(state_location, other),
                        "candidate-cleanup-completion",
                        other,
                    )
                    if completion is None:
                        earliest_unfinished = (
                            other
                            if earliest_unfinished is None
                            else min(earliest_unfinished, other)
                        )
                except InformixError:
                    try:
                        os.rename(
                            name,
                            f".invalid-bucket-{secrets.token_hex(16)}",
                            src_dir_fd=descriptor,
                            dst_dir_fd=descriptor,
                        )
                    except FileNotFoundError:
                        continue
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return () if earliest_unfinished is None else (earliest_unfinished,)


def _maybe_cleanup_immutable_candidates(state_location: str) -> None:
    _validate_state_path(state_location, state_location)
    now = time.monotonic()
    if (
        _LAST_CANDIDATE_CLEANUP.get(state_location, 0)
        > now - _CANDIDATE_CLEANUP_INTERVAL_SECONDS
    ):
        return
    bucket = int(time.time() // _CANDIDATE_CLEANUP_INTERVAL_SECONDS)
    completion = _candidate_cleanup_completion_path(state_location, bucket)
    try:
        completed = _read_cleanup_marker(
            state_location, completion, "candidate-cleanup-completion", bucket
        )
    except InformixError:
        logging.getLogger(__name__).exception(
            "Invalid Informix candidate-cleanup completion marker: location=%s", state_location
        )
        try:
            _quarantine_invalid_cleanup_marker(state_location, completion)
        except OSError:
            logging.getLogger(__name__).exception(
                "Cannot quarantine invalid Informix cleanup marker: location=%s",
                state_location,
            )
            _remember_candidate_cleanup(state_location, now)
            return
        completed = None
    if completed is not None:
        _remember_candidate_cleanup(state_location, now)
        return
    try:
        if _unfinished_cleanup_elections(state_location, bucket):
            _remember_candidate_cleanup(state_location, now)
            return
        election_path = _candidate_cleanup_election_path(state_location, bucket)
        won = _publish_cleanup_marker(
            state_location,
            election_path,
            {"format_version": _IMMUTABLE_STATE_VERSION,
             "record_type": "candidate-cleanup-election", "bucket": bucket,
             "created_at": time.time()},
        )
        if not won:
            try:
                _read_cleanup_marker(
                    state_location,
                    election_path,
                    "candidate-cleanup-election",
                    bucket,
                )
            except InformixError:
                _quarantine_invalid_cleanup_marker(state_location, election_path)
            _remember_candidate_cleanup(state_location, now)
            return
        unfinished = _unfinished_cleanup_elections(state_location, bucket)
        if unfinished and min(unfinished) < bucket:
            _cancel_cleanup_election(state_location, election_path)
            _remember_candidate_cleanup(state_location, now)
            return
        _cleanup_immutable_candidates(
            state_location,
            headless_cutoff=time.time() - _HEADLESS_CANDIDATE_RETENTION_SECONDS,
        )
        _publish_candidate_cleanup_completion(state_location, bucket)
        _prune_candidate_cleanup_markers(state_location, bucket)
        _remember_candidate_cleanup(state_location, now)
    except Exception:
        _remember_candidate_cleanup(state_location, now)
        logging.getLogger(__name__).exception(
            "Skipping failed Informix candidate cleanup: location=%s", state_location
        )


def _redirect_allowlist(value: str) -> frozenset[tuple[str, int]]:
    result: set[tuple[str, int]] = set()
    for item in filter(None, (part.strip() for part in value.split(","))):
        host, separator, port_text = item.rpartition(":")
        if not separator or not host or not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
            raise ValueError("redirect.allowlist entries must be exact host:numeric-port pairs")
        result.add((host, int(port_text)))
    return frozenset(result)


@dataclass(frozen=True)
class Column:
    name: str
    type_name: str
    nullable: bool = True
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    cdc_supported: bool = True

    @classmethod
    def parse(cls, raw: dict[str, Any]) -> "Column":
        return cls(
            name=str(raw["name"]),
            type_name=str(raw.get("type_name") or raw.get("type") or "VARCHAR").upper(),
            nullable=bool(raw.get("nullable", True)),
            length=_optional_int(raw.get("length")),
            precision=_optional_int(raw.get("precision")),
            scale=_optional_int(raw.get("scale")),
            cdc_supported=bool(raw.get("cdc_supported", True)),
        )


@dataclass(frozen=True)
class Table:
    database: str
    owner: str
    name: str
    columns: tuple[Column, ...]
    primary_keys: tuple[str, ...]
    incarnation: str | None = None

    @property
    def exposed_name(self) -> str:
        return f"{self.owner}.{self.name}"

    @property
    def identity(self) -> str:
        return f"{self.database}.{self.owner}.{self.name}"

    @property
    def native_identity(self) -> str:
        return f"{self.database}:{self.owner}.{self.name}"

    @classmethod
    def parse(cls, raw: dict[str, Any], default_database: str) -> "Table":
        database = str(raw.get("database") or default_database)
        owner = str(raw["owner"])
        name = str(raw["name"])
        for part in (database, owner, name):
            if not _IDENTIFIER.fullmatch(part):
                raise InformixError(f"Unsafe Informix identifier returned by metadata: {part!r}")
        columns = tuple(Column.parse(c) for c in raw.get("columns", ()))
        pks = tuple(str(v) for v in raw.get("primary_keys", ()))
        column_names = tuple(column.name for column in columns)
        for column_name in column_names:
            if not _IDENTIFIER.fullmatch(column_name):
                raise InformixError(
                    f"Unsafe Informix column identifier returned by metadata: {column_name!r}"
                )
        normalized_names = tuple(column_name.casefold() for column_name in column_names)
        if len(normalized_names) != len(set(normalized_names)):
            raise InformixError(f"Duplicate column names for {database}.{owner}.{name}")
        reserved = {column.casefold() for column in _INTERNAL_COLUMNS}
        collisions = sorted(
            column_name for column_name in column_names if column_name.casefold() in reserved
        )
        if collisions:
            raise InformixError(
                f"Source columns collide with reserved Informix metadata columns: {collisions!r}"
            )
        if len(pks) != len(set(pks)):
            raise InformixError(f"Duplicate primary-key columns for {database}.{owner}.{name}")
        known = {c.name for c in columns}
        if not columns or any(pk not in known for pk in pks):
            raise InformixError(f"Invalid metadata for {database}.{owner}.{name}")
        incarnation_value = raw.get("incarnation")
        incarnation = None if incarnation_value is None else str(incarnation_value)
        return cls(database, owner, name, columns, pks, incarnation)


@dataclass
class Transaction:
    tx_id: int
    begin_lsn: int
    records: list[dict[str, Any]] = field(default_factory=list)
    pending_before: dict[str, Any] | None = None
    last_lsn: int = field(init=False)

    def __post_init__(self) -> None:
        self.last_lsn = self.begin_lsn

    def advance(self, record: dict[str, Any]) -> int:
        lsn = _lsn(record)
        if lsn < self.last_lsn:
            raise InformixError(
                f"CDC LSN regressed in transaction {self.tx_id}: {lsn} < {self.last_lsn}"
            )
        self.last_lsn = lsn
        return lsn

    def append(self, record: dict[str, Any]) -> None:
        self.advance(record)
        op = _operation(record)
        if op == "BEFORE_UPDATE":
            if self.pending_before is not None:
                raise InformixError(f"Unpaired BEFORE_UPDATE in transaction {self.tx_id}")
            self.pending_before = record
            return
        if op == "AFTER_UPDATE":
            if self.pending_before is None:
                raise InformixError(
                    f"AFTER_UPDATE without BEFORE_UPDATE in transaction {self.tx_id}"
                )
            merged = dict(record)
            merged["op"] = "UPDATE"
            merged["before"] = self.pending_before.get("before", self.pending_before.get("row"))
            merged["after"] = record.get("after", record.get("row"))
            self.pending_before = None
            self.records.append(merged)
            return
        self.records.append(record)

    def discard(self, lsn: int) -> None:
        # DISCARD carries the rollback cutoff, not the forward position of the
        # DISCARD control record. It may legitimately precede the latest data LSN.
        self.records = [r for r in self.records if _lsn(r) < lsn]
        if self.pending_before is not None and _lsn(self.pending_before) >= lsn:
            self.pending_before = None


@dataclass(frozen=True)
class CommittedTransaction:
    tx_id: int
    begin_lsn: int
    commit_lsn: int
    restart_lsn: int
    records: tuple[dict[str, Any], ...]


class TransactionBuffer:
    """Debezium-compatible buffering for interleaved Informix transactions."""

    def __init__(self) -> None:
        self.open: dict[int, Transaction] = {}

    def feed(self, raw: dict[str, Any]) -> CommittedTransaction | None:
        record = _normalise_record(raw)
        op = _operation(record)
        if op in {"TIMEOUT", "METADATA"}:
            return None
        if op == "ERROR":
            detail = record.get("message") or record.get("payload") or ""
            raise InformixError(
                f"Informix CDC error {record.get('error', 'unknown')} "
                f"with flags {record.get('flags', 'unknown')}: {detail}"
            )
        tx_id = _tx_id(record)
        if op == "BEGIN":
            if tx_id in self.open:
                raise InformixError(f"Duplicate CDC BEGIN for transaction {tx_id}")
            self.open[tx_id] = Transaction(tx_id, _lsn(record))
            return None
        tx = self.open.get(tx_id)
        if tx is None:
            raise InformixError(f"CDC {op} for unknown transaction {tx_id}")
        if op in _DATA_OPS:
            tx.append(record)
            return None
        if op == "DISCARD":
            tx.discard(_lsn(record))
            return None
        if op == "ROLLBACK":
            tx.advance(record)
            del self.open[tx_id]
            return None
        if op != "COMMIT":
            raise InformixError(f"Unknown Informix CDC operation {op!r}")
        if tx.pending_before is not None:
            raise InformixError(f"Transaction {tx_id} committed with unpaired BEFORE_UPDATE")
        end = tx.advance(record)
        del self.open[tx_id]
        restart = min((item.begin_lsn for item in self.open.values()), default=end)
        return CommittedTransaction(tx_id, tx.begin_lsn, end, restart, tuple(tx.records))


class InformixLakeflowConnect(LakeflowConnect):
    """Pure-Python connector live-validated on disposable Informix 15.

    Normal auth, queries, discovery, snapshots, transactional CDC, and
    serverless Lakeflow pipeline execution have been exercised.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._shared_state_location = _shared_state_location(options)
        # Validate numeric configuration without opening a connection.
        for name, default, minimum in (
            ("snapshot.page.size", str(_DEFAULT_SNAPSHOT_PAGE_SIZE), 1),
            ("snapshot.max.rows", "100000", 1),
            ("snapshot.max.bytes", str(256 << 20), 0),
            ("metadata.max.bytes", str(64 << 20), 0),
            ("max.records.per.batch", str(_DEFAULT_MAX_RECORDS_PER_BATCH), 1),
            ("cdc.timeout", "5", 1),
            ("cdc.max.records", "64", 1),
            ("cdc.max.frame.bytes", str(16 << 20), 16),
            ("cdc.max.transaction.records", "100000", 1),
            ("cdc.max.poll.records", "200000", 1),
            ("cdc.max.poll.bytes", "0", 0),
            ("cdc.read.bytes", "32000", 1),
            ("authentication.pam.max.rounds", "16", 1),
            ("redirect.max", "3", 0),
            ("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS), 1),
        ):
            if int(options.get(name, default)) < minimum:
                raise ValueError(f"Option '{name}' must be >= {minimum}")
        if int(options.get("cdc.max.records", "64")) > 256:
            raise ValueError("Option 'cdc.max.records' must be <= 256")
        if int(options.get("cdc.read.bytes", "32000")) > 32767:
            raise ValueError("Option 'cdc.read.bytes' must be <= 32767")
        port = int(options.get("port", "9088"))
        if not 1 <= port <= 65535:
            raise ValueError("Option 'port' must be between 1 and 65535")
        login_timeout = float(options.get("authentication.login.timeout", "30"))
        if not math.isfinite(login_timeout) or login_timeout <= 0:
            raise ValueError("Option 'authentication.login.timeout' must be > 0")
        self._bridge_instance: InformixBridge | None = None
        self._tables: dict[str, Table] | None = None
        self._snapshot_high_water: dict[str, int] = {}
        self._snapshot_schema_ids: dict[str, str] = {}
        self._trigger_available_now = False
        self._trigger_boundaries: dict[str, tuple[int, str]] = {}
        self._registration_scope: str | None = None

    def set_registration_scope(self, scope: str) -> None:
        """Install the UUID shared by every reader serialized from one registration."""

        if not re.fullmatch(r"[0-9a-f]{32}", scope):
            raise ValueError("Informix registration scope must be a 32-character UUID")
        self._registration_scope = scope

    def _pipeline_scope(self, checkpoint: dict[str, Any] | None = None) -> str:
        scope = checkpoint.get("pipeline_scope") if checkpoint else None
        scope = scope or self._registration_scope
        if not isinstance(scope, str) or not re.fullmatch(r"[0-9a-f]{32}", scope):
            raise InformixError("Informix reader has no registration or checkpoint scope")
        return scope

    def prepare_for_trigger_available_now(self) -> None:
        """Freeze stream high-water marks when Spark selects AvailableNow."""

        self._trigger_available_now = True
        self._trigger_boundaries.clear()

    def close(self) -> None:
        """Close the live SQLI transport, if one was opened."""

        if self._bridge_instance is not None:
            transport = getattr(self._bridge_instance, "transport", None)
            close = getattr(transport, "close", None)
            if close is not None:
                close()
            self._bridge_instance = None

    def __enter__(self) -> "InformixLakeflowConnect":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_value is None:
            self.close()
            return
        try:
            self.close()
        except Exception as close_error:
            if hasattr(exc_value, "add_note"):
                exc_value.add_note(f"Informix cleanup also failed: {close_error}")

    def __getstate__(self) -> dict[str, Any]:
        """Exclude live SQLI state when Spark serializes the data source.

        Schema and metadata discovery run before Spark ships the reader to a
        Python worker, so ``_bridge_instance`` can contain a socket, buffered
        streams, and a thread lock.  None of those objects is transferable or
        valid in another process.  The worker reconstructs a fresh bridge from
        the immutable connection options on its first source operation.
        """

        state = self.__dict__.copy()
        state["_bridge_instance"] = None
        return state

    @property
    def _bridge(self) -> InformixBridge:
        if self._bridge_instance is None:
            factory_path = self.options.get("bridge.factory")
            factory = _load_factory(factory_path) if factory_path else _bridge_factory
            self._bridge_instance = factory(self.options)
        return self._bridge_instance

    def list_tables(self) -> list[str]:
        return sorted(self._table_map())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        table = self._table(table_name, table_options, refresh=True)
        _ensure_materializable(table)
        fields = [
            StructField(column.name, _spark_type(column), column.nullable)
            for column in table.columns
        ]
        fields.extend(
            (
                StructField(CURSOR, StringType(), False),
                StructField(COMMIT_LSN, StringType(), False),
                StructField(TX_ID, LongType(), True),
                StructField(OP, StringType(), False),
            )
        )
        return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        table = self._table(table_name, table_options, refresh=True)
        _ensure_materializable(table)
        if not _cdc_capable(table):
            return {"primary_keys": [], "cursor_field": None, "ingestion_type": "snapshot"}
        return {
            "primary_keys": list(table.primary_keys),
            "cursor_field": CURSOR,
            "ingestion_type": "cdc_with_deletes",
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        table = self._table(table_name, table_options)
        _ensure_materializable(table)
        if not _cdc_capable(table):
            return self._read_snapshot_only(table, start_offset, table_options)
        if not start_offset or start_offset.get("phase", "snapshot") == "snapshot":
            return self._read_snapshot(table, start_offset, table_options)
        return self._read_stream(table, start_offset, table_options, deletes=False)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        table = self._table(table_name, table_options)
        _ensure_materializable(table)
        if not _cdc_capable(table):
            raise ValueError(
                f"Table '{table_name}' lacks a primary key or has columns unsupported "
                "by Informix CDC and is snapshot-only"
            )
        # Lakeflow checkpoints this independently from read_table(). The
        # upsert reader owns initialization and publishes the table boundary
        # through durable shared state; delete readers only consume it.
        if not start_offset:
            high_water = self._initial_lsn(table, owner=False)
            return iter(()), _offset(
                high_water,
                high_water,
                high_water,
                None,
                "stream",
                table,
                self._snapshot_schema_ids[table.identity],
                self._pipeline_scope(),
            )
        if start_offset.get("phase") == "snapshot":
            snapshot_checkpoint = _validated_offset(start_offset)
            expected = snapshot_checkpoint.get("schema_fingerprint")
            if expected is None:
                raise InformixError(
                    f"Informix delete checkpoint for '{table.exposed_name}' predates "
                    "schema-safe offsets; run a full refresh"
                )
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

    def _read_snapshot(self, table: Table, start: dict | None, options: dict[str, str]):
        _maybe_cleanup_immutable_candidates(self._shared_state_location)
        checkpoint = _validated_offset(start) if start else None
        pipeline_scope = self._pipeline_scope(checkpoint)
        if checkpoint and checkpoint.get("schema_fingerprint") is None:
            raise InformixError(
                f"Informix snapshot checkpoint for '{table.exposed_name}' predates "
                "schema-safe offsets; run a full refresh"
            )
        expected_fingerprint = checkpoint.get("schema_fingerprint") if checkpoint else None
        table = self._refresh_table_schema(table, expected_fingerprint)
        if not _cdc_capable(table):
            raise InformixError(
                f"Table '{table.exposed_name}' is no longer CDC-capable after metadata refresh; "
                "run a full refresh after restoring its primary key and supported schema"
            )
        page_size = self._table_int_option(
            options, "snapshot.page.size", _DEFAULT_SNAPSHOT_PAGE_SIZE, minimum=1
        )
        if checkpoint:
            high_water = int(checkpoint["snapshot_lsn"])
            schema_id = str(checkpoint["schema_id"])
            last_pk = checkpoint["snapshot"]["last_pk"]
        else:
            high_water = self._initial_lsn(table)
            schema_id = self._snapshot_schema_ids[table.identity]
            last_pk = None
            consistent_snapshot = getattr(self._bridge, "consistent_snapshot", None)
            if consistent_snapshot is not None:
                max_rows = self._table_int_option(
                    options, "snapshot.max.rows", 100000, minimum=1
                )
                snapshot_lsn, rows = consistent_snapshot(
                    table.identity,
                    [c.name for c in table.columns],
                    table.primary_keys,
                    page_size,
                    max_rows,
                    self._table_int_option(
                        options, "snapshot.max.bytes", 256 << 20, minimum=0
                    ),
                )
                table = self._refresh_table_schema(table, _schema_fingerprint(table))
                self._publish_snapshot_boundary(
                    table, schema_id, high_water, snapshot_lsn, pipeline_scope
                )
                return iter(_shape_snapshot(row, snapshot_lsn) for row in rows), _offset(
                    snapshot_lsn,
                    snapshot_lsn,
                    snapshot_lsn,
                    None,
                    "stream",
                    table,
                    schema_id,
                    pipeline_scope,
                )
            self._publish_snapshot_boundary(
                table, schema_id, high_water, high_water, pipeline_scope
            )
        rows = self._bridge.snapshot_page(
            table.identity,
            [c.name for c in table.columns],
            table.primary_keys,
            last_pk,
            page_size + 1,
        )
        table = self._refresh_table_schema(table, _schema_fingerprint(table))
        page, has_more = rows[:page_size], len(rows) > page_size
        shaped = [_shape_snapshot(row, high_water) for row in page]
        if has_more:
            if not page:
                raise InformixError("Snapshot bridge returned an invalid empty continuation page")
            last = [page[-1][pk] for pk in table.primary_keys]
            end = _offset(
                high_water,
                high_water,
                high_water,
                None,
                "snapshot",
                table,
                schema_id,
                pipeline_scope,
            )
            end.update({"snapshot_lsn": str(high_water), "snapshot": {"last_pk": last}})
        else:
            end = _offset(
                high_water,
                high_water,
                high_water,
                None,
                "stream",
                table,
                schema_id,
                pipeline_scope,
            )
        return iter(shaped), end

    def _read_snapshot_only(self, table: Table, start: dict | None, options: dict[str, str]):
        _maybe_cleanup_immutable_candidates(self._shared_state_location)
        _ensure_materializable(table)
        table = self._refresh_table_schema(table, None)
        fingerprint = _schema_fingerprint(table)
        # PK-less tables cannot be seek-paginated safely.  Read exactly once;
        # returning None signals non-checkpointable full refresh semantics.
        limit = self._table_int_option(options, "snapshot.max.rows", 100000, minimum=1)
        rows = self._bridge.snapshot_page(
            table.identity, [c.name for c in table.columns], (), None, limit + 1
        )
        table = self._refresh_table_schema(table, fingerprint)
        if len(rows) > limit:
            raise InformixError(
                f"Snapshot-only table {table.exposed_name} exceeds snapshot.max.rows={limit}"
            )
        lsn = self._bridge.current_lsn()
        return iter(_shape_snapshot(row, lsn) for row in rows), None

    def _read_stream(self, table: Table, start: dict, options: dict[str, str], deletes: bool):
        _maybe_cleanup_immutable_candidates(self._shared_state_location)
        checkpoint = _validated_offset(start)
        pipeline_scope = self._pipeline_scope(checkpoint)
        table = self._refresh_table_schema(table, None)
        fingerprint = _schema_fingerprint(table)
        checkpoint_fingerprint = checkpoint.get("schema_fingerprint")
        checkpoint_schema_id = str(checkpoint["schema_id"])
        if checkpoint_fingerprint is None:
            raise InformixError(
                f"Informix checkpoint for '{table.exposed_name}' predates schema-safe offsets; "
                "run a full refresh before resuming CDC"
            )
        restart = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        minimum = self._bridge.minimum_lsn()
        if restart < minimum:
            raise LogRetentionError(
                f"Restart LSN {restart} is older than minimum retained LSN "
                f"{minimum}; resnapshot required"
            )
        capture_table = table
        transition_table = table
        transition_lsn: int | None = None
        capture_schema_id = checkpoint_schema_id
        transition_schema_id = checkpoint_schema_id
        if checkpoint_fingerprint != fingerprint:
            (
                capture_table,
                transition_table,
                transition_lsn,
                transition_schema_id,
            ) = self._schema_transition(
                table,
                checkpoint_schema_id,
                int(checkpoint["commit_lsn"]),
                owner=not deletes,
            )
        else:
            self._record_current_schema(
                table,
                int(checkpoint["commit_lsn"]),
                checkpoint_schema_id,
                str(checkpoint["schema_fingerprint"]),
                pipeline_scope,
                owner=not deletes,
            )
        max_rows = self._table_int_option(
            options, "max.records.per.batch", _DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        stop_lsn: int | None = None
        trigger_high_water: int | None = None
        trigger_generation: str | None = None
        if self._trigger_available_now:
            stop_lsn, trigger_generation = self._shared_trigger_boundary(
                table, checkpoint, owner=not deletes
            )
            trigger_high_water = stop_lsn
        if transition_lsn is not None:
            stop_lsn = transition_lsn if stop_lsn is None else min(stop_lsn, transition_lsn)
        raw_records = self._bridge.read_changes(
            [_capture_descriptor(capture_table, _client_encoding(self.options))],
            restart,
            self._table_int_option(options, "cdc.timeout", 5, minimum=1),
            self._table_int_option(options, "cdc.max.records", 64, minimum=1, maximum=256),
        )
        table = self._refresh_table_schema(table, fingerprint)
        committed, caught_up, open_begin = _transaction_batch(raw_records)
        recovered = _recover(committed, checkpoint)
        output: list[dict[str, Any]] = []
        end = start
        consumed = 0
        crossed_transition = False
        crossed_trigger_boundary = False
        for tx in recovered:
            if transition_lsn is not None and tx.commit_lsn > transition_lsn:
                if tx.begin_lsn < transition_lsn:
                    raise InformixError(
                        f"Transaction {tx.tx_id} spans schema transition LSN "
                        f"{transition_lsn} for '{table.exposed_name}'; keep source writes "
                        "quiesced until schema transition completes or run a full refresh"
                    )
                crossed_transition = True
                break
            if stop_lsn is not None and tx.commit_lsn > stop_lsn:
                crossed_trigger_boundary = True
                break
            projected = _project_transaction(tx, table, deletes)
            # Never split a transaction.  A single large transaction is
            # accepted; subsequent complete transactions wait for next poll.
            if output and len(output) + len(projected) > max_rows:
                break
            output.extend(projected)
            consumed += 1
            end = _offset(
                tx.commit_lsn,
                tx.commit_lsn,
                tx.restart_lsn,
                tx.tx_id,
                "stream",
                capture_table,
                capture_schema_id,
                pipeline_scope,
                trigger_generation=checkpoint.get("trigger_generation"),
            )
            if len(output) >= max_rows:
                break
        if (
            transition_lsn is not None
            and crossed_transition
            and open_begin is not None
            and open_begin < transition_lsn
        ):
            raise InformixError(
                f"An open transaction beginning at LSN {open_begin} spans schema transition "
                f"LSN {transition_lsn} for '{table.exposed_name}'; keep source writes "
                "quiesced until schema transition completes or run a full refresh"
            )
        if (
            transition_lsn is not None
            and (trigger_high_water is None or transition_lsn <= trigger_high_water)
            and (
                (caught_up and consumed == len(recovered))
                or (
                    crossed_transition
                    and (open_begin is None or open_begin >= transition_lsn)
                )
            )
        ):
            end = _offset(
                transition_lsn,
                transition_lsn,
                transition_lsn,
                None,
                "stream",
                transition_table,
                transition_schema_id,
                pipeline_scope,
                trigger_generation=checkpoint.get("trigger_generation"),
            )
        reached_trigger_boundary = trigger_generation is not None and (
            crossed_trigger_boundary or (caught_up and consumed == len(recovered))
        )
        if reached_trigger_boundary:
            end = dict(end)
            end["trigger_generation"] = trigger_generation
        return iter(output), end

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
            self._validate_immutable_record_header(
                existing, "schema-node", table.exposed_name
            )
            schema = existing.get("schema")
            predecessor = schema.get("predecessor") if isinstance(schema, dict) else None
            if (
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
                raise InformixError(
                    f"Checkpoint schema {checkpoint_schema_id} conflicts with immutable "
                    f"history for '{table.exposed_name}'"
                )
            if (
                _table_from_schema_state(schema, table.database).native_identity
                != table.native_identity
                or self._immutable_lsn(schema, "start_lsn", table.exposed_name)
                > checkpoint_lsn
            ):
                raise InformixError(
                    f"Invalid immutable schema-node state for '{table.exposed_name}'"
                )
            return
        minimum, current = self._bridge.minimum_lsn(), self._bridge.current_lsn()
        if not minimum <= checkpoint_lsn <= current:
            raise InformixError(
                f"Cannot rebuild immutable schema state for '{table.exposed_name}' from "
                f"checkpoint LSN {checkpoint_lsn}; retained/current range is [{minimum}, {current}]"
            )
        self._bridge.validate_initial_lsn(
            _capture_descriptor(table, _client_encoding(self.options)), checkpoint_lsn
        )
        authoritative = self._find_immutable_schema_record(
            table, checkpoint_schema_id, pipeline_scope
        )
        if authoritative is None:
            if checkpoint_fingerprint != _schema_fingerprint(table):
                raise InformixError(
                    f"Schema history for checkpoint node {checkpoint_schema_id} is missing "
                    f"for '{table.exposed_name}' and cannot be reconstructed after a schema "
                    "change; run a full refresh"
                )
            authoritative = {
                "created_at": time.time(),
                "schema": _schema_state(
                    table, checkpoint_lsn, schema_id=checkpoint_schema_id
                ),
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
        self._validate_immutable_record_header(
            winner, "schema-node", table.exposed_name
        )
        schema = winner.get("schema")
        if (
            not isinstance(schema, dict)
            or schema.get("id") != checkpoint_schema_id
            or self._immutable_lsn(schema, "start_lsn", table.exposed_name)
            != checkpoint_lsn
            or _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
            or schema.get("fingerprint") != _schema_fingerprint(table)
        ):
            raise InformixError(
                f"Checkpoint schema {checkpoint_schema_id} conflicts with immutable history "
                f"for '{table.exposed_name}'"
            )

    def _shared_trigger_boundary(
        self, table: Table, checkpoint: dict[str, Any], *, owner: bool
    ) -> tuple[int, str]:
        cached = self._trigger_boundaries.get(table.identity)
        if cached is not None:
            return cached
        # Both independently checkpointed readers present the same predecessor
        # after every successfully coordinated trigger. Keying the next boundary
        # by that durable predecessor avoids relying on Lakeflow runtime IDs,
        # which are not exposed to Python data-source workers.
        predecessor = str(checkpoint.get("trigger_generation") or "initial")
        scope = self._pipeline_scope(checkpoint)
        boundary_key = hashlib.sha256(
            "\0".join(
                (
                    scope,
                    str(checkpoint.get("schema_id", "")),
                    predecessor,
                )
            ).encode()
        ).hexdigest()
        namespace = self._immutable_namespace(table, "triggers", boundary_key)
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            trigger = self._read_immutable_head(namespace)
            if trigger is None and not owner:
                trigger_root = self._immutable_namespace(table, "triggers")
                direct: dict[str, object] | None = None
                direct_count = 0
                fallback_found = False
                try:
                    trigger_descriptor = _open_state_directory(
                        self._shared_state_location, trigger_root
                    )
                except FileNotFoundError:
                    trigger_descriptor = None
                except OSError as error:
                    raise InformixError(
                        f"Cannot inspect Informix trigger boundaries '{trigger_root}'"
                    ) from error
                if trigger_descriptor is not None:
                    try:
                        with os.scandir(trigger_descriptor) as entries:
                            for entry in entries:
                                try:
                                    is_directory = entry.is_dir(follow_symlinks=False)
                                except FileNotFoundError:
                                    continue
                                if not is_directory:
                                    continue
                                candidate = self._read_immutable_head(
                                    os.path.join(trigger_root, entry.name)
                                )
                                if candidate is None:
                                    continue
                                if (
                                    candidate.get("scope") != scope
                                    or candidate.get("schema_id")
                                    != checkpoint.get("schema_id")
                                ):
                                    continue
                                self._validate_immutable_record_header(
                                    candidate, "trigger", table.exposed_name
                                )
                                if (
                                    candidate.get("generation") != predecessor
                                    and self._immutable_lsn(
                                        candidate, "high_water", table.exposed_name
                                    )
                                    >= int(checkpoint.get("commit_lsn", 0))
                                ):
                                    fallback_found = True
                                    if candidate.get("predecessor") == predecessor:
                                        direct_count += 1
                                        if direct_count == 1:
                                            direct = candidate
                    finally:
                        os.close(trigger_descriptor)
                if direct_count == 1:
                    trigger = direct
                elif direct_count > 1 or fallback_found:
                    raise InformixError(
                        f"Ambiguous immutable trigger boundary for '{table.exposed_name}'; "
                        "run a full refresh rather than choosing between overlapping updates"
                    )
            if isinstance(trigger, dict):
                self._validate_immutable_record_header(
                    trigger, "trigger", table.exposed_name
                )
                generation = trigger.get("generation")
                try:
                    high_water = self._immutable_lsn(
                        trigger, "high_water", table.exposed_name
                    )
                except InformixError as error:
                    raise InformixError(
                        f"Invalid shared trigger boundary for '{table.exposed_name}'"
                    ) from error
                if not isinstance(generation, str) or not re.fullmatch(
                    r"[0-9a-f]{32}", generation
                ):
                    raise InformixError(
                        f"Invalid shared trigger generation for '{table.exposed_name}'"
                    )
                if (
                    trigger.get("scope") != scope
                    or trigger.get("schema_id") != checkpoint.get("schema_id")
                    or high_water < int(checkpoint.get("commit_lsn", 0))
                    or trigger.get("predecessor") != predecessor
                ):
                    raise InformixError(
                        f"Invalid immutable trigger identity for '{table.exposed_name}'"
                    )
                self._trigger_boundaries[table.identity] = (high_water, generation)
                return high_water, generation
            if owner:
                candidate_high_water = self._bridge.current_lsn()
                if candidate_high_water < int(checkpoint["commit_lsn"]):
                    raise InformixError(
                        f"Current LSN {candidate_high_water} precedes checkpoint LSN "
                        f"{checkpoint['commit_lsn']} for '{table.exposed_name}'"
                    )
                self._publish_immutable_head(
                    namespace,
                    {
                        "created_at": time.time(),
                        "generation": secrets.token_hex(16),
                        "high_water": str(candidate_high_water),
                        "predecessor": predecessor,
                        "schema_id": str(checkpoint.get("schema_id", "")),
                        "scope": scope,
                    },
                    record_type="trigger",
                )
                continue
            if time.monotonic() >= deadline:
                role = "upsert reader" if owner else "the table's upsert reader"
                raise InformixError(
                    f"Timed out waiting for {role} to publish a triggered boundary for "
                    f"'{table.exposed_name}'"
                )
            time.sleep(0.1)

    def _schema_transition(
        self,
        table: Table,
        checkpoint_schema_id: str,
        checkpoint_lsn: int,
        *,
        owner: bool,
    ) -> tuple[Table, Table, int, str]:
        namespace = self._immutable_namespace(table, "schemas", checkpoint_schema_id)
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            previous_record = self._read_immutable_head(
                self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id)
            )
            previous = previous_record.get("schema") if previous_record else None
            if not isinstance(previous, dict):
                raise InformixError(
                    f"Schema history for checkpoint node {checkpoint_schema_id} is missing for "
                    f"'{table.exposed_name}'; run a full refresh"
                )
            self._validate_immutable_record_header(
                previous_record, "schema-node", table.exposed_name
            )
            if previous.get("id") != checkpoint_schema_id:
                raise InformixError(
                    f"Immutable schema node identity mismatch for '{table.exposed_name}'"
                )
            previous_table = _table_from_schema_state(previous, table.database)
            _ensure_additive_schema_change(previous_table, table)
            transition_record = self._read_immutable_head(namespace)
            if transition_record is None and owner:
                transition = self._bridge.current_lsn()
                if transition < checkpoint_lsn:
                    raise InformixError(
                        f"Current LSN {transition} precedes checkpoint LSN "
                        f"{checkpoint_lsn} for '{table.exposed_name}'"
                    )
                self._bridge.validate_initial_lsn(
                    _capture_descriptor(table, _client_encoding(self.options)),
                    transition,
                )
                node = _schema_state(table, transition, predecessor=checkpoint_schema_id)
                transition_record = self._publish_immutable_head(
                    namespace,
                    {"created_at": time.time(), "schema": node},
                    record_type="schema-transition",
                )
            if transition_record is not None:
                self._validate_immutable_record_header(
                    transition_record, "schema-transition", table.exposed_name
                )
                current = transition_record.get("schema")
                if not isinstance(current, dict):
                    raise InformixError("Invalid immutable Informix schema transition")
                transition = self._immutable_lsn(
                    current, "start_lsn", table.exposed_name
                )
                previous_lsn = self._immutable_lsn(
                    previous, "start_lsn", table.exposed_name
                )
                minimum, now = self._bridge.minimum_lsn(), self._bridge.current_lsn()
                if previous_lsn > checkpoint_lsn:
                    raise InformixError(
                        f"Schema node LSN {previous_lsn} follows checkpoint LSN "
                        f"{checkpoint_lsn} for '{table.exposed_name}'"
                    )
                if transition <= previous_lsn:
                    raise InformixError(
                        f"Schema transition for '{table.exposed_name}' is not monotonic"
                    )
                if not minimum <= transition <= now:
                    raise InformixError(
                        f"Schema transition LSN {transition} for '{table.exposed_name}' is "
                        f"outside retained/current range [{minimum}, {now}]"
                    )
                if transition < checkpoint_lsn:
                    raise InformixError(
                        f"Schema transition LSN {transition} precedes checkpoint LSN "
                        f"{checkpoint_lsn} for '{table.exposed_name}'"
                    )
                target_table = _table_from_schema_state(current, table.database)
                if (
                    not isinstance(current.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", str(current["id"]))
                    or current.get("predecessor") != checkpoint_schema_id
                ):
                    raise InformixError(
                        f"Invalid immutable schema identity for '{table.exposed_name}'"
                    )
                _ensure_additive_schema_change(previous_table, target_table)
                if _schema_fingerprint(target_table) != _schema_fingerprint(table):
                    _ensure_additive_schema_change(target_table, table)
                schema_winner = self._publish_immutable_head(
                    self._immutable_namespace(table, "schema-nodes", str(current["id"])),
                    transition_record,
                    record_type="schema-node",
                )
                self._validate_schema_node_winner(schema_winner, current, table)
                return previous_table, target_table, transition, str(current["id"])
            if time.monotonic() >= deadline:
                raise InformixError(
                    f"Timed out waiting for the upsert reader to publish schema transition "
                    f"state for '{table.exposed_name}'"
                )
            time.sleep(0.1)

    def _refresh_table_schema(
        self, table: Table, expected_fingerprint: str | None
    ) -> Table:
        refreshed = Table.parse(self._bridge.get_table(table.identity), table.database)
        _ensure_materializable(refreshed)
        fingerprint = _schema_fingerprint(refreshed)
        if expected_fingerprint is not None and expected_fingerprint != fingerprint:
            raise InformixError(
                f"Informix schema changed for '{table.exposed_name}' during ingestion; "
                "run a full refresh before reading additional snapshot or CDC records"
            )
        if self._tables is not None:
            self._tables[table.exposed_name] = refreshed
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
        value = int(table_options.get(name, self.options.get(name, str(default))))
        if value < minimum:
            raise ValueError(f"Option '{name}' must be >= {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"Option '{name}' must be <= {maximum}")
        return value

    def _initial_lsn(self, table: Table, *, owner: bool = True) -> int:
        """Return one durable per-table boundary shared by upsert and delete readers."""

        if table.identity not in self._snapshot_high_water:
            value, schema_id = self._shared_table_lsn(table, owner=owner)
            minimum = self._bridge.minimum_lsn()
            if value < minimum:
                raise LogRetentionError(
                    f"Configured initial LSN {value} is older than minimum retained LSN "
                    f"{minimum}; choose a retained boundary after enabling full-row logging"
                )
            current = self._bridge.current_lsn()
            if value > current:
                raise InformixError(
                    f"Configured initial LSN {value} is newer than current Informix LSN "
                    f"{current}; choose a position captured after enabling full-row logging"
                )
            self._snapshot_high_water[table.identity] = value
            self._snapshot_schema_ids[table.identity] = schema_id
        return self._snapshot_high_water[table.identity]

    def _shared_table_lsn(self, table: Table, *, owner: bool) -> tuple[int, str]:
        scope = self._pipeline_scope()
        namespace = self._immutable_namespace(table, "initialization", scope)
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            record = self._read_immutable_head(namespace)
            if record is not None:
                self._validate_immutable_record_header(
                    record, "initialization", table.exposed_name
                )
                if (
                    record.get("table") != table.native_identity
                    or record.get("scope") != scope
                ):
                    raise InformixError("Informix immutable initialization table mismatch")
                node = record.get("schema")
                if (
                    not isinstance(node, dict)
                    or not isinstance(node.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", str(node.get("id")))
                    or node.get("fingerprint") != _schema_fingerprint(table)
                ):
                    raise InformixError("Invalid Informix immutable initialization schema")
                value = self._immutable_lsn(
                    record, "initial_lsn", table.exposed_name
                )
                if (
                    self._immutable_lsn(node, "start_lsn", table.exposed_name)
                    != value
                    or _table_from_schema_state(node, table.database).native_identity
                    != table.native_identity
                    or node.get("predecessor") is not None
                ):
                    raise InformixError("Invalid Informix immutable initialization schema")
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
                            or self._immutable_lsn(
                                snapshot, "initial_lsn", table.exposed_name
                            )
                            != value
                        ):
                            raise InformixError(
                                f"Invalid immutable snapshot boundary for '{table.exposed_name}'"
                            )
                        value = self._immutable_lsn(
                            snapshot, "snapshot_lsn", table.exposed_name
                        )
                if record is not None:
                    self._bridge.validate_initial_lsn(
                        _capture_descriptor(table, _client_encoding(self.options)), value
                    )
                    if owner:
                        schema_winner = self._publish_immutable_head(
                            self._immutable_namespace(
                                table, "schema-nodes", str(node["id"])
                            ),
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
                    _capture_descriptor(table, _client_encoding(self.options)), value
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
            if time.monotonic() >= deadline:
                role = "upsert initialization" if owner else "the table's upsert reader"
                raise InformixError(
                    f"Timed out waiting for {role} to publish shared CDC state for "
                    f"'{table.exposed_name}' at '{namespace}'"
                )
            time.sleep(0.1)

    def _publish_snapshot_boundary(
        self,
        table: Table,
        schema_id: str,
        initial_lsn: int,
        snapshot_lsn: int,
        pipeline_scope: str,
    ) -> None:
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
        self._validate_immutable_record_header(
            winner, "snapshot", table.exposed_name
        )
        if self._immutable_lsn(winner, "initial_lsn", table.exposed_name) != initial_lsn:
            raise InformixError(
                f"Conflicting immutable snapshot boundary for '{table.exposed_name}'"
            )
        if (
            winner.get("scope") != pipeline_scope
            or winner.get("schema_id") != schema_id
            or self._immutable_lsn(winner, "snapshot_lsn", table.exposed_name)
            < initial_lsn
        ):
            raise InformixError(
                f"Invalid immutable snapshot boundary for '{table.exposed_name}'"
            )

    def _shared_table_state_root(self, table: Table) -> str:
        hostname = self.options.get("hostname", "").strip().rstrip(".").casefold()
        port = str(int(self.options.get("port", "9088")))
        server = self.options.get("server", "").strip()
        database = self.options.get("database", "").strip()
        namespace = "\0".join(
            ("v2", hostname, port, server, database)
        )
        connection_key = hashlib.sha256(namespace.encode()).hexdigest()[:24]
        table_key = hashlib.sha256(table.native_identity.encode()).hexdigest()[:24]
        return os.path.join(self._shared_state_location, connection_key, table_key)

    def _immutable_namespace(self, table: Table, *parts: str) -> str:
        table_directory = self._shared_table_state_root(table)
        safe = []
        for part in parts:
            value = str(part)
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
                value = hashlib.sha256(value.encode()).hexdigest()
            safe.append(value)
        return os.path.join(table_directory, *safe)

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
        transitions = self._immutable_namespace(table, "schemas")
        try:
            transitions_descriptor = _open_state_directory(
                self._shared_state_location, transitions
            )
        except FileNotFoundError:
            return None
        except OSError as error:
            raise InformixError(
                f"Cannot inspect immutable schema transitions '{transitions}'"
            ) from error
        match: dict[str, object] | None = None
        try:
            with os.scandir(transitions_descriptor) as entries:
                for entry in entries:
                    try:
                        is_directory = entry.is_dir(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if not is_directory:
                        continue
                    record = self._read_immutable_head(
                        os.path.join(transitions, entry.name)
                    )
                    if record is not None:
                        self._validate_immutable_record_header(
                            record, "schema-transition", table.exposed_name
                        )
                    schema = record.get("schema") if record is not None else None
                    if isinstance(schema, dict) and schema.get("id") == schema_id:
                        if match is not None:
                            raise InformixError(
                                f"Duplicate immutable schema node {schema_id} for "
                                f"'{table.exposed_name}'"
                            )
                        match = record
        finally:
            os.close(transitions_descriptor)
        return match

    def _read_immutable_head(self, namespace: str) -> dict[str, object] | None:
        head = os.path.join(namespace, "head")
        path = os.path.join(head, "record.json")
        try:
            descriptor = _open_state_file(self._shared_state_location, path)
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                os.close(descriptor)
                raise InformixError(f"Invalid Informix immutable record '{path}'")
            if metadata.st_size > _MAX_SHARED_STATE_BYTES:
                os.close(descriptor)
                raise InformixError(f"Informix immutable record '{path}' is too large")
            with os.fdopen(descriptor, encoding="utf-8") as handle:
                value = json.load(handle)
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError) as error:
            raise InformixError(f"Cannot read Informix immutable record '{path}'") from error
        if not isinstance(value, dict):
            raise InformixError(f"Invalid Informix immutable record '{path}'")
        return value

    def _publish_immutable_head(
        self,
        namespace: str,
        record: dict[str, object],
        *,
        record_type: str = "generic",
    ) -> dict[str, object]:
        """Elect exactly one complete immutable record for a logical key."""

        _validate_shared_state_filesystem(self._shared_state_location)
        _maybe_cleanup_immutable_candidates(self._shared_state_location)
        record = {
            **record,
            "format_version": _IMMUTABLE_STATE_VERSION,
            "record_type": record_type,
        }
        token = secrets.token_hex(16)
        candidate_name = f"candidate-{token}"
        candidate = os.path.join(namespace, candidate_name)
        namespace_descriptor: int | None = None
        candidate_descriptor: int | None = None
        try:
            _makedirs_durable(self._shared_state_location, namespace)
            namespace_descriptor = _open_state_directory(
                self._shared_state_location, namespace
            )
            os.mkdir(candidate_name, mode=0o700, dir_fd=namespace_descriptor)
            candidate_descriptor = os.open(
                candidate_name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=namespace_descriptor,
            )
        except OSError as error:
            if candidate_descriptor is not None:
                os.close(candidate_descriptor)
            if namespace_descriptor is not None:
                os.close(namespace_descriptor)
            raise InformixError(
                f"Cannot create Informix immutable candidate '{candidate}'"
            ) from error
        record_path = os.path.join(candidate, "record.json")
        try:
            payload = json.dumps(record, separators=(",", ":"), sort_keys=True)
            if len(payload.encode()) > _MAX_SHARED_STATE_BYTES:
                raise InformixError(
                    f"Informix immutable record '{record_path}' is too large"
                )
            record_descriptor = os.open(
                "record.json",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                mode=0o600,
                dir_fd=candidate_descriptor,
            )
            with os.fdopen(record_descriptor, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.fsync(candidate_descriptor)
            os.close(candidate_descriptor)
            candidate_descriptor = None
            try:
                os.rename(
                    candidate_name,
                    "head",
                    src_dir_fd=namespace_descriptor,
                    dst_dir_fd=namespace_descriptor,
                )
                os.fsync(namespace_descriptor)
                return record
            except OSError as error:
                if error.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                winner = self._read_immutable_head(namespace)
                if winner is None:
                    raise InformixError(
                        f"Informix immutable election at '{namespace}' has no readable winner"
                    ) from error
                return winner
        except OSError as error:
            raise InformixError(
                f"Cannot publish Informix immutable record at '{namespace}'"
            ) from error
        finally:
            if candidate_descriptor is not None:
                os.close(candidate_descriptor)
            try:
                if namespace_descriptor is not None:
                    os.unlink(
                        os.path.join(candidate_name, "record.json"),
                        dir_fd=namespace_descriptor,
                    )
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).warning(
                    "Retained abandoned Informix immutable candidate: path=%s", candidate
                )
            try:
                if namespace_descriptor is not None:
                    os.rmdir(candidate_name, dir_fd=namespace_descriptor)
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).warning(
                    "Retained abandoned Informix immutable candidate: path=%s", candidate
                )
            if namespace_descriptor is not None:
                os.close(namespace_descriptor)

    @staticmethod
    def _validate_immutable_record_header(
        record: dict[str, object], expected_type: str, context: str
    ) -> None:
        if (
            not isinstance(record.get("format_version"), int)
            or isinstance(record.get("format_version"), bool)
            or record.get("format_version") != _IMMUTABLE_STATE_VERSION
            or record.get("record_type") != expected_type
        ):
            raise InformixError(
                f"Unsupported or mismatched Informix immutable {expected_type} record "
                f"for {context}"
            )

    def _validate_schema_node_winner(
        self,
        winner: dict[str, object],
        expected_schema: dict[str, object],
        table: Table,
    ) -> None:
        self._validate_immutable_record_header(
            winner, "schema-node", table.exposed_name
        )
        schema = winner.get("schema")
        if not isinstance(schema, dict) or schema != expected_schema:
            raise InformixError(
                f"Conflicting immutable schema-node winner for '{table.exposed_name}'"
            )
        if (
            _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
        ):
            raise InformixError(
                f"Immutable schema-node table mismatch for '{table.exposed_name}'"
            )

    @staticmethod
    def _immutable_lsn(
        record: dict[str, object], field: str, context: str
    ) -> int:
        try:
            return _strict_lsn(record.get(field), field)
        except (TypeError, ValueError) as error:
            raise InformixError(
                f"Invalid {field} in Informix immutable record for {context}"
            ) from error

    @staticmethod
    def _fsync_directory(path: str) -> None:
        _fsync_directory_path(path)

    def _table_map(self, refresh: bool = False) -> dict[str, Table]:
        if self._tables is None or refresh:
            result: dict[str, Table] = {}
            database = self.options.get("database", "")
            for raw in self._bridge.list_tables():
                table = Table.parse(raw, database)
                if _eligible(table) and self._selected(table):
                    if table.exposed_name in result:
                        raise InformixError(f"Duplicate exposed table name {table.exposed_name}")
                    result[table.exposed_name] = table
            self._tables = result
        return self._tables

    def _selected(self, table: Table) -> bool:
        include = _patterns(self.options.get("table.include.list") or self.options.get("tables"))
        exclude = _patterns(self.options.get("table.exclude.list"))
        names = (table.identity, table.exposed_name, table.native_identity)
        return (
            not include or any(fnmatch.fnmatchcase(n, p) for n in names for p in include)
        ) and not any(fnmatch.fnmatchcase(n, p) for n in names for p in exclude)

    def _table(self, name: str, options: dict[str, str], refresh: bool = False) -> Table:
        exposed = options.get("qualified_source_table", name)
        tables = self._table_map()
        if exposed not in tables and refresh:
            tables = self._table_map(refresh=True)
        if exposed not in tables:
            raise ValueError(f"Unknown or excluded Informix table '{exposed}'")
        table = tables[exposed]
        if refresh:
            raw = self._bridge.get_table(table.identity)
            table = Table.parse(raw, table.database)
            self._tables[exposed] = table
        return table


# Conventional alias used by some connector loaders.
InformixConnect = InformixLakeflowConnect


def _load_factory(path: str) -> Callable[[dict[str, str]], InformixBridge]:
    module_name, separator, attribute = path.partition(":")
    if not separator:
        module_name, separator, attribute = path.rpartition(".")
    if not module_name or not attribute:
        raise ValueError("bridge.factory must be 'module:callable'")
    factory = getattr(importlib.import_module(module_name), attribute)
    if not callable(factory):
        raise TypeError(f"Bridge factory {path!r} is not callable")
    return factory


_CATALOG_TYPES = {
    0: "CHAR",
    1: "SMALLINT",
    2: "INTEGER",
    3: "FLOAT",
    4: "SMALLFLOAT",
    5: "DECIMAL",
    6: "SERIAL",
    7: "DATE",
    8: "MONEY",
    10: "DATETIME",
    11: "BYTE",
    12: "TEXT",
    13: "VARCHAR",
    14: "INTERVAL",
    15: "NCHAR",
    16: "NVARCHAR",
    17: "INT8",
    18: "SERIAL8",
    19: "SET",
    20: "MULTISET",
    21: "LIST",
    22: "ROW",
    23: "COLLECTION",
    40: "UDT_VAR",
    41: "UDT_FIXED",
    43: "LVARCHAR",
    45: "BOOLEAN",
    52: "BIGINT",
    53: "BIGSERIAL",
    101: "BLOB",
    102: "CLOB",
}


def _field(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        if name in row:
            return row[name]
        lowered = {str(key).lower(): value for key, value in row.items()}
        if name.lower() in lowered:
            return lowered[name.lower()]
        # Informix names unaliased routine results after the full expression.
        # Lifecycle calls return a single scalar, so retain the positional
        # fallback used for tuple rows when a stable label is unavailable.
        return tuple(row.values())[index]
    return row[index]


def _catalog_column(row: Any) -> dict[str, Any]:
    name = str(_field(row, "colname", 0))
    raw_type = int(_field(row, "coltype", 1))
    length = int(_field(row, "collength", 2))
    base_type = raw_type & 0xFF
    type_name = _CATALOG_TYPES.get(base_type)
    if type_name is None:
        raise InformixError(f"Unsupported Informix catalog coltype {base_type} for {name}")
    if type_name in {"UDT_VAR", "UDT_FIXED"}:
        if isinstance(row, dict):
            lowered = {str(key).lower(): value for key, value in row.items()}
            extended_name = str(lowered.get("extended_name") or "").strip().upper()
            extended_owner = str(lowered.get("extended_owner") or "").strip().upper()
        else:
            extended_name = str(row[6] or "").strip().upper() if len(row) > 6 else ""
            extended_owner = str(row[7] or "").strip().upper() if len(row) > 7 else ""
        builtin_extended_types = {
            ("UDT_VAR", "INFORMIX", "LVARCHAR"): "LVARCHAR",
            ("UDT_FIXED", "INFORMIX", "BOOLEAN"): "BOOLEAN",
        }
        type_name = builtin_extended_types.get(
            (type_name, extended_owner, extended_name), type_name
        )
    if type_name == "DATETIME":
        # syscolumns.collength stores the packed width in its high byte and
        # start/end qualifier nibbles in its low byte.  CDC descriptors use
        # the JDBC extended-id layout instead: start in bits 8..11 and end in
        # bits 0..3.  For example, live YEAR TO FRACTION(5) is 0x130f in the
        # catalog and must become 0x000f for the native row decoder.
        encoded_qualifier = length & 0xFF
        length = ((encoded_qualifier >> 4) << 8) | (encoded_qualifier & 0x0F)
    precision = scale = None
    if type_name in {"DECIMAL", "MONEY"}:
        precision, scale = (length >> 8) & 0xFF, length & 0xFF
    unsupported = {
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "INTERVAL",
        "NCHAR",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    cdc_supported = type_name not in unsupported
    if type_name == "DATETIME":
        cdc_supported = _datetime_qualifier_supported(length)
    return {
        "name": name,
        "type_name": type_name,
        "nullable": not bool(raw_type & 0x100),
        "length": length,
        "precision": precision,
        "scale": scale,
        "cdc_supported": cdc_supported,
    }


def _column_descriptor(raw: dict[str, Any]) -> ColumnDescriptor:
    return ColumnDescriptor(
        name=str(raw["name"]),
        type_name=str(raw["type_name"]),
        length=int(raw.get("length") or 0),
        precision=_optional_int(raw.get("precision")),
        scale=_optional_int(raw.get("scale")),
        encoding=str(raw.get("encoding") or "utf-8"),
    )


def _expect_zero(rows: list[Any], operation: str) -> None:
    if not rows:
        raise InformixError(f"{operation} returned no status")
    status = int(_field(rows[0], "status", 0))
    if status != 0:
        raise InformixError(f"{operation} failed with status {status}")


def _optional_int(value: Any) -> int | None:
    return None if value is None or value == "" else int(value)


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not (
        isinstance(value, int)
        or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value))
    ):
        raise ValueError(f"{name} must be an integer, not {value!r}")
    return int(value)


def _strict_timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} must be a timestamp, not {value!r}")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite, not {value!r}")
    return result


def _strict_lsn(value: object, name: str) -> int:
    result = _strict_int(value, name)
    if not 0 <= result < 1 << 64:
        raise ValueError(f"{name} is outside the unsigned 64-bit LSN domain")
    return result


def _strict_tx_id(value: object, name: str = "tx_id") -> int:
    result = _strict_int(value, name)
    if not -(1 << 31) <= result < 1 << 32:
        raise ValueError(f"{name} is outside the native 32-bit transaction-ID domain")
    return result + (1 << 32) if result < 0 else result


def _patterns(value: str | None) -> tuple[str, ...]:
    return tuple(part.strip() for part in (value or "").split(",") if part.strip())


def _eligible(table: Table) -> bool:
    return not (
        table.owner.lower().startswith("sys")
        or table.name.lower().startswith("sys")
        or table.database.lower() == "syscdcv1"
    )


def _cdc_capable(table: Table) -> bool:
    # Missing/placeholder values are unsafe in Lakeflow table rows, especially
    # for binary and complex types, so a table containing uncaptured columns is
    # explicitly snapshot-only rather than silently nulling those values.
    return bool(table.primary_keys) and all(column.cdc_supported for column in table.columns)


def _snapshot_unsupported_columns(table: Table) -> tuple[Column, ...]:
    """Return types whose ordinary SQLI row representation is not implemented."""

    unsupported = {
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "INTERVAL",
        "NCHAR",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    return tuple(
        column
        for column in table.columns
        if column.type_name in unsupported
        or (
            column.type_name == "DATETIME"
            and not _datetime_qualifier_supported(column.length or 0)
        )
    )


def _datetime_qualifier_supported(qualifier: int) -> bool:
    start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
    fields = {0, 2, 4, 6, 8, 10}
    return start in fields and end in {*fields, 11, 12, 13, 14, 15} and end >= start


def _ensure_materializable(table: Table) -> None:
    unsupported = _snapshot_unsupported_columns(table)
    if unsupported:
        details = ", ".join(f"{column.name} ({column.type_name})" for column in unsupported)
        raise InformixError(
            f"Table '{table.exposed_name}' contains columns that the pure-Python SQLI "
            f"snapshot decoder cannot materialize: {details}"
        )
    for column in table.columns:
        if column.type_name in {"DECIMAL", "NUMERIC", "MONEY"} and (
            column.precision is None
            or column.scale is None
            or not 1 <= column.precision <= 38
            or not 0 <= column.scale <= column.precision
        ):
            raise InformixError(
                f"Table '{table.exposed_name}' has invalid {column.type_name} metadata for "
                f"column {column.name}: precision={column.precision}, scale={column.scale}"
            )


def _spark_type(column: Column):
    name = column.type_name.split("(", 1)[0].strip()
    if name in {"SMALLINT", "INT2"}:
        # The framework row converter does not support Spark ShortType. Widen
        # Informix's signed 16-bit value to IntegerType without losing data.
        return IntegerType()
    if name in {"INTEGER", "INT", "SERIAL"}:
        return IntegerType()
    if name in {"BIGINT", "INT8", "BIGSERIAL", "SERIAL8"}:
        return LongType()
    if name in {"REAL", "SMALLFLOAT"}:
        return FloatType()
    if name in {"FLOAT", "DOUBLE", "DOUBLE PRECISION"}:
        return DoubleType()
    if name in {"DECIMAL", "NUMERIC", "MONEY"}:
        precision = column.precision or (19 if name == "MONEY" else 38)
        scale = column.scale or (2 if name == "MONEY" else 0)
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return DecimalType(precision, scale)
        return StringType()
    if name == "DATE":
        return DateType()
    if name.startswith("DATETIME") or name == "TIMESTAMP":
        start, end = ((column.length or 0) >> 8) & 0xF, (column.length or 0) & 0xF
        return TimestampType() if start == 0 and end >= 4 else StringType()
    if name in {"BOOLEAN", "BOOL"}:
        return BooleanType()
    if name in {"BYTE", "BLOB", "BINARY", "VARBINARY"}:
        return BinaryType()
    return StringType()


def _operation(record: dict[str, Any]) -> str:
    return str(record.get("op") or record.get("operation") or record.get("type") or "").upper()


def _lsn(record: dict[str, Any]) -> int:
    value = record.get("lsn", record.get("sequence", record.get("sequence_id")))
    if value is None:
        raise InformixError(f"CDC record has no LSN: {record!r}")
    try:
        return _strict_lsn(value, "CDC record LSN")
    except ValueError as error:
        raise InformixError(f"CDC record has an invalid LSN: {record!r}") from error


def _tx_id(record: dict[str, Any]) -> int:
    value = record.get("tx_id", record.get("transaction_id"))
    if value is None:
        raise InformixError(f"CDC record has no transaction ID: {record!r}")
    try:
        return _strict_tx_id(value, "CDC transaction ID")
    except ValueError as error:
        raise InformixError(f"CDC record has an invalid transaction ID: {record!r}") from error


def _normalise_record(raw: dict[str, Any]) -> dict[str, Any]:
    record = dict(raw)
    record["op"] = _operation(record)
    if record["op"] not in {"METADATA", "ERROR"}:
        record["lsn"] = _lsn(record)
    if record["op"] not in {"TIMEOUT", "METADATA", "ERROR"}:
        record["tx_id"] = _tx_id(record)
    return record


def _committed_transactions(records: Sequence[dict[str, Any]]) -> list[CommittedTransaction]:
    return _transaction_batch(records)[0]


def _transaction_batch(
    records: Sequence[dict[str, Any]],
) -> tuple[list[CommittedTransaction], bool, int | None]:
    buffer = TransactionBuffer()
    result = []
    last_lsn: int | None = None
    timed_out = False
    for record in records:
        if _operation(record) == "TIMEOUT":
            timed_out = True
        if _operation(record) not in {"METADATA", "ERROR", "DISCARD", "TIMEOUT"}:
            lsn = _lsn(record)
            if last_lsn is not None and lsn < last_lsn:
                raise InformixError(f"CDC stream LSN regressed globally: {lsn} < {last_lsn}")
            last_lsn = lsn
        committed = buffer.feed(record)
        if committed is not None:
            result.append(committed)
    # Open transactions are intentionally discarded.  The returned offset
    # remains before their BEGIN so a finite next call safely replays them.
    open_begin = min((tx.begin_lsn for tx in buffer.open.values()), default=None)
    return result, timed_out and not buffer.open, open_begin


def _recover(
    transactions: Sequence[CommittedTransaction], checkpoint: dict[str, Any]
) -> list[CommittedTransaction]:
    commit = int(checkpoint["commit_lsn"])
    # Offsets are transaction-atomic. Replaying from the oldest open BEGIN can
    # reproduce transactions already checkpointed, but records from a newly
    # committed transaction must never be filtered using another transaction's
    # commit/change LSN.
    return [tx for tx in transactions if tx.commit_lsn > commit]


def _client_encoding(options: dict[str, str]) -> str:
    locale = options.get("CLIENT_LOCALE") or options.get("client.locale") or "en_US.utf8"
    return informix_locale_encoding(locale)


def _capture_descriptor(table: Table, encoding: str = "utf-8") -> dict[str, Any]:
    return {
        "identity": table.native_identity,
        "logical_identity": table.identity,
        "columns": [column.name for column in table.columns if column.cdc_supported],
        "descriptors": [
            {
                "name": column.name,
                "type_name": column.type_name,
                "length": column.length or column.precision or 0,
                "precision": column.precision,
                "scale": column.scale,
                "encoding": encoding,
            }
            for column in table.columns
            if column.cdc_supported
        ],
    }


def _project_transaction(
    tx: CommittedTransaction, table: Table, deletes: bool
) -> list[dict[str, Any]]:
    output = []
    for record in tx.records:
        if not _record_matches(record, table):
            continue
        op = _operation(record)
        if op == "TRUNCATE":
            raise UnsupportedChangeError(
                f"TRUNCATE on {table.exposed_name} cannot be represented as keyed Lakeflow deletes"
            )
        before = record.get("before", record.get("row") if op == "DELETE" else None)
        after = record.get("after", record.get("row") if op == "INSERT" else None)
        if deletes:
            if op == "DELETE":
                output.append(_shape_delete(before, table, record, tx))
            elif op == "UPDATE" and _key(before, table) != _key(after, table):
                output.append(_shape_delete(before, table, record, tx))
        elif op in {"INSERT", "UPDATE"}:
            output.append(_shape_change(after, record, tx, "u" if op == "UPDATE" else "c"))
    return output


def _record_matches(record: dict[str, Any], table: Table) -> bool:
    identity = str(record.get("table") or record.get("identity") or "")
    return not identity or identity in {
        table.name,
        table.exposed_name,
        table.identity,
        table.native_identity,
    }


def _key(row: dict[str, Any] | None, table: Table) -> tuple[Any, ...]:
    if row is None:
        raise InformixError(f"Missing before/after image for {table.exposed_name}")
    return tuple(row.get(pk) for pk in table.primary_keys)


def _shape_change(row, record, tx, op):
    if row is None:
        raise InformixError("CDC upsert has no after image")
    result = _framework_row(row)
    result.update(
        {
            CURSOR: _sortable_lsn(_lsn(record)),
            COMMIT_LSN: _sortable_lsn(tx.commit_lsn),
            TX_ID: tx.tx_id,
            OP: op,
        }
    )
    return result


def _shape_delete(row, table, record, tx):
    if row is None:
        raise InformixError("CDC delete has no before image; full-row logging is required")
    result = {column.name: None for column in table.columns}
    for pk in table.primary_keys:
        if row.get(pk) is None:
            raise InformixError(f"CDC delete has no primary-key value for {pk}")
        result[pk] = _framework_value(row[pk])
    result.update(
        {
            CURSOR: _sortable_lsn(_lsn(record)),
            COMMIT_LSN: _sortable_lsn(tx.commit_lsn),
            TX_ID: tx.tx_id,
            OP: "d",
        }
    )
    return result


def _shape_snapshot(row: dict[str, Any], lsn: int) -> dict[str, Any]:
    result = _framework_row(row)
    result.update(
        {CURSOR: _sortable_lsn(lsn), COMMIT_LSN: _sortable_lsn(lsn), TX_ID: None, OP: "r"}
    )
    return result


def _sortable_lsn(value: int) -> str:
    lsn = int(value)
    if not 0 <= lsn < 1 << 64:
        raise InformixError(f"Informix LSN {lsn} is outside the unsigned 64-bit decimal domain")
    return f"{lsn:0{_LSN_DECIMAL_WIDTH}d}"


def _framework_row(row: dict[str, Any]) -> dict[str, Any]:
    return {name: _framework_value(value) for name, value in row.items()}


def _framework_value(value: Any) -> Any:
    # The shared Spark Python Data Source parser accepts ISO strings for DateType
    # and TimestampType, but rejects a native datetime.date.  Normalize both
    # temporal Python objects at the connector boundary for consistent snapshot
    # and CDC behavior.
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def _deep_size(value: Any, seen: set[int] | None = None) -> int:
    """Estimate retained Python container/value memory without double counting."""

    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        return size + sum(
            _deep_size(key, seen) + _deep_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return size + sum(_deep_size(item, seen) for item in value)
    return size


def _offset(
    commit: int,
    change: int,
    begin: int,
    tx_id: int | None,
    phase: str,
    table: Table,
    schema_id: str,
    pipeline_scope: str,
    *,
    trigger_generation: str | None = None,
) -> dict:
    return {
        "version": _OFFSET_VERSION,
        "commit_lsn": str(commit),
        "change_lsn": str(change),
        "begin_lsn": str(begin),
        "tx_id": tx_id,
        "phase": phase,
        "schema_fingerprint": _schema_fingerprint(table),
        "schema_id": schema_id,
        "pipeline_scope": pipeline_scope,
        "trigger_generation": trigger_generation,
    }


def _schema_fingerprint(table: Table) -> str:
    layout = repr(
        (
            table.database,
            table.owner,
            table.name,
            table.incarnation,
            table.primary_keys,
            tuple(
                (
                    column.name,
                    column.type_name,
                    column.nullable,
                    column.length,
                    column.precision,
                    column.scale,
                    column.cdc_supported,
                )
                for column in table.columns
            ),
        )
    ).encode("utf-8")
    return hashlib.sha256(layout).hexdigest()


def _schema_state(
    table: Table,
    start_lsn: int,
    predecessor: str | None = None,
    *,
    schema_id: str | None = None,
) -> dict[str, object]:
    return {
        "id": schema_id or secrets.token_hex(16),
        "fingerprint": _schema_fingerprint(table),
        "start_lsn": str(start_lsn),
        "predecessor": predecessor,
        "table": {
            "database": table.database,
            "owner": table.owner,
            "name": table.name,
            "incarnation": table.incarnation,
            "primary_keys": list(table.primary_keys),
            "columns": [
                {
                    "name": column.name,
                    "type_name": column.type_name,
                    "nullable": column.nullable,
                    "length": column.length,
                    "precision": column.precision,
                    "scale": column.scale,
                    "cdc_supported": column.cdc_supported,
                }
                for column in table.columns
            ],
        },
    }


def _table_from_schema_state(state: dict[str, object], default_database: str) -> Table:
    raw = state.get("table")
    if not isinstance(raw, dict):
        raise InformixError("Informix shared CDC schema is missing table metadata")
    try:
        start_lsn = _strict_lsn(state["start_lsn"], "start_lsn")
    except (KeyError, TypeError, ValueError) as error:
        raise InformixError("Informix shared CDC schema has an invalid start LSN") from error
    if start_lsn < 1:
        raise InformixError("Informix shared CDC schema has an invalid start LSN")
    table = Table.parse(raw, default_database)
    if _schema_fingerprint(table) != state.get("fingerprint"):
        raise InformixError("Informix shared CDC schema fingerprint does not match metadata")
    return table


def _ensure_additive_schema_change(previous: Table, current: Table) -> None:
    if (
        previous.database,
        previous.owner,
        previous.name,
        previous.incarnation,
        previous.primary_keys,
    ) != (
        current.database,
        current.owner,
        current.name,
        current.incarnation,
        current.primary_keys,
    ):
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' changed table identity or "
            "primary keys; run a full refresh"
        )
    if len(current.columns) <= len(previous.columns):
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' is not an additive column "
            "change; run a full refresh"
        )
    if current.columns[: len(previous.columns)] != previous.columns:
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' modified, removed, or "
            "reordered existing columns; run a full refresh"
        )
    additions = current.columns[len(previous.columns) :]
    if any(not column.nullable or not column.cdc_supported for column in additions):
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' added a non-nullable or "
            "CDC-unsupported column; run a full refresh"
        )


def _validated_offset(offset: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(offset, dict):
        raise ValueError("Informix offset must be a dictionary")
    result = dict(offset)
    if result.get("version") != _OFFSET_VERSION:
        raise ValueError(
            f"Informix offset version {result.get('version')!r} is unsupported; "
            "run a full refresh with this connector version"
        )
    values = {}
    for key in ("commit_lsn", "change_lsn", "begin_lsn"):
        if key not in result:
            raise ValueError(f"Informix stream offset is missing '{key}'")
        values[key] = _strict_lsn(result[key], key)
    if not values["begin_lsn"] <= values["change_lsn"] <= values["commit_lsn"]:
        raise ValueError("Informix offset must satisfy begin_lsn <= change_lsn <= commit_lsn")
    phase = result.get("phase")
    if phase not in {"snapshot", "stream"}:
        raise ValueError("Informix offset phase must be 'snapshot' or 'stream'")
    fingerprint = result.get("schema_fingerprint")
    if fingerprint is not None and (
        not isinstance(fingerprint, str)
        or len(fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in fingerprint)
    ):
        raise ValueError("Informix offset has an invalid schema_fingerprint")
    schema_id = result.get("schema_id")
    if not isinstance(schema_id, str) or not re.fullmatch(r"[0-9a-f]{32}", schema_id):
        raise ValueError(
            "Informix offset has an invalid schema_id; run a full refresh with this "
            "connector version"
        )
    pipeline_scope = result.get("pipeline_scope")
    if not isinstance(pipeline_scope, str) or not re.fullmatch(
        r"[0-9a-f]{32}", pipeline_scope
    ):
        raise ValueError(
            "Informix offset has an invalid pipeline_scope; run a full refresh with this "
            "connector version"
        )
    trigger_generation = result.get("trigger_generation")
    if trigger_generation is not None and (
        not isinstance(trigger_generation, str)
        or not re.fullmatch(r"[0-9a-f]{32}", trigger_generation)
    ):
        raise ValueError("Informix offset has an invalid trigger_generation")
    tx_id = result.get("tx_id")
    if tx_id is not None:
        result["tx_id"] = _strict_tx_id(tx_id)
    if phase == "snapshot":
        if (
            "snapshot_lsn" not in result
            or _strict_lsn(result["snapshot_lsn"], "snapshot_lsn") < 0
        ):
            raise ValueError("Informix snapshot offset has an invalid snapshot_lsn")
        if any(
            values[key] != _strict_lsn(result["snapshot_lsn"], "snapshot_lsn")
            for key in values
        ):
            raise ValueError(
                "Informix snapshot offset requires snapshot_lsn, begin_lsn, "
                "change_lsn, and commit_lsn to be equal"
            )
        snapshot = result.get("snapshot")
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("last_pk"), list):
            raise ValueError("Informix snapshot offset is missing snapshot.last_pk")
    return result


__all__ = [
    "InformixConnect",
    "InformixLakeflowConnect",
    "InformixBridge",
    "InformixError",
    "PurePythonInformixBridge",
    "LogRetentionError",
    "TransactionBuffer",
    "UnsupportedChangeError",
    "cleanup_abandoned_immutable_candidates",
    "set_bridge_factory",
]
