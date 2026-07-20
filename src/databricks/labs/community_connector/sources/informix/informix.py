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
import math
import os
import re
import secrets
import socket
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
_OFFSET_VERSION = 6
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_DATA_OPS = {"INSERT", "BEFORE_UPDATE", "AFTER_UPDATE", "DELETE", "TRUNCATE"}
_DEFAULT_SNAPSHOT_PAGE_SIZE = 10000
_DEFAULT_MAX_RECORDS_PER_BATCH = 10000
_SHARED_STATE_VERSION = 5
_SHARED_STATE_WAIT_SECONDS = 300
_MAX_SHARED_STATE_BYTES = 1 << 20
_ARTIFACT_RETENTION_SECONDS = 3600
_VALIDATED_STATE_LOCATIONS: set[str] = set()

def _informix_available_now_base(base: type) -> type:
    """Wrap the generated reader base without changing the shared adapter source."""

    registration_scope = secrets.token_hex(16)

    class InformixAvailableNowBase(base):
        _informix_available_now_wrapper = True

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
    if not getattr(_generated_trigger_base, "_informix_available_now_wrapper", False):
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
            "SELECT c.colname, c.coltype, c.collength, c.colno, t.tabid "
            "FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
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
        "tls": options.get("encrypt", "true").lower() in {"1", "true", "yes"},
        "ca_file": options.get("ssl.ca.file"),
        "pad_varchar": options.get("padVarchar", "false").lower() in {"1", "true", "yes"},
        "cdc_timeout": int(options.get("cdc.timeout", "5")),
        "cdc_max_records": int(options.get("cdc.max.records", "64")),
        "stop_logging_on_close": False,
    }


def _option_bool(options: dict[str, str], name: str, default: bool) -> bool:
    value = options.get(name)
    return default if value is None else value.lower() in {"1", "true", "yes"}


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
    _cleanup_probe_artifacts(location)
    probe_root = os.path.join(location, f".informix-probe-{secrets.token_hex(8)}")
    contender = os.path.join(probe_root, "exclusive")
    renamed = os.path.join(probe_root, "renamed")
    try:
        os.makedirs(probe_root, mode=0o700)
        barrier = threading.Barrier(8, timeout=5)
        winners: list[int] = []
        failures: list[BaseException] = []

        def compete(index: int) -> None:
            try:
                barrier.wait()
                os.mkdir(contender, mode=0o700)
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
        os.rename(contender, renamed)
        if os.path.exists(contender) or not os.path.isdir(renamed):
            raise InformixError(
                "cdc.shared.state.location does not provide atomic directory rename"
            )
        # A duplicate concurrent probe is harmless because every probe uses a
        # unique directory. Avoid retaining a process lock in the generated
        # source closure: Spark must pickle the DataSource class for workers.
        _VALIDATED_STATE_LOCATIONS.add(location)
    except OSError as error:
        raise InformixError(
            f"Cannot validate Informix shared-state filesystem at '{location}'"
        ) from error
    finally:
        for path in (renamed, contender, probe_root):
            try:
                os.rmdir(path)
            except OSError:
                pass


def _cleanup_probe_artifacts(location: str) -> None:
    cutoff = time.time() - _ARTIFACT_RETENTION_SECONDS
    try:
        entries = list(os.scandir(location))
    except FileNotFoundError:
        return
    except OSError as error:
        raise InformixError(
            f"Cannot inspect Informix shared-state location '{location}'"
        ) from error
    for entry in entries:
        try:
            if (
                not entry.name.startswith(".informix-probe-")
                or not entry.is_dir(follow_symlinks=False)
                or entry.stat(follow_symlinks=False).st_mtime > cutoff
            ):
                continue
            for child_name in ("exclusive", "renamed"):
                try:
                    os.rmdir(os.path.join(entry.path, child_name))
                except FileNotFoundError:
                    pass
            os.rmdir(entry.path)
        except FileNotFoundError:
            continue
        except OSError as error:
            raise InformixError(
                f"Cannot clean abandoned Informix probe '{entry.path}'"
            ) from error


def recover_shared_state_lock(
    state_location: str,
    lock_path: str,
    expected_token: str,
    *,
    acknowledge_pipelines_stopped: bool,
) -> None:
    """Remove one abandoned lock after explicit, ownership-checked operator recovery."""

    location = os.path.normpath(state_location)
    path = os.path.normpath(lock_path)
    if not acknowledge_pipelines_stopped:
        raise ValueError("Lock recovery requires acknowledgement that all pipelines are stopped")
    if os.path.commonpath((location, path)) != location or not path.endswith(".lock"):
        raise ValueError("Lock path must be a .lock directory under the shared-state location")
    if not re.fullmatch(r"[0-9a-f]{32}", expected_token):
        raise ValueError("expected_token must be the 32-character owner token")
    owner_path = os.path.join(path, "owner.json")
    try:
        with open(owner_path, encoding="utf-8") as handle:
            owner = json.load(handle)
        if not isinstance(owner, dict) or owner.get("token") != expected_token:
            raise InformixError("Shared-state lock owner token changed; recovery aborted")
        tombstone = f"{path}.{expected_token}.recovered"
        os.rename(path, tombstone)
        os.unlink(os.path.join(tombstone, "owner.json"))
        os.rmdir(tombstone)
    except FileNotFoundError as error:
        raise InformixError(f"Shared-state lock '{path}' no longer exists") from error
    except (OSError, json.JSONDecodeError) as error:
        raise InformixError(f"Cannot recover shared-state lock '{path}'") from error


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
        self._trigger_high_water: int | None = None
        self._trigger_generation: str | None = None
        configured_scope = str(options.get("pipeline.id", ""))
        self._registration_scope = (
            hashlib.sha256(configured_scope.encode()).hexdigest()[:32]
            if configured_scope
            else None
        )

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
                owner=not deletes,
            )
        max_rows = self._table_int_option(
            options, "max.records.per.batch", _DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        stop_lsn: int | None = None
        trigger_generation: str | None = None
        if self._trigger_available_now:
            stop_lsn, trigger_generation = self._shared_trigger_boundary(
                table, checkpoint, owner=not deletes
            )
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
            and (self._trigger_high_water is None or transition_lsn <= self._trigger_high_water)
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
        *,
        owner: bool,
    ) -> None:
        directory, state_path, lock_path = self._shared_table_state_paths(table)
        if not owner:
            return
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            state = self._read_shared_table_state(state_path, table)
            if state is not None and _state_schema(
                state, _schema_fingerprint(table), schema_id=checkpoint_schema_id
            ) is not None:
                return
            lock_token = self._acquire_shared_state_lock(directory, lock_path)
            if lock_token is not None:
                try:
                    state = self._read_shared_table_state(state_path, table)
                    if state is None:
                        minimum, current = (
                            self._bridge.minimum_lsn(),
                            self._bridge.current_lsn(),
                        )
                        if not minimum <= checkpoint_lsn <= current:
                            raise InformixError(
                                f"Cannot rebuild shared schema state for "
                                f"'{table.exposed_name}' from checkpoint LSN {checkpoint_lsn}; "
                                f"retained/current range is [{minimum}, {current}]"
                            )
                        self._bridge.validate_initial_lsn(
                            _capture_descriptor(table, _client_encoding(self.options)),
                            checkpoint_lsn,
                        )
                        node = _schema_state(
                            table, checkpoint_lsn, schema_id=checkpoint_schema_id
                        )
                        self._renew_shared_state_lock(lock_path, lock_token)
                        self._write_shared_table_state(
                            state_path, table, checkpoint_lsn, node
                        )
                        return
                    if _state_schema(
                        state,
                        _schema_fingerprint(table),
                        schema_id=checkpoint_schema_id,
                    ) is not None:
                        return
                    schemas = list(state.get("schemas", []))
                    schemas.append(
                        _schema_state(
                            table, checkpoint_lsn, schema_id=checkpoint_schema_id
                        )
                    )
                    state["schemas"] = schemas
                    state["active_schema_id"] = checkpoint_schema_id
                    self._renew_shared_state_lock(lock_path, lock_token)
                    self._write_shared_state(state_path, state)
                    return
                finally:
                    self._release_shared_state_lock(lock_path, lock_token)
            if time.monotonic() >= deadline:
                raise InformixError(
                    f"Timed out seeding schema history for '{table.exposed_name}'. "
                    f"{self._lock_recovery_detail(lock_path)}"
                )
            time.sleep(0.1)

    def _shared_trigger_boundary(
        self, table: Table, checkpoint: dict[str, Any], *, owner: bool
    ) -> tuple[int, str]:
        if self._trigger_high_water is not None and self._trigger_generation is not None:
            return self._trigger_high_water, self._trigger_generation
        directory, state_path, lock_path = self._shared_table_state_paths(table)
        # Both independently checkpointed readers present the same predecessor
        # after every successfully coordinated trigger. Keying the next boundary
        # by that durable predecessor avoids relying on Lakeflow runtime IDs,
        # which are not exposed to Python data-source workers.
        predecessor = str(checkpoint.get("trigger_generation", "initial"))
        scope = self._pipeline_scope(checkpoint)
        boundary_key = hashlib.sha256(
            "\0".join(
                (
                    scope,
                    str(checkpoint.get("schema_id", "")),
                    str(checkpoint.get("commit_lsn", "0")),
                    predecessor,
                )
            ).encode()
        ).hexdigest()
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            state = self._read_shared_table_state(state_path, table)
            boundaries = state.get("trigger_boundaries", {}) if state is not None else {}
            trigger = boundaries.get(boundary_key) if isinstance(boundaries, dict) else None
            if trigger is None and not owner and isinstance(boundaries, dict):
                candidates = [
                    value
                    for value in boundaries.values()
                    if isinstance(value, dict)
                    and value.get("scope") == scope
                    and value.get("generation") != predecessor
                    and int(value.get("high_water", 0))
                    >= int(checkpoint.get("commit_lsn", 0))
                ]
                if candidates:
                    trigger = max(
                        candidates, key=lambda value: float(value.get("created_at", 0))
                    )
            if isinstance(trigger, dict):
                generation = trigger.get("generation")
                try:
                    high_water = int(trigger["high_water"])
                except (KeyError, TypeError, ValueError) as error:
                    raise InformixError(
                        f"Invalid shared trigger boundary for '{table.exposed_name}'"
                    ) from error
                if not isinstance(generation, str) or not re.fullmatch(
                    r"[0-9a-f]{32}", generation
                ):
                    raise InformixError(
                        f"Invalid shared trigger generation for '{table.exposed_name}'"
                    )
                self._trigger_high_water = high_water
                self._trigger_generation = generation
                return high_water, generation
            token = self._acquire_shared_state_lock(directory, lock_path) if owner else None
            if token is not None:
                try:
                    state = self._read_shared_table_state(state_path, table)
                    if state is None:
                        raise InformixError(
                            f"Shared CDC state is missing for '{table.exposed_name}'"
                        )
                    boundaries = state.get("trigger_boundaries", {})
                    if not isinstance(boundaries, dict):
                        raise InformixError(
                            f"Invalid shared trigger boundaries for '{table.exposed_name}'"
                        )
                    existing = boundaries.get(boundary_key)
                    if isinstance(existing, dict):
                        high_water = int(existing["high_water"])
                        generation = str(existing["generation"])
                        self._trigger_high_water = high_water
                        self._trigger_generation = generation
                        return high_water, generation
                    high_water = self._bridge.current_lsn()
                    if high_water < int(checkpoint["commit_lsn"]):
                        raise InformixError(
                            f"Current LSN {high_water} precedes checkpoint LSN "
                            f"{checkpoint['commit_lsn']} for '{table.exposed_name}'"
                        )
                    generation = secrets.token_hex(16)
                    boundaries[boundary_key] = {
                        "created_at": time.time(),
                        "generation": generation,
                        "high_water": str(high_water),
                        "predecessor": predecessor,
                        "scope": scope,
                    }
                    state["trigger_boundaries"] = boundaries
                    self._renew_shared_state_lock(lock_path, token)
                    self._write_shared_state(state_path, state)
                    self._trigger_high_water = high_water
                    self._trigger_generation = generation
                    return high_water, generation
                finally:
                    self._release_shared_state_lock(lock_path, token)
            if time.monotonic() >= deadline:
                role = "upsert reader" if owner else "the table's upsert reader"
                raise InformixError(
                    f"Timed out waiting for {role} to publish a triggered boundary for "
                    f"'{table.exposed_name}'. {self._lock_recovery_detail(lock_path)}"
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
        directory, state_path, lock_path = self._shared_table_state_paths(table)
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            state = self._read_shared_table_state(state_path, table)
            if state is None:
                raise InformixError(
                    f"Shared CDC state is missing for schema transition on "
                    f"'{table.exposed_name}'"
                )
            previous = _state_schema(state, schema_id=checkpoint_schema_id)
            if previous is None:
                raise InformixError(
                    f"Schema history for checkpoint node {checkpoint_schema_id} is missing for "
                    f"'{table.exposed_name}'; run a full refresh"
                )
            previous_table = _table_from_schema_state(previous, table.database)
            _ensure_additive_schema_change(previous_table, table)
            current = _active_descendant_schema(
                state, checkpoint_schema_id, _schema_fingerprint(table)
            )
            if current is not None:
                target = _next_schema_transition(
                    state, checkpoint_schema_id, str(current["id"])
                )
                target_table, transition = self._validate_schema_transition(
                    state, previous, target, table, checkpoint_lsn
                )
                return previous_table, target_table, transition, str(target["id"])
            lock_token = self._acquire_shared_state_lock(directory, lock_path) if owner else None
            if lock_token is not None:
                try:
                    state = self._read_shared_table_state(state_path, table)
                    if state is None:
                        raise InformixError(
                            f"Shared CDC state disappeared for '{table.exposed_name}'"
                        )
                    current = _active_descendant_schema(
                        state, checkpoint_schema_id, _schema_fingerprint(table)
                    )
                    if current is None:
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
                        schemas = list(state.get("schemas", []))
                        node = _schema_state(
                            table,
                            transition,
                            predecessor=checkpoint_schema_id,
                        )
                        schemas.append(node)
                        state["schemas"] = schemas
                        state["active_schema_id"] = node["id"]
                        self._renew_shared_state_lock(lock_path, lock_token)
                        self._write_shared_state(state_path, state)
                        return previous_table, table, transition, str(node["id"])
                    target = _next_schema_transition(
                        state, checkpoint_schema_id, str(current["id"])
                    )
                    target_table, transition = self._validate_schema_transition(
                        state,
                        previous,
                        target,
                        table,
                        checkpoint_lsn,
                    )
                    return previous_table, target_table, transition, str(target["id"])
                finally:
                    self._release_shared_state_lock(lock_path, lock_token)
            if time.monotonic() >= deadline:
                raise InformixError(
                    f"Timed out waiting for the upsert reader to publish schema transition "
                    f"state for '{table.exposed_name}'. "
                    f"{self._lock_recovery_detail(lock_path)}"
                )
            time.sleep(0.1)

    def _validate_schema_transition(
        self,
        state: dict[str, object],
        previous: dict[str, object],
        current: dict[str, object],
        table: Table,
        checkpoint_lsn: int,
    ) -> tuple[Table, int]:
        _validate_schema_history(state, table)
        previous_lsn = int(previous["start_lsn"])
        transition = int(current["start_lsn"])
        target_table = _table_from_schema_state(current, table.database)
        _ensure_additive_schema_change(
            _table_from_schema_state(previous, table.database), target_table
        )
        if transition <= previous_lsn:
            raise InformixError(
                f"Schema transition for '{table.exposed_name}' is not monotonic"
            )
        minimum, now = self._bridge.minimum_lsn(), self._bridge.current_lsn()
        if not minimum <= transition <= now:
            raise InformixError(
                f"Schema transition LSN {transition} for '{table.exposed_name}' is outside "
                f"retained/current range [{minimum}, {now}]"
            )
        if transition < checkpoint_lsn:
            raise InformixError(
                f"Schema transition LSN {transition} precedes checkpoint LSN "
                f"{checkpoint_lsn} for '{table.exposed_name}'"
            )
        return target_table, transition

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
        directory, state_path, lock_path = self._shared_table_state_paths(table)
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            state = self._read_shared_table_state(state_path, table)
            if state is not None:
                active = _state_schema(
                    state, schema_id=str(state.get("active_schema_id"))
                )
                schema_known = (
                    active is not None
                    and active.get("fingerprint") == _schema_fingerprint(table)
                )
                value = int(active["start_lsn"]) if schema_known else 0
                if not owner and schema_known:
                    snapshots = state.get("snapshot_boundaries", {})
                    snapshot_key = hashlib.sha256(
                        "\0".join(
                            (
                                self._pipeline_scope(),
                                str(active.get("id")),
                            )
                        ).encode()
                    ).hexdigest()
                    snapshot = (
                        snapshots.get(snapshot_key)
                        if isinstance(snapshots, dict)
                        else None
                    )
                    if (
                        isinstance(snapshot, dict)
                        and snapshot.get("schema_id") == active.get("id")
                    ):
                        value = int(snapshot["snapshot_lsn"])
                    else:
                        schema_known = False
                if (
                    schema_known
                    and self._bridge.minimum_lsn() <= value <= self._bridge.current_lsn()
                ):
                    self._bridge.validate_initial_lsn(
                        _capture_descriptor(table, _client_encoding(self.options)), value
                    )
                    return value, str(active["id"])
                if not owner:
                    state = None
            lock_token = self._acquire_shared_state_lock(directory, lock_path) if owner else None
            if lock_token is not None:
                try:
                    state = self._read_shared_table_state(state_path, table)
                    if state is not None:
                        active = _state_schema(
                            state, schema_id=str(state.get("active_schema_id"))
                        )
                        schema_known = (
                            active is not None
                            and active.get("fingerprint") == _schema_fingerprint(table)
                        )
                        value = int(active["start_lsn"]) if schema_known else 0
                        if (
                            schema_known
                            and self._bridge.minimum_lsn()
                            <= value
                            <= self._bridge.current_lsn()
                        ):
                            return value, str(active["id"])
                    value = self._bridge.prepare_initial_capture([table.native_identity])
                    self._bridge.validate_initial_lsn(
                        _capture_descriptor(table, _client_encoding(self.options)), value
                    )
                    node = _schema_state(table, value)
                    self._renew_shared_state_lock(lock_path, lock_token)
                    if state is None or not state.get("schemas"):
                        self._write_shared_table_state(state_path, table, value, node)
                    else:
                        schemas = list(state["schemas"])
                        schemas.append(node)
                        state["schemas"] = schemas
                        state["lsn"] = str(value)
                        state["active_schema_id"] = node["id"]
                        state["created_at"] = time.time()
                        self._write_shared_state(state_path, state)
                    return value, str(node["id"])
                finally:
                    self._release_shared_state_lock(lock_path, lock_token)
            if time.monotonic() >= deadline:
                role = "upsert initialization" if owner else "the table's upsert reader"
                raise InformixError(
                    f"Timed out waiting for {role} to publish shared CDC state for "
                    f"'{table.exposed_name}' at '{state_path}'. "
                    f"{self._lock_recovery_detail(lock_path)}"
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
        directory, state_path, lock_path = self._shared_table_state_paths(table)
        deadline = time.monotonic() + int(
            self.options.get(
                "cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS)
            )
        )
        while True:
            token = self._acquire_shared_state_lock(directory, lock_path)
            if token is not None:
                try:
                    state = self._read_shared_table_state(state_path, table)
                    if state is None:
                        raise InformixError(
                            f"Shared CDC state disappeared for '{table.exposed_name}'"
                        )
                    snapshots = state.get("snapshot_boundaries", {})
                    if not isinstance(snapshots, dict):
                        raise InformixError(
                            f"Invalid shared snapshot boundaries for '{table.exposed_name}'"
                        )
                    snapshot_key = hashlib.sha256(
                        "\0".join(
                            (pipeline_scope, schema_id)
                        ).encode()
                    ).hexdigest()
                    snapshots[snapshot_key] = {
                        "created_at": time.time(),
                        "initial_lsn": str(initial_lsn),
                        "schema_id": schema_id,
                        "snapshot_lsn": str(snapshot_lsn),
                    }
                    state["snapshot_boundaries"] = snapshots
                    self._renew_shared_state_lock(lock_path, token)
                    self._write_shared_state(state_path, state)
                    return
                finally:
                    self._release_shared_state_lock(lock_path, token)
            if time.monotonic() >= deadline:
                raise InformixError(
                    f"Timed out publishing snapshot boundary for '{table.exposed_name}'. "
                    f"{self._lock_recovery_detail(lock_path)}"
                )
            time.sleep(0.1)

    def _shared_table_state_paths(self, table: Table) -> tuple[str, str, str]:
        hostname = self.options.get("hostname", "").strip().rstrip(".").casefold()
        port = str(int(self.options.get("port", "9088")))
        server = self.options.get("server", "").strip()
        database = self.options.get("database", "").strip()
        namespace = "\0".join(
            ("v2", hostname, port, server, database)
        )
        connection_key = hashlib.sha256(namespace.encode()).hexdigest()[:24]
        table_key = hashlib.sha256(table.native_identity.encode()).hexdigest()[:24]
        directory = os.path.join(self._shared_state_location, connection_key)
        state_path = os.path.join(directory, f"{table_key}.json")
        return directory, state_path, f"{state_path}.lock"

    def _read_shared_table_state(
        self, path: str, table: Table
    ) -> dict[str, object] | None:
        try:
            if os.stat(path).st_size > _MAX_SHARED_STATE_BYTES:
                raise InformixError(
                    f"Informix shared CDC state '{path}' exceeds "
                    f"{_MAX_SHARED_STATE_BYTES} bytes"
                )
            with open(path, encoding="utf-8") as handle:
                payload = handle.read(_MAX_SHARED_STATE_BYTES + 1)
            if len(payload.encode("utf-8")) > _MAX_SHARED_STATE_BYTES:
                raise InformixError(
                    f"Informix shared CDC state '{path}' exceeds "
                    f"{_MAX_SHARED_STATE_BYTES} bytes"
                )
            state = json.loads(payload)
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError) as error:
            raise InformixError(f"Cannot read Informix shared CDC state '{path}'") from error
        if not isinstance(state, dict):
            raise InformixError(f"Invalid Informix shared CDC state object in '{path}'")
        if state.get("version") == 1:
            state = _upgrade_legacy_schema_state(state)
        elif state.get("version") in (2, 3, 4):
            state = dict(state)
            state["version"] = _SHARED_STATE_VERSION
            state.pop("snapshot_boundary", None)
            state.pop("trigger", None)
            state.pop("snapshot_boundaries", None)
            state.pop("trigger_boundaries", None)
        elif state.get("version") != _SHARED_STATE_VERSION:
            raise InformixError(f"Unsupported Informix shared CDC state version in '{path}'")
        if state.get("table") != table.native_identity:
            raise InformixError(f"Informix shared CDC state table mismatch in '{path}'")
        try:
            if int(state["lsn"]) < 1:
                raise ValueError
        except (KeyError, TypeError, ValueError) as error:
            raise InformixError(f"Invalid Informix shared CDC LSN in '{path}'") from error
        schemas = state.get("schemas", [])
        if not isinstance(schemas, list):
            raise InformixError(f"Invalid Informix shared CDC schema history in '{path}'")
        if schemas:
            _validate_schema_history(state, table)
            active_schema_id = state.get("active_schema_id")
            if not isinstance(active_schema_id, str) or _state_schema(
                state, schema_id=active_schema_id
            ) is None:
                raise InformixError(
                    f"Invalid Informix shared CDC active schema in '{path}'"
                )
        snapshots = state.get("snapshot_boundaries", {})
        if not isinstance(snapshots, dict):
            raise InformixError(f"Invalid Informix shared snapshot boundaries in '{path}'")
        for key, snapshot in snapshots.items():
            try:
                if (
                    not isinstance(key, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", key)
                    or not isinstance(snapshot, dict)
                    or not isinstance(snapshot["schema_id"], str)
                    or int(snapshot["initial_lsn"]) < 1
                    or int(snapshot["snapshot_lsn"]) < int(snapshot["initial_lsn"])
                    or not math.isfinite(float(snapshot["created_at"]))
                ):
                    raise ValueError
            except (KeyError, TypeError, ValueError) as error:
                raise InformixError(
                    f"Invalid Informix shared snapshot boundary in '{path}'"
                ) from error
        boundaries = state.get("trigger_boundaries", {})
        if not isinstance(boundaries, dict):
            raise InformixError(f"Invalid Informix shared trigger boundaries in '{path}'")
        for key, trigger in boundaries.items():
            try:
                if (
                    not isinstance(key, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", key)
                    or not isinstance(trigger, dict)
                    or not isinstance(trigger["generation"], str)
                    or not re.fullmatch(r"[0-9a-f]{32}", trigger["generation"])
                    or int(trigger["high_water"]) < 1
                    or not math.isfinite(float(trigger["created_at"]))
                    or not isinstance(trigger["predecessor"], str)
                    or not isinstance(trigger["scope"], str)
                    or not re.fullmatch(r"[0-9a-f]{32}", trigger["scope"])
                ):
                    raise ValueError
            except (KeyError, TypeError, ValueError) as error:
                raise InformixError(
                    f"Invalid Informix shared trigger boundary in '{path}'"
                ) from error
        return state

    def _acquire_shared_state_lock(self, directory: str, lock_path: str) -> str | None:
        _validate_shared_state_filesystem(self._shared_state_location)
        token = secrets.token_hex(16)
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)
            os.mkdir(lock_path, mode=0o700)
        except FileExistsError:
            return None
        except OSError as error:
            raise InformixError(
                f"Cannot create Informix shared CDC state lock '{lock_path}'"
            ) from error
        owner_path = os.path.join(lock_path, "owner.json")
        temporary = os.path.join(lock_path, f"owner.{token}.tmp")
        try:
            with open(temporary, "x", encoding="utf-8") as handle:
                json.dump(
                    {
                        "created_at": time.time(),
                        "host": socket.gethostname(),
                        "pid": os.getpid(),
                        "token": token,
                    },
                    handle,
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, owner_path)
        except OSError as error:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            try:
                os.rmdir(lock_path)
            except OSError:
                pass
            raise InformixError(
                f"Cannot initialize Informix shared CDC state lock '{lock_path}'"
            ) from error
        try:
            self._cleanup_shared_state_artifacts(directory, lock_path)
        except Exception:
            self._release_shared_state_lock(lock_path, token)
            raise
        return token

    def _cleanup_shared_state_artifacts(self, directory: str, lock_path: str) -> None:
        cutoff = time.time() - _ARTIFACT_RETENTION_SECONDS
        state_name = os.path.basename(lock_path.removesuffix(".lock"))
        try:
            entries = list(os.scandir(directory))
        except FileNotFoundError:
            return
        except OSError as error:
            raise InformixError(
                f"Cannot inspect Informix shared-state directory '{directory}'"
            ) from error
        for entry in entries:
            try:
                if entry.stat(follow_symlinks=False).st_mtime > cutoff:
                    continue
                if entry.is_file(follow_symlinks=False) and (
                    entry.name.startswith(f"{state_name}.") and entry.name.endswith(".tmp")
                ):
                    os.unlink(entry.path)
                elif entry.is_dir(follow_symlinks=False) and (
                    entry.name.startswith(f"{state_name}.lock.")
                    and entry.name.endswith(".released")
                ):
                    try:
                        os.unlink(os.path.join(entry.path, "owner.json"))
                    except FileNotFoundError:
                        pass
                    os.rmdir(entry.path)
            except FileNotFoundError:
                continue
            except OSError as error:
                raise InformixError(
                    f"Cannot clean abandoned Informix shared-state artifact '{entry.path}'"
                ) from error

    def _release_shared_state_lock(self, lock_path: str, token: str) -> None:
        owner_path = os.path.join(lock_path, "owner.json")
        try:
            with open(owner_path, encoding="utf-8") as handle:
                owner = json.load(handle)
            if not isinstance(owner, dict) or owner.get("token") != token:
                return
            tombstone = f"{lock_path}.{token}.released"
            os.rename(lock_path, tombstone)
            os.unlink(os.path.join(tombstone, "owner.json"))
            os.rmdir(tombstone)
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as error:
            raise InformixError(
                f"Cannot release Informix shared CDC state lock '{lock_path}'"
            ) from error

    def _lock_recovery_detail(self, lock_path: str) -> str:
        owner_path = os.path.join(lock_path, "owner.json")
        try:
            with open(owner_path, encoding="utf-8") as handle:
                owner = json.load(handle)
            created_at = float(owner["created_at"])
            host = str(owner["host"])
            pid = int(owner["pid"])
            token = str(owner["token"])
            age = max(0, int(time.time() - created_at))
            owner_detail = f"owner {host} pid {pid}, age {age}s, token {token}"
        except (FileNotFoundError, OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            owner_detail = "owner metadata unavailable"
        return (
            f"Lock '{lock_path}' is held ({owner_detail}). If its worker terminated, stop "
            "every pipeline using this connection, remove that .lock directory, and restart"
        )

    def _renew_shared_state_lock(self, lock_path: str, token: str) -> None:
        """Assert ownership immediately before publishing shared state."""

        owner_path = os.path.join(lock_path, "owner.json")
        try:
            with open(owner_path, encoding="utf-8") as handle:
                owner = json.load(handle)
                if not isinstance(owner, dict) or owner.get("token") != token:
                    raise InformixError(
                        f"Lost Informix shared CDC state lock '{lock_path}'"
                    )
        except FileNotFoundError as error:
            raise InformixError(
                f"Lost Informix shared CDC state lock '{lock_path}'"
            ) from error
        except (OSError, json.JSONDecodeError) as error:
            raise InformixError(
                f"Cannot renew Informix shared CDC state lock '{lock_path}'"
            ) from error

    def _write_shared_table_state(
        self,
        path: str,
        table: Table,
        lsn: int,
        node: dict[str, object] | None = None,
    ) -> None:
        schema = node or _schema_state(table, lsn)
        state = {
            "version": _SHARED_STATE_VERSION,
            "table": table.native_identity,
            "lsn": str(lsn),
            "active_schema_id": schema["id"],
            "created_at": time.time(),
            "schemas": [schema],
        }
        self._write_shared_state(path, state)

    def _write_shared_state(self, path: str, state: dict[str, object]) -> None:
        payload = json.dumps(state, separators=(",", ":"), sort_keys=True)
        if len(payload.encode("utf-8")) > _MAX_SHARED_STATE_BYTES:
            raise InformixError(
                f"Informix shared CDC state '{path}' exceeds {_MAX_SHARED_STATE_BYTES} bytes; "
                "stop all pipelines using this connection, configure a new shared-state "
                "location, and perform full refreshes"
            )
        temporary = f"{path}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        try:
            with open(temporary, "x", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            try:
                directory_descriptor = os.open(os.path.dirname(path), os.O_RDONLY)
            except OSError as error:
                if error.errno in {errno.EINVAL, errno.ENOTSUP, errno.EACCES}:
                    directory_descriptor = None
                else:
                    raise
            try:
                if directory_descriptor is not None:
                    try:
                        os.fsync(directory_descriptor)
                    except OSError as error:
                        if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                            raise
            finally:
                if directory_descriptor is not None:
                    os.close(directory_descriptor)
        except OSError as error:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise InformixError(f"Cannot publish Informix shared CDC state '{path}'") from error

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
        "INT8",
        "SERIAL8",
        "DATETIME",
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "INTERVAL",
        "LVARCHAR",
        "NCHAR",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    return {
        "name": name,
        "type_name": type_name,
        "nullable": not bool(raw_type & 0x100),
        "length": length,
        "precision": precision,
        "scale": scale,
        "cdc_supported": type_name not in unsupported,
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
        "LVARCHAR",
        "NCHAR",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    return tuple(column for column in table.columns if column.type_name in unsupported)


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
    result = int(value)
    if result < 0:
        raise InformixError(f"CDC record has a negative LSN: {record!r}")
    return result


def _tx_id(record: dict[str, Any]) -> int:
    value = record.get("tx_id", record.get("transaction_id"))
    if value is None:
        raise InformixError(f"CDC record has no transaction ID: {record!r}")
    return int(value)


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
        if _operation(record) not in {"METADATA", "ERROR", "DISCARD"}:
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


def _state_schema(
    state: dict[str, object], fingerprint: str | None = None, *, schema_id: str | None = None
) -> dict[str, object] | None:
    schemas = state.get("schemas", [])
    if not isinstance(schemas, list):
        raise InformixError("Informix shared CDC schemas must be a list")
    if fingerprint is None and schema_id is None:
        raise InformixError("Informix shared CDC schema lookup requires an id or fingerprint")
    matches = [schema for schema in schemas if isinstance(schema, dict)]
    if schema_id is not None:
        matches = [schema for schema in matches if schema.get("id") == schema_id]
    if fingerprint is not None:
        matches = [schema for schema in matches if schema.get("fingerprint") == fingerprint]
    if len(matches) > 1:
        raise InformixError(
            f"Ambiguous Informix shared CDC schema lookup id={schema_id!r}, "
            f"fingerprint={fingerprint!r}"
        )
    return matches[0] if matches else None


def _table_from_schema_state(state: dict[str, object], default_database: str) -> Table:
    raw = state.get("table")
    if not isinstance(raw, dict):
        raise InformixError("Informix shared CDC schema is missing table metadata")
    try:
        start_lsn = int(state["start_lsn"])
    except (KeyError, TypeError, ValueError) as error:
        raise InformixError("Informix shared CDC schema has an invalid start LSN") from error
    if start_lsn < 1:
        raise InformixError("Informix shared CDC schema has an invalid start LSN")
    table = Table.parse(raw, default_database)
    if _schema_fingerprint(table) != state.get("fingerprint"):
        raise InformixError("Informix shared CDC schema fingerprint does not match metadata")
    return table


def _upgrade_legacy_schema_state(state: dict[str, object]) -> dict[str, object]:
    """Normalize version-1 fingerprint-linked history for a version-4 full refresh."""

    schemas = state.get("schemas", [])
    if not isinstance(schemas, list):
        raise InformixError("Informix shared CDC schemas must be a list")
    upgraded = dict(state)
    upgraded_schemas: list[dict[str, object]] = []
    fingerprint_ids: dict[str, str] = {}
    for index, raw in enumerate(schemas):
        if not isinstance(raw, dict):
            raise InformixError("Informix shared CDC schema history contains a non-object")
        schema = dict(raw)
        fingerprint = str(schema.get("fingerprint", ""))
        identity = hashlib.sha256(
            f"legacy\0{index}\0{fingerprint}\0{schema.get('start_lsn')}".encode()
        ).hexdigest()[:32]
        has_predecessor = "predecessor" in schema
        predecessor = schema.get("predecessor")
        if not has_predecessor and index:
            predecessor_id = str(upgraded_schemas[-1]["id"])
        elif predecessor is None:
            predecessor_id = None
        else:
            predecessor_id = fingerprint_ids.get(str(predecessor))
            if predecessor_id is None:
                raise InformixError(
                    "Informix legacy shared CDC schema predecessor is missing"
                )
        schema["id"] = identity
        schema["predecessor"] = predecessor_id
        upgraded_schemas.append(schema)
        fingerprint_ids[fingerprint] = identity
    upgraded["version"] = _SHARED_STATE_VERSION
    upgraded["schemas"] = upgraded_schemas
    if upgraded_schemas:
        upgraded["active_schema_id"] = upgraded_schemas[-1]["id"]
    return upgraded


def _validate_schema_history(state: dict[str, object], expected_table: Table) -> None:
    schemas = state.get("schemas", [])
    if not isinstance(schemas, list):
        raise InformixError("Informix shared CDC schema history has invalid bounds")
    validated: dict[str, tuple[Table, int]] = {}
    for index, schema in enumerate(schemas):
        if not isinstance(schema, dict):
            raise InformixError("Informix shared CDC schema history contains a non-object")
        table = _table_from_schema_state(schema, expected_table.database)
        if table.native_identity != expected_table.native_identity:
            raise InformixError("Informix shared CDC schema history table identity mismatch")
        schema_id = schema.get("id")
        if not isinstance(schema_id, str) or not re.fullmatch(r"[0-9a-f]{32}", schema_id):
            raise InformixError("Informix shared CDC schema has an invalid id")
        if schema_id in validated:
            raise InformixError(f"Duplicate Informix shared CDC schema id {schema_id}")
        start_lsn = int(schema["start_lsn"])
        predecessor = _schema_predecessor(schemas, index)
        if predecessor is not None:
            if predecessor not in validated:
                raise InformixError(
                    f"Informix shared CDC schema predecessor {predecessor} is missing"
                )
            previous_table, previous_lsn = validated[predecessor]
            if start_lsn <= previous_lsn:
                raise InformixError(
                    "Informix shared CDC schema transition LSN is not monotonic"
                )
            _ensure_additive_schema_change(previous_table, table)
        validated[schema_id] = (table, start_lsn)


def _schema_predecessor(
    schemas: list[object], index: int
) -> str | None:
    schema = schemas[index]
    if not isinstance(schema, dict):
        raise InformixError("Informix shared CDC schema history contains a non-object")
    if "predecessor" in schema:
        predecessor = schema["predecessor"]
        if predecessor is not None and not isinstance(predecessor, str):
            raise InformixError("Informix shared CDC schema predecessor is invalid")
        return predecessor
    # Migrate the original linear history representation in place logically:
    # its first entry is a root and every later entry follows the previous one.
    if index == 0:
        return None
    previous = schemas[index - 1]
    if not isinstance(previous, dict) or not isinstance(previous.get("id"), str):
        raise InformixError("Informix shared CDC schema predecessor is invalid")
    return previous["id"]


def _next_schema_transition(
    state: dict[str, object], checkpoint_schema_id: str, current_schema_id: str
) -> dict[str, object]:
    schemas = state.get("schemas", [])
    if not isinstance(schemas, list):
        raise InformixError("Informix shared CDC schemas must be a list")
    by_id = {
        str(schema.get("id")): (index, schema)
        for index, schema in enumerate(schemas)
        if isinstance(schema, dict)
    }
    if current_schema_id not in by_id:
        raise InformixError(f"Current Informix schema node {current_schema_id} is missing")
    path: list[dict[str, object]] = []
    cursor = current_schema_id
    while True:
        index, schema = by_id[cursor]
        path.append(schema)
        if cursor == checkpoint_schema_id:
            break
        predecessor = _schema_predecessor(schemas, index)
        if predecessor is None or predecessor not in by_id:
            raise InformixError(
                "Current Informix schema is in a different history generation; "
                "run a full refresh for this pipeline"
            )
        cursor = predecessor
    path.reverse()
    if len(path) < 2:
        raise InformixError("Schema transition path has no successor")
    return path[1]


def _active_descendant_schema(
    state: dict[str, object], ancestor_id: str, fingerprint: str
) -> dict[str, object] | None:
    """Return the active schema only when it descends from the checkpoint node."""

    schemas = state.get("schemas", [])
    if not isinstance(schemas, list):
        raise InformixError("Informix shared CDC schemas must be a list")
    active_id = state.get("active_schema_id")
    if not isinstance(active_id, str):
        raise InformixError("Informix shared CDC state has an invalid active schema id")
    by_id = {
        str(schema.get("id")): (index, schema)
        for index, schema in enumerate(schemas)
        if isinstance(schema, dict)
    }
    if active_id not in by_id:
        raise InformixError("Informix shared CDC active schema is missing")
    cursor = active_id
    while True:
        index, schema = by_id[cursor]
        if cursor == ancestor_id:
            active = by_id[active_id][1]
            return active if active.get("fingerprint") == fingerprint else None
        predecessor = _schema_predecessor(schemas, index)
        if predecessor is None or predecessor not in by_id:
            return None
        cursor = predecessor


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
        values[key] = int(result[key])
        if values[key] < 0:
            raise ValueError(f"Informix offset '{key}' must be non-negative")
        if values[key] >= 1 << 64:
            raise ValueError(f"Informix offset '{key}' exceeds the unsigned 64-bit LSN domain")
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
    if phase == "snapshot":
        if "snapshot_lsn" not in result or int(result["snapshot_lsn"]) < 0:
            raise ValueError("Informix snapshot offset has an invalid snapshot_lsn")
        if any(values[key] != int(result["snapshot_lsn"]) for key in values):
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
    "recover_shared_state_lock",
    "set_bridge_factory",
]
