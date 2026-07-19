"""Pure-Python, serverless-capable Informix snapshot and CDC connector.

The SQLI, CDC framing, codec, transaction, and Lakeflow paths are covered by
source-local regression tests. A disposable Informix 15 instance has validated
normal authentication, queries, discovery, snapshots, and core transactional
CDC. Validation in an actual serverless Lakeflow pipeline is still pending.
"""

from __future__ import annotations

import fnmatch
import importlib
import re
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
    ShortType,
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
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_DATA_OPS = {"INSERT", "BEFORE_UPDATE", "AFTER_UPDATE", "DELETE", "TRUNCATE"}


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

    def snapshot_page(
        self,
        identity: str,
        columns: Sequence[str],
        primary_keys: Sequence[str],
        after: Sequence[Any] | None,
        limit: int,
    ) -> list[dict[str, Any]]: ...

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
    Lakeflow or CDC decoding code. Serverless Lakeflow pipeline validation is
    still pending.
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
        rows = self.transport.execute(
            "SELECT owner, tabname FROM systables "
            "WHERE tabtype = 'T' AND owner NOT MATCHES 'sys*' "
            "AND tabname NOT MATCHES 'sys*' ORDER BY owner, tabname"
        )
        return [
            self._describe_table(str(_field(row, "owner", 0)), str(_field(row, "tabname", 1)))
            for row in rows
        ]

    def get_table(self, identity: str) -> dict[str, Any]:
        parts = identity.split(".")
        if len(parts) != 3:
            raise InformixError(f"Invalid logical table identity {identity!r}")
        return self._describe_table(parts[1], parts[2])

    def _describe_table(self, owner: str, name: str) -> dict[str, Any]:
        columns = self.transport.execute(
            "SELECT c.colname, c.coltype, c.collength, c.colno "
            "FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
            "WHERE t.owner = ? AND t.tabname = ? ORDER BY c.colno",
            (owner, name),
        )
        keys = self.transport.execute(
            "SELECT i.part1,i.part2,i.part3,i.part4,i.part5,i.part6,i.part7,i.part8,"
            "i.part9,i.part10,i.part11,i.part12,i.part13,i.part14,i.part15,i.part16 "
            "FROM systables t JOIN sysconstraints x ON t.tabid=x.tabid "
            "JOIN sysindexes i ON x.idxname=i.idxname "
            "WHERE x.constrtype='P' AND t.owner=? AND t.tabname=?",
            (owner, name),
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
        return {
            "database": self.config["database"],
            "owner": owner,
            "name": name,
            "columns": parsed_columns,
            "primary_keys": primary_keys,
        }

    def current_lsn(self) -> int:
        row = self.transport.execute(
            "SELECT uniqid, used FROM sysmaster:syslogs WHERE is_current = 1"
        )[0]
        return (int(_field(row, "uniqid", 0)) << 32) + (int(_field(row, "used", 1)) << 12)

    def minimum_lsn(self) -> int:
        row = self.transport.execute("SELECT MIN(uniqid) AS uniqid FROM sysmaster:syslogs")[0]
        return int(_field(row, "uniqid", 0)) << 32

    def snapshot_page(self, identity, columns, primary_keys, after, limit):
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
        rows = self.transport.execute(sql, tuple(parameters))
        return [
            dict(row) if isinstance(row, dict) else dict(zip(columns, row, strict=True))
            for row in rows
        ]

    def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
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
            open_transactions = OpenTransactionRecords()
            max_transaction_records = int(self.options.get("cdc.max.transaction.records", "100000"))
            requested = int(self.options.get("cdc.read.bytes", "32000"))
            empty_reads = 0
            while (len(records) < max_records or open_transactions) and empty_reads < 1:
                chunk = self.transport.read_lodata(session, requested)
                if not chunk:
                    empty_reads += 1
                    continue
                empty_reads = 0
                for frame in parser.feed(chunk):
                    record = decode_frame(frame, labels)
                    label = record.get("label", record.get("capture_label"))
                    if record["op"] == "METADATA" and label in labels:
                        descriptors = {column.name: column for column in labels[label]}
                        names = metadata_column_names(record["metadata"])
                        if set(names) != set(descriptors):
                            raise InformixError(
                                f"CDC metadata for capture label {label} does not match "
                                "the requested columns"
                            )
                        labels[label] = tuple(descriptors[name] for name in names)
                    if label and 1 <= label <= len(tables):
                        record["table"] = tables[label - 1]["logical_identity"]
                    records.append(record)
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
            if parser.buffered_bytes:
                raise InformixError("CDC read ended with an incomplete native frame")
            return records
        finally:
            for native in captures:
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_endcapture')}(?, 0, ?)",
                    (session, native),
                )
            self.transport.execute(
                f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)", (session,)
            )


_bridge_factory: Callable[[dict[str, str]], InformixBridge] = PurePythonInformixBridge


def set_bridge_factory(factory: Callable[[dict[str, str]], InformixBridge]) -> None:
    """Set the process-wide bridge factory (primarily for deterministic tests)."""

    global _bridge_factory
    _bridge_factory = factory


def _bridge_config(options: dict[str, str]) -> dict[str, Any]:
    required = ("hostname", "database", "user", "password")
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
        known = {c.name for c in columns}
        if not columns or any(pk not in known for pk in pks):
            raise InformixError(f"Invalid metadata for {database}.{owner}.{name}")
        return cls(database, owner, name, columns, pks)


@dataclass
class Transaction:
    tx_id: int
    begin_lsn: int
    records: list[dict[str, Any]] = field(default_factory=list)
    pending_before: dict[str, Any] | None = None

    def append(self, record: dict[str, Any]) -> None:
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
            raise InformixError(str(record.get("message") or "Informix CDC error record"))
        tx_id = _tx_id(record)
        if op == "BEGIN":
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
            del self.open[tx_id]
            return None
        if op != "COMMIT":
            raise InformixError(f"Unknown Informix CDC operation {op!r}")
        if tx.pending_before is not None:
            raise InformixError(f"Transaction {tx_id} committed with unpaired BEFORE_UPDATE")
        del self.open[tx_id]
        end = _lsn(record)
        restart = min((item.begin_lsn for item in self.open.values()), default=end)
        return CommittedTransaction(tx_id, tx.begin_lsn, end, restart, tuple(tx.records))


class InformixLakeflowConnect(LakeflowConnect):
    """Pure-Python connector live-validated on disposable Informix 15.

    Normal auth, queries, discovery, snapshots, and core transactional CDC have
    been exercised; an actual serverless Lakeflow pipeline run remains pending.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        # Validate numeric configuration without opening a connection.
        for name, default, minimum in (
            ("snapshot.page.size", "1000", 1),
            ("max.records.per.batch", "1000", 1),
            ("cdc.timeout", "5", 0),
            ("cdc.max.records", "64", 1),
        ):
            if int(options.get(name, default)) < minimum:
                raise ValueError(f"Option '{name}' must be >= {minimum}")
        if int(options.get("cdc.max.records", "64")) > 256:
            raise ValueError("Option 'cdc.max.records' must be <= 256")
        self._bridge_instance: InformixBridge | None = None
        self._tables: dict[str, Table] | None = None
        self._snapshot_high_water: dict[str, int] = {}

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
        table = self._table(table_name, table_options)
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
        if not _cdc_capable(table):
            return self._read_snapshot_only(table, start_offset, table_options)
        if not start_offset or start_offset.get("phase", "snapshot") == "snapshot":
            return self._read_snapshot(table, start_offset, table_options)
        return self._read_stream(table, start_offset, table_options, deletes=False)

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        table = self._table(table_name, table_options)
        if not _cdc_capable(table):
            raise ValueError(
                f"Table '{table_name}' lacks a primary key or has columns unsupported "
                "by Informix CDC and is snapshot-only"
            )
        # The delete channel has no snapshot rows, but it must start at the
        # same pre-snapshot high-water mark.  Lakeflow checkpoints this method
        # independently from read_table().
        if not start_offset:
            high_water = self._initial_lsn(table)
            return iter(()), _offset(high_water, high_water, high_water, None, "stream")
        if start_offset.get("phase") == "snapshot":
            high_water = int(start_offset.get("snapshot_lsn", start_offset.get("commit_lsn", 0)))
            start_offset = _offset(high_water, high_water, high_water, None, "stream")
        return self._read_stream(table, start_offset, table_options, deletes=True)

    def _read_snapshot(self, table: Table, start: dict | None, options: dict[str, str]):
        page_size = int(
            options.get("snapshot.page.size", self.options.get("snapshot.page.size", "1000"))
        )
        if start:
            high_water = int(start["snapshot_lsn"])
            last_pk = start.get("snapshot", {}).get("last_pk")
        else:
            high_water = self._initial_lsn(table)
            last_pk = None
        rows = self._bridge.snapshot_page(
            table.identity,
            [c.name for c in table.columns],
            table.primary_keys,
            last_pk,
            page_size + 1,
        )
        page, has_more = rows[:page_size], len(rows) > page_size
        shaped = [_shape_snapshot(row, high_water) for row in page]
        if has_more:
            if not page:
                raise InformixError("Snapshot bridge returned an invalid empty continuation page")
            last = [page[-1][pk] for pk in table.primary_keys]
            end = _offset(high_water, high_water, high_water, None, "snapshot")
            end.update({"snapshot_lsn": str(high_water), "snapshot": {"last_pk": last}})
        else:
            end = _offset(high_water, high_water, high_water, None, "stream")
        return iter(shaped), end

    def _read_snapshot_only(self, table: Table, start: dict | None, options: dict[str, str]):
        # PK-less tables cannot be seek-paginated safely.  Read exactly once;
        # returning None signals non-checkpointable full refresh semantics.
        limit = int(
            options.get("snapshot.max.rows", self.options.get("snapshot.max.rows", "100000"))
        )
        rows = self._bridge.snapshot_page(
            table.identity, [c.name for c in table.columns], (), None, limit + 1
        )
        if len(rows) > limit:
            raise InformixError(
                f"Snapshot-only table {table.exposed_name} exceeds snapshot.max.rows={limit}"
            )
        lsn = self._bridge.current_lsn()
        return iter(_shape_snapshot(row, lsn) for row in rows), None

    def _read_stream(self, table: Table, start: dict, options: dict[str, str], deletes: bool):
        checkpoint = _validated_offset(start)
        restart = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        minimum = self._bridge.minimum_lsn()
        if restart < minimum:
            raise LogRetentionError(
                f"Restart LSN {restart} is older than minimum retained LSN "
                f"{minimum}; resnapshot required"
            )
        raw_records = self._bridge.read_changes(
            [_capture_descriptor(table)],
            restart,
            int(options.get("cdc.timeout", self.options.get("cdc.timeout", "5"))),
            int(options.get("cdc.max.records", self.options.get("cdc.max.records", "64"))),
        )
        committed = _committed_transactions(raw_records)
        recovered = _recover(committed, checkpoint)
        max_rows = int(
            options.get("max.records.per.batch", self.options.get("max.records.per.batch", "1000"))
        )
        output: list[dict[str, Any]] = []
        end = start
        for tx in recovered:
            projected = _project_transaction(tx, table, deletes)
            # Never split a transaction.  A single large transaction is
            # accepted; subsequent complete transactions wait for next poll.
            if output and len(output) + len(projected) > max_rows:
                break
            output.extend(projected)
            end = _offset(tx.commit_lsn, tx.commit_lsn, tx.restart_lsn, tx.tx_id, "stream")
            if len(output) >= max_rows:
                break
        return iter(output), end

    def _initial_lsn(self, table: Table) -> int:
        """Share the pre-snapshot high-water mark between upsert/delete channels."""

        if table.identity not in self._snapshot_high_water:
            self._snapshot_high_water[table.identity] = self._bridge.current_lsn()
        return self._snapshot_high_water[table.identity]

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
        exposed = options.get("source_table", name)
        tables = self._table_map(refresh)
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
    20: "LVARCHAR",
    21: "BLOB",
    22: "CLOB",
    23: "BOOLEAN",
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


def _spark_type(column: Column):
    name = column.type_name.split("(", 1)[0].strip()
    if name in {"SMALLINT", "INT2"}:
        return ShortType()
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
    return int(value)


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
    buffer = TransactionBuffer()
    result = []
    for record in records:
        committed = buffer.feed(record)
        if committed is not None:
            result.append(committed)
    # Open transactions are intentionally discarded.  The returned offset
    # remains before their BEGIN so a finite next call safely replays them.
    return result


def _recover(
    transactions: Sequence[CommittedTransaction], checkpoint: dict[str, Any]
) -> list[CommittedTransaction]:
    commit = int(checkpoint["commit_lsn"])
    change = int(checkpoint["change_lsn"])
    recovering = int(checkpoint.get("begin_lsn") or commit) < commit
    output = []
    for tx in transactions:
        if tx.commit_lsn < commit:
            continue
        if tx.commit_lsn == commit and change == commit:
            continue
        records = tx.records
        if tx.commit_lsn == commit or recovering:
            records = tuple(record for record in records if _lsn(record) > change)
        if records or tx.commit_lsn > commit:
            output.append(
                CommittedTransaction(tx.tx_id, tx.begin_lsn, tx.commit_lsn, tx.restart_lsn, records)
            )
        if tx.commit_lsn > commit:
            recovering = False
    return output


def _capture_descriptor(table: Table) -> dict[str, Any]:
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
                "encoding": "utf-8",
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
        {CURSOR: str(_lsn(record)), COMMIT_LSN: str(tx.commit_lsn), TX_ID: tx.tx_id, OP: op}
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
        {CURSOR: str(_lsn(record)), COMMIT_LSN: str(tx.commit_lsn), TX_ID: tx.tx_id, OP: "d"}
    )
    return result


def _shape_snapshot(row: dict[str, Any], lsn: int) -> dict[str, Any]:
    result = _framework_row(row)
    result.update({CURSOR: str(lsn), COMMIT_LSN: str(lsn), TX_ID: None, OP: "r"})
    return result


def _framework_row(row: dict[str, Any]) -> dict[str, Any]:
    return {name: _framework_value(value) for name, value in row.items()}


def _framework_value(value: Any) -> Any:
    # The shared Spark Python Data Source parser accepts ISO strings for DateType
    # and TimestampType, but rejects a native datetime.date.  Normalize both
    # temporal Python objects at the connector boundary for consistent snapshot
    # and CDC behavior.
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def _offset(commit: int, change: int, begin: int, tx_id: int | None, phase: str) -> dict:
    return {
        "commit_lsn": str(commit),
        "change_lsn": str(change),
        "begin_lsn": str(begin),
        "tx_id": tx_id,
        "phase": phase,
    }


def _validated_offset(offset: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(offset, dict):
        raise ValueError("Informix offset must be a dictionary")
    result = dict(offset)
    for key in ("commit_lsn", "change_lsn", "begin_lsn"):
        if key not in result:
            raise ValueError(f"Informix stream offset is missing '{key}'")
        int(result[key])
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
    "set_bridge_factory",
]
