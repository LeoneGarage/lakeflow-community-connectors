"""Bounded pure-Python Informix SQLI socket and message primitives.

The normal ASC username/password prefix, six-byte session header, socket/TLS
handling, response grouping, simple command request, and SQ_LODATA dispatcher
are bytecode-backed. Generic ASC tail, DESCRIBE/tuple and bind layouts remain
fail-closed where the recovered evidence is incomplete.
"""

from __future__ import annotations

import contextlib
import ipaddress
import math
import socket
import ssl
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import BinaryIO, Callable

from databricks.labs.community_connector.sources.informix.cdc_protocol import apply_null_byte
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    _CA_FILE_RETRY_SECONDS,
    _NON_ROW_STATEMENTS,
    LO_READ,
    LO_READ_WITH_SEEK,
    MAX_PACKET,
    SQ_ASCEOT,
    SQ_CLOSE,
    SQ_DONE,
    SQ_EOT,
    SQ_ERR,
    SQ_EXIT,
    SQ_INSERTDONE,
    SQ_LODATA,
    SQ_PROTOCOLS,
    AscAccept,
    AuthenticationProvider,
    ConnectionState,
    InformixDatetimeLiteral,
    PasswordAuthenticationProvider,
    ResultColumn,
    ResultDescription,
    SqliDescriptorNotImplemented,
    SqliInvalidCaFile,
    SqliProtocolError,
    SqliRedirect,
    SqliSessionRejected,
    SqliTransportError,
    SqliUnsupportedAuthentication,
    _ca_file_misnamed,
    _connector_sql,
    _DeadlineReader,
    _decode_result_value,
    _decode_text,
    _effective_result_kind,
    _fixed_result_size,
    _protocol_feature,
    _raise_connection_loss,
    _retained_size,
    _sql_error,
    _statement_keyword,
    add_informix_exception_note,
    datetime_order_preserving_cast,
    decode_asc_accept,
    decode_char,
    encode_asc_char,
    encode_asc_environment,
    encode_asc_tail,
    encode_char,
    encode_close_release,
    encode_cursor_open,
    encode_dbopen,
    encode_fetch,
    encode_fixed_open_fetch,
    encode_lodata_read,
    encode_normal_auth_prefix,
    encode_normal_auth_request,
    encode_pam_response,
    encode_prepare,
    encode_protocol_offer,
    encode_secondary_info,
    encode_session_packet,
    encode_simple_command,
    encode_variable_fetch,
    informix_locale_encoding,
    parse_redirect_detail,
    read_exact,
    read_int32,
    read_session_packet,
    read_smallint,
)


@dataclass(eq=False, repr=False)
class InformixSqliClient:
    hostname: str
    port: int
    database: str
    user: str
    password: str
    server_name: str | None = None
    db_locale: str | None = "en_US.819"
    client_locale: str | None = "en_US.utf8"
    tls: bool = True
    ssl_context: ssl.SSLContext | None = None
    ca_file: str | None = None
    pad_varchar: bool = False
    # How a decoded string containing an embedded NUL is sanitized: "keep"/"null"/"empty"
    # (see cdc_protocol.apply_null_byte). The bridge sets this from the connection option.
    null_byte: str = "keep"
    connect_timeout: float = 10.0
    socket_timeout: float = 30.0
    authentication_mode: str = "password"
    authentication_provider: AuthenticationProvider | None = None
    pam_max_rounds: int = 16
    login_timeout: float = 30.0
    redirect_enabled: bool = False
    redirect_allowlist: frozenset[tuple[str, int]] = frozenset()
    redirect_max: int = 3
    state: ConnectionState = field(default=ConnectionState.NEW, init=False)
    remove_64k_limit: bool = field(default=False, init=False)
    large_tuple_size: bool = field(default=False, init=False)
    long_row_id: bool = field(default=False, init=False)
    bigint_supported: bool = field(default=False, init=False)
    _socket: socket.socket | None = field(default=None, init=False, repr=False)
    _input: BinaryIO | None = field(default=None, init=False, repr=False)
    _output: BinaryIO | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    # The columns string.null.byte changed a value of in the last query's rows, which the
    # bridge doesn't page on (see PurePythonInformixBridge._unaltered); a command after the
    # query, such as the COMMIT WORK ending its read, leaves them.
    altered: set[str] = field(default_factory=set, init=False, repr=False)
    # Whether a statement ran since the last COMMIT or ROLLBACK WORK: in an ANSI database it
    # opened a transaction, which keeps what it read locked (see the bridge's operations).
    uncommitted: bool = field(default=False, init=False, repr=False)

    # pylint: disable-next=too-many-statements
    def connect(self) -> "InformixSqliClient":
        if not self.server_name:
            raise SqliUnsupportedAuthentication(
                "server_name is required; DB_LOCALE defaults to en_US.819 and CLIENT_LOCALE "
                "defaults to en_US.utf8; locale discovery, GSS and private-server "
                "authentication are unsupported"
            )
        deadline = time.monotonic() + self.login_timeout
        host, port, server = self.hostname, self.port, self.server_name
        visited: set[tuple[str, str, int]] = set()
        redirects = 0
        # pylint: disable-next=too-many-nested-blocks
        while True:
            self._reset_connection_state()
            redirected = redirects > 0
            addresses = self._resolved_addresses(host, port) if redirected else (host,)
            self._validate_redirect_destination(
                host, port, redirected=redirected, addresses=addresses
            )
            identity = (server, ",".join(addresses), port)
            if identity in visited:
                raise SqliUnsupportedAuthentication("Informix redirect loop detected")
            visited.add(identity)
            try:
                if redirected:
                    failures: list[OSError | SqliTransportError] = []
                    for address in addresses:
                        try:
                            self._connect_once(host, port, server, deadline, address)
                            break
                        except SqliRedirect:
                            raise
                        except SqliUnsupportedAuthentication:
                            raise
                        except (OSError, SqliTransportError) as error:
                            failures.append(error)
                            self._reset_connection_state()
                    else:
                        protocol_failure = next(
                            (error for error in failures if isinstance(error, SqliTransportError)),
                            None,
                        )
                        selected = protocol_failure or failures[-1]
                        for failure in failures:
                            if failure is not selected:
                                add_informix_exception_note(
                                    selected,
                                    f"Another validated redirect address failed: "
                                    f"{type(failure).__name__}: {failure}",
                                )
                        raise selected
                else:
                    self._connect_once(host, port, server, deadline, None)
                # Where this session ended up, apart from where the client was told to
                # connect: a reconnect starts from the configured endpoint again, so a
                # redirect's target is validated each time and a connection manager can
                # name a new one after a failover.
                self.connected_hostname, self.connected_port = host, port
                self.connected_server_name = server
                return self
            except SqliRedirect as redirect:
                self._reset_connection_state()
                if not self.redirect_enabled:
                    raise SqliUnsupportedAuthentication(
                        "Informix redirect is disabled"
                    ) from redirect
                redirects += 1
                if redirects > self.redirect_max:
                    raise SqliUnsupportedAuthentication(
                        "Informix redirect limit exceeded"
                    ) from redirect
                server, host, port = parse_redirect_detail(
                    redirect.detail, set(self.redirect_allowlist)
                )
            except Exception:
                self._poison()
                raise

    # pylint: disable-next=too-many-statements
    def _connect_once(
        self,
        host: str,
        port: int,
        server: str,
        deadline: float,
        validated_address: str | None = None,
    ) -> None:
        context = self.ssl_context
        if self.tls and context is None:
            context = self._ca_file_context(deadline)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SqliUnsupportedAuthentication("Informix login deadline exceeded")
        raw = socket.create_connection(
            (validated_address or host, port), min(self.connect_timeout, remaining)
        )
        # Tracks whatever socket must be closed on failure: the raw socket until a
        # TLS wrap replaces it, so a makefile failure after a successful handshake
        # closes the wrapped socket rather than leaking it.
        connection = raw
        try:
            raw.settimeout(min(self.socket_timeout, max(0.001, deadline - time.monotonic())))
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self.tls:
                connection = context.wrap_socket(raw, server_hostname=host)
            self._socket = connection
            self._input = _DeadlineReader(
                self._socket.makefile("rb", buffering=4096),
                self._socket,
                deadline,
                self.socket_timeout,
            )
            self._output = self._socket.makefile("wb", buffering=4096)
        except BaseException as primary_error:
            try:
                connection.close()
            except OSError as close_error:
                add_informix_exception_note(
                    primary_error,
                    f"Closing the failed Informix socket also failed: {close_error}",
                )
            raise
        self.state = ConnectionState.SOCKET_OPEN
        properties = {
            # The defaults the codecs take (informix_locale_encoding) for a locale left None.
            "CLIENT_LOCALE": self.client_locale or "en_US.utf8",
            "CLNT_PAM_CAPABLE": "1",
            "DBPATH": ".",
            "DB_LOCALE": self.db_locale or "en_US.819",
            # Honor double-quoted delimited identifiers so table/owner names with
            # characters that are not valid undelimited (e.g. "+") can be escaped.
            # Safe here because the connector's SQL uses single-quoted string
            # literals exclusively, so no literal is reinterpreted as an identifier.
            "DELIMIDENT": "1",
            "IFX_UPDDESC": "1",
            "NODEFDAC": "no",
        }
        if self.pad_varchar:
            # padVarchar reads rows in the fixed layout, where every VARCHAR has a slot
            # of its full width. The server sends that layout only when the session
            # asks for it; without this a padded read fails on the first row (seen
            # live: the row is shorter than the layout says).
            properties["IFX_PAD_VARCHAR"] = "1"
        request = encode_normal_auth_request(
            self.user,
            self.password,
            server,
            self.database,
            properties,
            self._encoding,
        )
        self._output.write(request)
        self._output.flush()
        self.state = ConnectionState.ASC_SENT
        header = read_exact(self._input, 6)
        total = struct.unpack_from(">H", header)[0]
        response = header + read_exact(self._input, total - 6)
        self.asc_accept = decode_asc_accept(response, self._encoding)
        self.state = ConnectionState.ACCEPTED
        self._output.write(encode_protocol_offer())
        self._output.flush()
        self.server_protocols = self._read_protocol_offer()
        # Bytes 7 and 8 (0x02; 0x04 and 0x02) in _protocol_feature's numbering, which treats
        # an offer too short to carry them as not offering them.
        self.remove_64k_limit = _protocol_feature(self.server_protocols, 62)
        self.large_tuple_size = _protocol_feature(self.server_protocols, 69)
        self.long_row_id = _protocol_feature(self.server_protocols, 70)
        self.bigint_supported = _protocol_feature(self.server_protocols, 54)
        pam_advertised = _protocol_feature(self.server_protocols, 44)
        if self.authentication_mode == "pam":
            if not pam_advertised:
                raise SqliUnsupportedAuthentication("PAM requested but server did not advertise it")
            self._authenticate_pam(deadline)
        self._output.write(encode_secondary_info(properties, self._encoding))
        self._output.flush()
        self._read_status_group()
        self.state = ConnectionState.AUTHENTICATED
        self._output.write(encode_dbopen(self.database, self._encoding))
        self._output.flush()
        self._read_status_group(require_done=True)
        if isinstance(self._input, _DeadlineReader):
            self._input.finish()
        self.state = ConnectionState.DATABASE_OPEN

    def _authenticate_pam(self, deadline: float) -> None:
        if self._input is None or self._output is None or self.authentication_provider is None:
            raise SqliUnsupportedAuthentication("PAM requires a noninteractive response provider")
        # Send SQ_ACK then EOT together.  Informix waits for the
        # EOT before it starts the PAM conversation.
        self._output.write(struct.pack(">hh", 128, SQ_EOT))  # SQ_ACK, SQ_EOT
        self._output.flush()
        for _ in range(self.pam_max_rounds):
            if time.monotonic() >= deadline:
                raise SqliUnsupportedAuthentication("PAM login deadline exceeded")
            code = read_smallint(self._input)
            if code == SQ_ASCEOT:  # SQ_ACCEPT aliases ASCEOT numerically
                if read_smallint(self._input) != SQ_EOT:
                    raise SqliProtocolError("PAM accept is missing EOT")
                return
            if code == SQ_EXIT:
                self._output.write(struct.pack(">hh", SQ_EXIT, SQ_EOT))
                self._output.flush()
                raise SqliUnsupportedAuthentication("Informix rejected PAM authentication")
            if code != 129:
                raise SqliProtocolError(f"unexpected PAM message {code}")
            style = read_smallint(self._input)
            length = read_smallint(self._input)
            if length < 0 or length > 512:
                raise SqliProtocolError("invalid PAM challenge length")
            raw = read_exact(self._input, length)
            if length & 1:
                read_exact(self._input, 1)
            if read_smallint(self._input) != SQ_EOT:
                raise SqliProtocolError("PAM challenge is missing EOT")
            if style not in {1, 2, 3, 4}:
                raise SqliProtocolError(f"unsupported PAM challenge style {style}")
            if style in {3, 4}:
                continue
            challenge = _decode_text(raw, self._encoding)
            response = self.authentication_provider.respond(style, challenge)
            if isinstance(response, bytes):
                response_raw = response
                if len(response_raw) > 512:
                    raise SqliUnsupportedAuthentication("PAM response exceeds 512-byte bound")
                packet = (
                    struct.pack(">hh", 130, len(response_raw))
                    + response_raw
                    + (b"\0" if len(response_raw) & 1 else b"")
                    + struct.pack(">h", SQ_EOT)
                )
            else:
                packet = encode_pam_response(response, self._encoding)
            self._output.write(packet)
            self._output.flush()
        raise SqliUnsupportedAuthentication("PAM round limit exceeded")

    def _resolved_addresses(self, host: str, port: int) -> tuple[str, ...]:
        try:
            values = {
                item[4][0] for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            }
        except OSError as exc:
            raise SqliUnsupportedAuthentication("redirect destination cannot be resolved") from exc
        if not values:
            raise SqliUnsupportedAuthentication("redirect destination has no addresses")
        return tuple(sorted(values))

    def _validate_redirect_destination(
        self,
        host: str,
        port: int,
        redirected: bool,
        addresses: tuple[str, ...] | None = None,
    ) -> None:
        if not redirected:
            return
        if (host, port) not in self.redirect_allowlist:
            raise SqliUnsupportedAuthentication("redirect target is not explicitly allow-listed")
        resolved = addresses if addresses is not None else self._resolved_addresses(host, port)
        for value in resolved:
            address = ipaddress.ip_address(value)
            if not address.is_global and (str(address), port) not in self.redirect_allowlist:
                raise SqliUnsupportedAuthentication("redirect resolved to a non-public address")

    def _reset_connection_state(self) -> None:
        self.close()
        self.remove_64k_limit = self.large_tuple_size = self.long_row_id = False
        self.bigint_supported = False
        for name in ("asc_accept", "server_protocols"):
            if hasattr(self, name):
                delattr(self, name)
        self.state = ConnectionState.NEW

    @property
    def _encoding(self) -> str:
        # The login's codec: user, password, server and database names go out in it.
        return informix_locale_encoding(self.client_locale)

    @property
    def _text_encoding(self) -> str:
        # The codec of every text after login, statements and values both ways: the
        # database's codeset (DB_LOCALE). The server converts no text between the client's
        # locale and the database's (probed on 14.10: a session declaring
        # CLIENT_LOCALE=en_US.utf8 stored and was sent a Latin-1 database's bytes as they
        # were); a CSDK or JDBC client converts on its own side. Decoded in
        # CLIENT_LOCALE's codec, a Latin-1 database's text failed under the default
        # en_US.utf8, and a UTF-8 database's came out garbled under en_US.819.
        return informix_locale_encoding(self.db_locale or "en_US.819")

    def _require_open(self) -> tuple[BinaryIO, BinaryIO]:
        if (
            self.state != ConnectionState.DATABASE_OPEN
            or self._input is None
            or self._output is None
        ):
            raise SqliProtocolError("SQLI database session is not open")
        return self._input, self._output

    def _ca_file_context(self, deadline: float) -> ssl.SSLContext:
        # Docstring: The TLS context, ssl.ca.file loaded. The file can sit on a Volume, read
        # through its FUSE mount, which refuses a read now and then: a failed read is tried
        # twice more, before the socket is opened, if the login deadline leaves the time.
        # Contents that don't load (ssl.SSLError), or a path that names no file, fail at once.

        attempt = 0
        while True:
            try:
                return ssl.create_default_context(cafile=self.ca_file)
            except ssl.SSLError as error:
                # Not as itself: an SSLError is a handshake cut short, which every reconnect
                # loop retried, for half a minute each, on a file that fails the same way.
                raise SqliInvalidCaFile(f"ssl.ca.file does not load: {error}") from error
            except OSError as error:
                if _ca_file_misnamed(error, self.ca_file):
                    raise
                attempt += 1
                delay = _CA_FILE_RETRY_SECONDS * attempt
                if attempt == 3 or time.monotonic() + delay >= deadline:
                    # A transport loss, so a reconnect tries again: a mount dropped for
                    # longer (ENOTCONN, seen on serverless) failed the stream reader's
                    # lookup at once. The OSError stays the cause.
                    raise SqliTransportError(f"ssl.ca.file could not be read: {error!r}") from error
                time.sleep(delay)

    def set_socket_timeout(self, timeout: float) -> None:
        # Docstring: Update the timeout used by the live SQLI socket.

        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("socket timeout must be finite and positive")
        self.socket_timeout = timeout
        if self._socket is not None:
            self._socket.settimeout(timeout)

    def _read_protocol_offer(self) -> bytes:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        code = read_smallint(self._input)
        if code != SQ_PROTOCOLS:
            raise SqliProtocolError(f"expected SQ_PROTOCOLS, received {code}")
        size = read_smallint(self._input)
        if size < 5 or size > 1024:
            raise SqliProtocolError(f"invalid enhanced protocol size {size}")
        value = read_exact(self._input, size)
        if size & 1:
            read_exact(self._input, 1)
        if read_smallint(self._input) != SQ_EOT:
            raise SqliProtocolError("enhanced protocol response is missing EOT")
        return value

    def _read_status_group(self, require_done: bool = False) -> None:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        saw_done = False
        saw_description = False
        statement_type = None
        while True:
            code = read_smallint(self._input)
            if code in {SQ_EOT, SQ_EXIT}:
                if require_done and not saw_done:
                    raise SqliProtocolError("status response reached EOT without SQ_DONE")
                return
            if code == 55:  # SQ_COST
                read_int32(self._input)
                read_int32(self._input)
                continue
            if code == 99:  # SQ_XACTSTAT
                read_exact(self._input, 6)
                continue
            if code == 8:  # SQ_DESCRIBE
                if saw_description:
                    raise SqliProtocolError("duplicate SQ_DESCRIBE in status response")
                description = self._read_description()
                if description.columns:
                    raise SqliProtocolError("non-row command returned a row-producing SQ_DESCRIBE")
                saw_description = True
                statement_type = description.statement_type
                continue
            if code == SQ_INSERTDONE:
                if statement_type != 6:
                    raise SqliProtocolError("SQ_INSERTDONE arrived outside an INSERT")
                self._read_insertdone()
                continue
            if code == SQ_DONE:
                self._read_done()
                saw_done = True
                continue
            if code == SQ_CLOSE:
                continue
            if code == SQ_ERR:
                error = self._read_error()
                if error[0] != 100:
                    raise _sql_error(error)
                continue
            raise SqliProtocolError(f"unknown status response message {code}")

    def _read_done(self) -> tuple[int, int, int, int]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        warnings = read_smallint(self._input)
        if self.long_row_id:
            rows, row_id = struct.unpack(">qq", read_exact(self._input, 16))
        else:
            rows, row_id = read_int32(self._input), read_int32(self._input)
        serial = read_int32(self._input)
        return warnings, rows, row_id, serial

    def _read_insertdone(self) -> None:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        # INT8 uses Informix's 10-byte signed-magnitude
        # representation; BIGSERIAL uses an 8-byte bigint.
        read_exact(self._input, 10)
        if self.bigint_supported:
            read_exact(self._input, 8)

    def _read_error(self) -> tuple[int, int, int, str]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        sqlcode, isamcode = read_smallint(self._input), read_smallint(self._input)
        statement_offset = (
            read_int32(self._input) if self.remove_64k_limit else read_smallint(self._input)
        )
        message = "" if sqlcode == -368 else decode_char(self._input, self._text_encoding)
        return sqlcode, isamcode, statement_offset, message

    # pylint: disable-next=too-many-statements
    def execute(
        self, sql: str, parameters: tuple = (), max_result_bytes: int | None = None
    ) -> list[tuple]:
        # Connector queries are fixed templates. Literalizing their bounded scalar
        # values avoids depending on the separate input-parameter descriptor that
        # the native protocol uses to choose VARCHAR bind widths.
        sql = _connector_sql(sql, parameters, self._text_encoding)
        if _statement_keyword(sql) in _NON_ROW_STATEMENTS:
            self.execute_command(sql)
            return []
        _, output_stream = self._require_open()
        with self._lock:
            try:
                output_stream.write(
                    encode_prepare(sql, self._text_encoding, 0, self.remove_64k_limit)
                )
                output_stream.flush()
                description, rows, _ = self._read_query_group(None)
                retained_bytes = _retained_size(rows) if max_result_bytes is not None else 0

                def append_batch(batch: list[dict[str, object]]) -> None:
                    nonlocal retained_bytes
                    if max_result_bytes is not None:
                        retained_bytes += _retained_size(batch)
                    if max_result_bytes is not None and retained_bytes > max_result_bytes:
                        raise SqliProtocolError(
                            f"SQLI result exceeded max_result_bytes={max_result_bytes}"
                        )
                    rows.extend(batch)

                if max_result_bytes is not None and retained_bytes > max_result_bytes:
                    raise SqliProtocolError(
                        f"SQLI result exceeded max_result_bytes={max_result_bytes}"
                    )
                if description is None:
                    return rows
                cursor_name = f"lc_{description.statement_id}"
                binds = b""
                variable = self._variable_tuples(description)
                if variable:
                    output_stream.write(
                        encode_cursor_open(
                            description.statement_id, cursor_name, self._text_encoding, binds
                        )
                    )
                    output_stream.flush()
                    self._read_query_group(description)
                    output_stream.write(
                        encode_variable_fetch(description, encoding=self._text_encoding)
                    )
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    append_batch(batch)
                    if exhausted:
                        return self._close_query(description, rows, output_stream)
                else:
                    output_stream.write(
                        encode_fixed_open_fetch(
                            description.statement_id,
                            cursor_name,
                            binds,
                            self._text_encoding,
                            4096,
                        )
                    )
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    append_batch(batch)
                    if exhausted:
                        return self._close_query(description, rows, output_stream)
                while True:
                    output_stream.write(encode_fetch(description.statement_id))
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    append_batch(batch)
                    if exhausted:
                        break
                output_stream.write(encode_close_release(description.statement_id))
                output_stream.flush()
                self._read_query_group(description)
                output_stream.write(encode_close_release(description.statement_id, release=True))
                output_stream.flush()
                self._read_query_group(description)
                return rows
            except Exception as error:
                self._poison()
                _raise_connection_loss(error)
                raise

    def execute_pages(
        self,
        sql: str,
        parameters: tuple,
        page_size: int,
        page_consumer: Callable[[list[dict[str, object]]], None],
    ) -> None:
        # Docstring: Stream one SELECT through one forward-only cursor in bounded pages.

        if page_size < 1:
            raise ValueError("page_size must be positive")
        sql = _connector_sql(sql, parameters, self._text_encoding)
        if _statement_keyword(sql) in _NON_ROW_STATEMENTS:
            raise ValueError("execute_pages requires a row-producing statement")
        _, output_stream = self._require_open()
        pending: list[dict[str, object]] = []

        def consume(batch: list[dict[str, object]]) -> None:
            pending.extend(batch)
            while len(pending) >= page_size:
                page = pending[:page_size]
                del pending[:page_size]
                page_consumer(page)

        with self._lock:
            try:
                output_stream.write(
                    encode_prepare(sql, self._text_encoding, 0, self.remove_64k_limit)
                )
                output_stream.flush()
                description, initial, _ = self._read_query_group(None)
                consume(initial)
                if description is None:
                    if pending:
                        page_consumer(pending)
                    return
                cursor_name = f"lc_{description.statement_id}"
                variable = self._variable_tuples(description)
                if variable:
                    output_stream.write(
                        encode_cursor_open(
                            description.statement_id, cursor_name, self._text_encoding, b""
                        )
                    )
                    output_stream.flush()
                    self._read_query_group(description)
                    output_stream.write(
                        encode_variable_fetch(description, encoding=self._text_encoding)
                    )
                else:
                    output_stream.write(
                        encode_fixed_open_fetch(
                            description.statement_id,
                            cursor_name,
                            b"",
                            self._text_encoding,
                            4096,
                        )
                    )
                output_stream.flush()
                _, batch, exhausted = self._read_query_group(description)
                consume(batch)
                while not exhausted:
                    output_stream.write(encode_fetch(description.statement_id))
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    consume(batch)
                output_stream.write(encode_close_release(description.statement_id))
                output_stream.flush()
                self._read_query_group(description)
                output_stream.write(encode_close_release(description.statement_id, release=True))
                output_stream.flush()
                self._read_query_group(description)
                if pending:
                    page_consumer(pending)
            except Exception as error:
                self._poison()
                _raise_connection_loss(error)
                raise

    def execute_command(self, sql: str) -> None:
        # Docstring: Execute a connector-owned non-row SQL statement through SQ_COMMAND.

        _, output_stream = self._require_open()
        with self._lock:
            try:
                output_stream.write(
                    encode_simple_command(sql, self._text_encoding, self.remove_64k_limit)
                )
                output_stream.flush()
                self._read_status_group(require_done=True)
                keyword = _statement_keyword(sql)
                if keyword in ("COMMIT", "ROLLBACK"):
                    self.uncommitted = False
                elif keyword not in ("SET", "BEGIN"):
                    self.uncommitted = True
            except Exception as error:
                self._poison()
                _raise_connection_loss(error)
                raise

    def _close_query(self, description, rows, output_stream):
        output_stream.write(encode_close_release(description.statement_id))
        output_stream.flush()
        self._read_query_group(description)
        output_stream.write(encode_close_release(description.statement_id, release=True))
        output_stream.flush()
        self._read_query_group(description)
        return rows

    def _read_query_group(
        self, description: ResultDescription | None
    ) -> tuple[ResultDescription | None, list[dict[str, object]], bool]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        if description is None:
            # A query's first result group: its rows' own (see altered). Not a command's,
            # which left none of the COMMIT WORK after a read to check.
            self.altered = set()
            self.uncommitted = True
        rows, exhausted = [], False
        # Fixed for the whole result, so decided once per batch rather than per tuple.
        variable_layout = None
        while True:
            code = read_smallint(self._input)
            if code in {SQ_EOT, SQ_EXIT}:
                return description, rows, exhausted
            if code == 8:
                if description is not None:
                    raise SqliProtocolError("duplicate SQ_DESCRIBE")
                description = self._read_description()
            elif code == 14:
                if description is None:
                    raise SqliProtocolError("SQ_TUPLE arrived before SQ_DESCRIBE")
                if variable_layout is None:
                    variable_layout = self._variable_tuples(description)
                rows.append(self._read_tuple(description, variable_layout))
            elif code == SQ_ERR:
                error = self._read_error()
                if error[0] == 100:
                    exhausted = True
                else:
                    raise _sql_error(error)
            elif code == 55:
                read_exact(self._input, 8)
            elif code == 99:
                read_exact(self._input, 6)
            elif code == SQ_INSERTDONE:
                if description is None or description.statement_type != 6:
                    raise SqliProtocolError("SQ_INSERTDONE arrived outside an INSERT")
                self._read_insertdone()
            elif code == SQ_DONE:
                _, affected_rows, _, _ = self._read_done()
                # A forward-only fetch that has no more tuples is reported as
                # SQ_DONE(rows=0) by IDS 15 (the accompanying zero row id
                # corresponds to the SQLSTATE-02000/end-of-data warning).
                if description is not None and (affected_rows == 0 or not rows):
                    exhausted = True
            elif code == SQ_CLOSE:
                # Informix may close a forward-only cursor in the same group as
                # the final tuple instead of emitting SQLCODE 100.  Track this
                # as a received close and do not issue another NFETCH.
                exhausted = True
            else:
                raise SqliProtocolError(f"unknown query response message {code}")

    def _read_description(self) -> ResultDescription:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        statement_type, statement_id = read_smallint(self._input), read_smallint(self._input)
        read_int32(self._input)  # optimizer row estimate
        tuple_size = (
            read_int32(self._input) if self.large_tuple_size else read_smallint(self._input)
        )
        count = read_smallint(self._input)
        name_size = read_int32(self._input)
        tuple_limit = MAX_PACKET if self.large_tuple_size else 32767
        if tuple_size < 0 or tuple_size > tuple_limit or count < 0 or count > 1024:
            raise SqliProtocolError("unsafe result description bounds")
        raw = []
        for _ in range(count):
            read_int32(self._input)
            position = read_int32(self._input)
            type_code = read_smallint(self._input)
            extended_id = read_int32(self._input)
            extended_owner = decode_char(self._input, self._text_encoding)
            extended_name = decode_char(self._input, self._text_encoding)
            read_smallint(self._input)
            read_smallint(self._input)
            read_int32(self._input)
            encoded_length = read_int32(self._input)
            if (
                position < 0
                or position > tuple_size
                or encoded_length < 0
                # Variable-width descriptors carry their declared maximum,
                # which may exceed the current tuple's encoded size.
                or encoded_length > tuple_limit
            ):
                raise SqliProtocolError("invalid result column position/length")
            raw.append(
                (
                    position,
                    type_code,
                    extended_id,
                    encoded_length,
                    extended_owner,
                    extended_name,
                )
            )
        if name_size < 0 or name_size > 1 << 20:
            raise SqliProtocolError(f"unsafe descriptor name blob length {name_size}")
        names_raw = read_exact(self._input, name_size)
        if name_size & 1:
            read_exact(self._input, 1)
        names = (
            []
            if count == 0 and not names_raw
            else [
                _decode_text(item, self._text_encoding)
                for item in names_raw.rstrip(b"\0").split(b"\0")
            ]
        )
        if len(names) != count or len(set(names)) != count:
            raise SqliProtocolError("descriptor names do not match column count")
        columns = tuple(ResultColumn(name, *descriptor) for name, descriptor in zip(names, raw))
        if any(b.position < a.position for a, b in zip(columns, columns[1:])):
            raise SqliProtocolError("result column positions are not monotonic")
        return ResultDescription(statement_type, statement_id, tuple_size, columns)

    def _read_tuple(
        self, description: ResultDescription, variable_layout: bool | None = None
    ) -> dict[str, object]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        read_smallint(self._input)  # tuple warning
        size = read_int32(self._input)
        tuple_limit = MAX_PACKET if self.large_tuple_size else 32767
        if size < 0 or size > tuple_limit:
            raise SqliProtocolError(f"unsafe tuple payload length {size}")
        payload = read_exact(self._input, size)
        if size & 1:
            read_exact(self._input, 1)
        result = {}
        if variable_layout is None:
            variable_layout = self._variable_tuples(description)
        # Every VARCHAR is read by its length byte, padded or not (see
        # _decode_result_value_raw); ``padded`` only tells the decoder which layout the
        # row came in.
        padded = self.pad_varchar and not variable_layout
        cursor = 0
        for index, column in enumerate(description.columns):
            kind = _effective_result_kind(column)
            start = cursor if variable_layout else column.position
            if variable_layout and kind in {13, 16}:
                if start >= len(payload):
                    raise SqliProtocolError("truncated variable VARCHAR tuple")
                # Under padVarchar the session asks for padded VARCHARs, and a variable
                # tuple carries them padded too: the length byte, then the column's full
                # width (seen live: a VARCHAR(10) holding '' took 11 bytes beside an
                # LVARCHAR). Without it the slot ends with the value.
                width = (column.encoded_length & 0xFF) if self.pad_varchar else payload[start]
                limit = start + 1 + width
            elif variable_layout and kind in {40, 41, 43, 45, 46}:
                if start + 5 > len(payload):
                    raise SqliProtocolError("truncated complex variable tuple")
                envelope = struct.unpack_from(">i", payload, start + 1)[0]
                if envelope < 0:
                    # -4 to -1 left the slice inside its own 5-byte header, which passed
                    # the bounds check, decoded as a value, and misaligned the row.
                    raise SqliProtocolError(
                        f"negative variable tuple length {envelope} for column {column.name!r}"
                    )
                limit = start + 5 + envelope
            elif variable_layout:
                limit = start + _fixed_result_size(column)
            else:
                limit = (
                    description.columns[index + 1].position
                    if index + 1 < len(description.columns)
                    else len(payload)
                )
            if not 0 <= start <= limit <= len(payload):
                raise SqliProtocolError(
                    f"tuple column {column.name!r} slice [{start}:{limit}] "
                    f"exceeds payload size {len(payload)}"
                )
            if not variable_layout:
                # A fixed-layout row shorter than its description left the last column a
                # short slice: a CHAR decoded what was there and a number failed as a
                # struct.error, which no SqliProtocolError retry catches. A VARCHAR has
                # no fixed width to hold it to.
                try:
                    width = _fixed_result_size(column)
                except SqliDescriptorNotImplemented:
                    width = 0
                if limit - start < width:
                    raise SqliProtocolError(
                        f"tuple column {column.name!r} slice [{start}:{limit}] is shorter "
                        f"than its {width}-byte width"
                    )
            value = _decode_result_value(payload[start:limit], column, self._text_encoding, padded)
            result[column.name] = apply_null_byte(value, self.null_byte)
            if result[column.name] is not value:
                # A key or index value it changed is one the bridge can't page on.
                self.altered.add(column.name)
            cursor = limit
        return result

    def _variable_tuples(self, description: ResultDescription) -> bool:
        # Docstring: Whether ``description``'s rows travel in the variable tuple layout.
        #
        # One answer for both the fetch request and the tuple decode. An LVARCHAR or
        # opaque column (types 40, 41, 43, 46; BOOLEAN is one, also typed 45) always makes
        # the layout variable, padVarchar or not. Otherwise VARCHARs make it variable
        # unless padVarchar asks for the fixed one. Under padVarchar a VARCHAR is padded
        # to its full width in either layout (see _read_tuple). The kinds match the ones
        # _read_tuple reads as variable envelopes.

        return any(
            (column.type_code & 0xFF) in {40, 41, 43, 45, 46}
            or (not self.pad_varchar and (column.type_code & 0xFF) in {13, 16})
            for column in description.columns
        )

    def read_lodata(self, descriptor: int, requested: int) -> bytes:
        input_stream, output_stream = self._require_open()
        with self._lock:
            try:
                output_stream.write(encode_lodata_read(descriptor, requested))
                output_stream.write(struct.pack(">h", SQ_EOT))
                output_stream.flush()
                chunks = bytearray()
                while True:
                    code = read_smallint(input_stream)
                    if code == SQ_EOT:
                        return bytes(chunks)
                    if code == SQ_LODATA:
                        operation, size = read_smallint(input_stream), read_int32(input_stream)
                        if operation not in {LO_READ, LO_READ_WITH_SEEK}:
                            raise SqliProtocolError(
                                f"unexpected SQ_LODATA response operation {operation}"
                            )
                        if size < 0:
                            raise SqliProtocolError(f"SQ_LODATA server/ISAM error {size}")
                        if size < 1:
                            read_smallint(input_stream)
                            continue
                        if len(chunks) + size > requested:
                            raise SqliProtocolError("SQ_LODATA response exceeds requested bound")
                        remaining = size
                        while remaining:
                            chunk_size = read_smallint(input_stream)
                            if chunk_size <= 0 or chunk_size > remaining:
                                raise SqliProtocolError(
                                    f"invalid SQ_LODATA chunk size {chunk_size}"
                                )
                            chunks.extend(read_exact(input_stream, chunk_size))
                            if chunk_size & 1:
                                read_exact(input_stream, 1)
                            remaining -= chunk_size
                        continue
                    if code == SQ_ERR:
                        raise _sql_error(self._read_error(), " during LODATA")
                    raise SqliProtocolError(f"unexpected SQLI message {code} during LODATA")
            except Exception as error:
                self._poison()
                _raise_connection_loss(error)
                raise

    def _poison(self) -> None:
        self.close()
        self.state = ConnectionState.POISONED

    def close(self, abrupt: bool = False) -> None:
        # ``abrupt`` closes under an operation another thread still has in flight (a
        # lost connection-slot lease). Nothing is written then: the goodbye would land
        # in the middle of that operation's statement. And the socket is shut down
        # before anything is closed: the reader holds the buffered stream's lock while
        # it waits on the socket, so closing the stream first would wait for that read
        # to time out by itself, where the shutdown ends it at once.
        if abrupt and self._socket is not None:
            with contextlib.suppress(OSError):
                self._socket.shutdown(socket.SHUT_RDWR)
        elif self.state == ConnectionState.DATABASE_OPEN and self._output is not None:
            try:
                self._output.write(struct.pack(">hh", SQ_EXIT, SQ_EOT))
                self._output.flush()
            except (OSError, ValueError):
                # Closing the underlying streams remains the fallback for a
                # broken transport; graceful exit is best effort.
                pass
        for stream in (self._input, self._output):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
        self._input = self._output = None
        self._socket = None
        # The session took its transaction with it: nothing is left for a COMMIT WORK, which
        # on a closed client failed the operation that had reset it and returned.
        self.uncommitted = False
        if self.state != ConnectionState.POISONED:
            self.state = ConnectionState.CLOSED


__all__ = [
    "AscAccept",
    "datetime_order_preserving_cast",
    "AuthenticationProvider",
    "ConnectionState",
    "InformixDatetimeLiteral",
    "InformixSqliClient",
    "PasswordAuthenticationProvider",
    "SqliDescriptorNotImplemented",
    "SqliInvalidCaFile",
    "SqliSessionRejected",
    "SqliProtocolError",
    "SqliTransportError",
    "SqliRedirect",
    "SqliUnsupportedAuthentication",
    "add_informix_exception_note",
    "decode_char",
    "decode_asc_accept",
    "encode_asc_environment",
    "encode_asc_char",
    "encode_asc_tail",
    "encode_char",
    "encode_close_release",
    "encode_cursor_open",
    "encode_dbopen",
    "encode_fetch",
    "encode_variable_fetch",
    "encode_fixed_open_fetch",
    "encode_lodata_read",
    "encode_normal_auth_prefix",
    "encode_normal_auth_request",
    "encode_protocol_offer",
    "encode_pam_response",
    "encode_prepare",
    "encode_secondary_info",
    "encode_session_packet",
    "encode_simple_command",
    "informix_locale_encoding",
    "parse_redirect_detail",
    "read_session_packet",
]
