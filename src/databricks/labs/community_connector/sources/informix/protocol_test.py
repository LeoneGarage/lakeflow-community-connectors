"""Source-local golden tests for bytecode-verified protocol layouts."""

import io
import struct
import unittest
from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    CdcFrameParser,
    CdcProtocolError,
    ColumnDescriptor,
    OpenTransactionRecords,
    cdc_routine,
    decode_frame,
    decode_value,
    metadata_column_names,
    validate_snapshot_arity,
)
from databricks.labs.community_connector.sources.informix.sqli import (
    ConnectionState,
    InformixSqliClient,
    PasswordAuthenticationProvider,
    ResultColumn,
    ResultDescription,
    SqliDescriptorNotImplemented,
    SqliProtocolError,
    SqliUnsupportedAuthentication,
    TypedBind,
    _connector_sql,
    _DeadlineReader,
    _decode_result_value,
    _statement_keyword,
    decode_asc_accept,
    decode_asc_response,
    decode_lodata_response,
    decode_pam_challenge,
    encode_asc_char,
    encode_asc_environment,
    encode_bind,
    encode_char,
    encode_close_release,
    encode_cursor_open,
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
    materialize_blob_chunks,
    parse_redirect_detail,
    read_session_packet,
)


def _frame(kind, header=b"", payload=b""):
    header_size = 16 + len(header)
    return struct.pack(">IIII", header_size, len(payload), 0, kind) + header + payload


class FrameTests(unittest.TestCase):
    def test_cdc_routines_are_cross_database_qualified(self):
        self.assertEqual(
            cdc_routine("cdc_opensess"), "syscdcv1:informix.cdc_opensess"
        )
        with self.assertRaises(CdcProtocolError):
            cdc_routine("cdc_opensess); DROP DATABASE testdb")

    def test_partial_and_multiple_frames(self):
        first = _frame(201, struct.pack(">q", 17))
        second = _frame(2, struct.pack(">qiq", 22, 4, 123))
        parser = CdcFrameParser(1024)
        self.assertEqual(parser.feed(first[:7]), [])
        frames = parser.feed(first[7:] + second)
        self.assertEqual(frames, [first, second])
        self.assertEqual(decode_frame(first, {})["lsn"], 17)

    def test_lsn_uses_full_unsigned_64_bit_wire_domain(self):
        lsn = (1 << 63) + 17
        record = decode_frame(_frame(201, struct.pack(">Q", lsn)), {})
        self.assertEqual(record["lsn"], lsn)

    def test_metadata_tolerates_ibm_header_extension(self):
        record = decode_frame(_frame(200, struct.pack(">i", 7) + bytes(16), b"integer id"), {})
        self.assertEqual(record["label"], 7)
        self.assertEqual(record["metadata"], b"integer id")

    def test_metadata_column_order_ignores_type_argument_commas(self):
        self.assertEqual(
            metadata_column_names(b"id serial, amount decimal(12,2), name varchar(60,0)\0"),
            ("id", "amount", "name"),
        )

    def test_metadata_uses_negotiated_client_encoding(self):
        self.assertEqual(
            metadata_column_names("café varchar(20)".encode("iso8859-1"), "iso8859-1"),
            ("café",),
        )

    def test_operation_primitive_row(self):
        header = struct.pack(">qii", 99, 7, 3)
        payload = struct.pack(">hi", 12, 34) + b"\x03abc" + b"\x00\x01"
        columns = {
            3: (
                ColumnDescriptor("s", "SMALLINT"),
                ColumnDescriptor("i", "INTEGER"),
                ColumnDescriptor("v", "VARCHAR"),
                ColumnDescriptor("b", "BOOLEAN"),
            )
        }
        record = decode_frame(_frame(40, header, payload), columns)
        self.assertEqual(record["row"], {"s": 12, "i": 34, "v": "abc", "b": True})

    def test_bounds_reject_oversize(self):
        parser = CdcFrameParser(32)
        with self.assertRaises(CdcProtocolError):
            parser.feed(struct.pack(">IIII", 16, 17, 0, 201))

    def test_truncate_preserves_wire_user_id_and_capture_interpretation(self):
        record = decode_frame(_frame(119, struct.pack(">qii", 9, 4, 7)), {})
        self.assertEqual(record["user_id"], 7)
        self.assertEqual(record["capture_label"], 7)
        self.assertNotIn("label", record)

    def test_null_sentinels_and_lvarchar_flags(self):
        self.assertEqual(
            decode_value(memoryview(struct.pack(">h", -32768)), ColumnDescriptor("s", "SMALLINT")),
            (None, 2),
        )
        self.assertEqual(
            decode_value(memoryview(b"\x00\x01\x01"), ColumnDescriptor("v", "LVARCHAR")),
            (None, 3),
        )
        self.assertEqual(
            decode_value(memoryview(b"\x01\x00"), ColumnDescriptor("v", "VARCHAR")),
            (None, 2),
        )
        self.assertEqual(
            decode_value(memoryview(b"\x01\x00"), ColumnDescriptor("b", "BOOLEAN")),
            (None, 2),
        )
        with self.assertRaisesRegex(CdcProtocolError, "boolean null flag"):
            decode_value(memoryview(b"\x02\x00"), ColumnDescriptor("b", "BOOLEAN"))
        self.assertEqual(
            decode_value(memoryview(b"\xff" * 8), ColumnDescriptor("f", "FLOAT")),
            (None, 8),
        )

    def test_informix_date_epoch_is_1899_12_31(self):
        self.assertEqual(
            decode_value(memoryview(struct.pack(">i", 0)), ColumnDescriptor("d", "DATE")),
            (datetime(1899, 12, 31).date(), 4),
        )

    def test_packed_decimal_positive_negative_and_null(self):
        column = ColumnDescriptor("d", "DECIMAL", precision=4, scale=2)
        self.assertEqual(decode_value(memoryview(b"\xc0\x0c\x22"), column), (Decimal("12.34"), 3))
        self.assertEqual(decode_value(memoryview(b"\x3f\x57\x42"), column), (Decimal("-12.34"), 3))
        self.assertEqual(decode_value(memoryview(b"\0\0\0"), column), (None, 3))

    def test_open_record_bound_excludes_committed_and_handles_interleaving(self):
        tracker = OpenTransactionRecords()
        tracker.begin(1)
        tracker.append(1, 10)
        tracker.begin(2)
        tracker.append(2, 11)
        tracker.append(2, 12)
        self.assertEqual(tracker.buffered, 3)
        tracker.finish(1)
        self.assertEqual(tracker.buffered, 2)
        tracker.discard(2, 12)
        self.assertEqual(tracker.buffered, 1)
        tracker.finish(2)
        self.assertEqual(tracker.buffered, 0)
        self.assertFalse(tracker)

    def test_open_record_bound_rejects_duplicate_begin(self):
        tracker = OpenTransactionRecords()
        tracker.begin(1)
        with self.assertRaisesRegex(CdcProtocolError, "duplicate CDC BEGIN"):
            tracker.begin(1)

    def test_snapshot_continuation_arity(self):
        validate_snapshot_arity([1, 2], ["a", "b"])
        with self.assertRaises(CdcProtocolError):
            validate_snapshot_arity([1], ["a", "b"])

    def test_int8_signed_magnitude_and_datetime_year_to_fraction5(self):
        int8 = ColumnDescriptor("i", "INT8")
        self.assertEqual(decode_value(memoryview(struct.pack(">hII", -1, 7, 0)), int8), (-7, 10))
        self.assertEqual(decode_value(memoryview(b"\0" * 10), int8), (None, 10))
        with self.assertRaises(CdcProtocolError):
            decode_value(memoryview(struct.pack(">hII", 2, 1, 0)), int8)
        snapshot_int8 = ResultColumn("i", 0, 17, 0, 10)
        snapshot_serial8 = ResultColumn("s", 0, 18, 0, 10)
        self.assertEqual(
            _decode_result_value(struct.pack(">hII", -1, 7, 0), snapshot_int8, "utf-8"),
            -7,
        )
        self.assertIsNone(_decode_result_value(b"\0" * 10, snapshot_serial8, "utf-8"))
        temporal = ColumnDescriptor("t", "DATETIME", length=0x000F)
        raw = bytes([0xC0, 20, 24, 1, 2, 3, 4, 5, 12, 34, 50])
        self.assertEqual(
            decode_value(memoryview(raw), temporal),
            (datetime(2024, 1, 2, 3, 4, 5, 123450), 11),
        )
        time_only = ColumnDescriptor("t", "DATETIME", length=0x060A)
        self.assertEqual(
            decode_value(memoryview(bytes([0xC0, 12, 34, 56])), time_only),
            ("HOUR=12:MINUTE=34:SECOND=56", 4),
        )

    def test_datetime_partial_qualifiers(self):
        cases = (
            (0x060A, bytes([0xC0, 12, 34, 56]), "HOUR=12:MINUTE=34:SECOND=56"),
            (0x0408, bytes([0xC0, 12, 3, 4]), "DAY=12:HOUR=03:MINUTE=04"),
            (0x0002, bytes([0xC0, 20, 24, 1]), "YEAR=2024:MONTH=01"),
            (0x080F, bytes([0xC0, 34, 56, 12, 34, 50]),
             "MINUTE=34:SECOND=56:FRACTION(5)=12345"),
        )
        for qualifier, raw, expected in cases:
            with self.subTest(qualifier=qualifier):
                self.assertEqual(
                    decode_value(
                        memoryview(raw), ColumnDescriptor("t", "DATETIME", length=qualifier)
                    ),
                    (expected, len(raw)),
                )

    def test_ordinary_datetime_uses_extended_id_qualifier(self):
        # SQL result encoded_length is packed storage metadata, not the
        # start/end qualifier. The latter must survive from extended_id.
        column = ResultColumn("t", 0, 10, 0x060A, 0x0603)
        self.assertEqual(
            _decode_result_value(bytes([0xC0, 12, 34, 56]), column, "utf-8"),
            "HOUR=12:MINUTE=34:SECOND=56",
        )

    def test_live_ordinary_datetime_uses_encoded_length_qualifier_fallback(self):
        column = ResultColumn("updated_at", 0, 266, 0, 0x130F)
        raw = bytes([0xC0, 20, 26, 7, 19, 12, 34, 56, 12, 34, 50])
        self.assertEqual(
            _decode_result_value(raw, column, "utf-8"),
            datetime(2026, 7, 19, 12, 34, 56, 123450),
        )

    def test_native_boolean_and_bigint_result_ids(self):
        boolean = ResultColumn("enabled", 0, 45, 0, 1)
        bigint = ResultColumn("count", 0, 52, 0, 8)
        bigserial = ResultColumn("serial", 0, 53, 0, 8)
        self.assertTrue(_decode_result_value(b"\x01", boolean, "utf-8"))
        self.assertEqual(
            _decode_result_value(struct.pack(">q", 1 << 40), bigint, "utf-8"), 1 << 40
        )
        self.assertEqual(
            _decode_result_value(struct.pack(">q", 123), bigserial, "utf-8"), 123
        )

    def test_ordinary_boolean_null_and_marker_validation(self):
        one_byte = ResultColumn("enabled", 0, 45, 0, 1)
        two_byte = ResultColumn("enabled", 0, 45, 0, 2)
        self.assertIsNone(_decode_result_value(b"\xff", one_byte, "utf-8"))
        self.assertIsNone(_decode_result_value(b"\x00\xff", two_byte, "utf-8"))
        self.assertIs(_decode_result_value(b"t", one_byte, "utf-8", True), True)
        self.assertIs(_decode_result_value(b"f", one_byte, "utf-8", True), False)
        with self.assertRaisesRegex(SqliProtocolError, "invalid BOOLEAN"):
            _decode_result_value(b"\x02", one_byte, "utf-8")
        enveloped = ResultColumn("enabled", 0, 45, 0, 1)
        self.assertTrue(
            _decode_result_value(
                b"\x00" + struct.pack(">i", 1) + b"\x01", enveloped, "utf-8"
            )
        )
        self.assertIsNone(
            _decode_result_value(
                b"\x01" + struct.pack(">i", 0), enveloped, "utf-8"
            )
        )
        with self.assertRaises(SqliDescriptorNotImplemented):
            _decode_result_value(
                b"\x00" + struct.pack(">i", 1) + b"\x01",
                ResultColumn("collection", 0, 23, 0, 1),
                "utf-8",
            )

    def test_ordinary_char_strips_only_space_padding(self):
        column = ResultColumn("value", 0, 0, 0, 4)
        self.assertEqual(_decode_result_value(b"a\t  ", column, "utf-8"), "a\t")

    def test_snapshot_and_cdc_char_use_the_same_padding_normalization(self):
        column = ColumnDescriptor("value", "CHAR", length=4)
        self.assertEqual(decode_value(memoryview(b"a   "), column), ("a", 4))

    def test_ordinary_varchar_null_sentinel(self):
        for type_code in (13, 16):
            with self.subTest(type_code=type_code):
                column = ResultColumn("value", 0, type_code, 0, 20)
                self.assertIsNone(_decode_result_value(b"\x01\x00", column, "utf-8"))

    def test_ordinary_lvarchar_value_null_and_validation(self):
        column = ResultColumn("value", 0, 43, 0, 100)
        self.assertEqual(
            _decode_result_value(b"\x00" + struct.pack(">i", 5) + b"hello", column, "utf-8"),
            "hello",
        )
        self.assertIsNone(
            _decode_result_value(b"\x01" + struct.pack(">i", 0), column, "utf-8")
        )
        with self.assertRaisesRegex(SqliProtocolError, "length"):
            _decode_result_value(
                b"\x00" + struct.pack(">i", 4) + b"short", column, "utf-8"
            )


class LoDataTests(unittest.TestCase):
    def test_verified_read_body(self):
        self.assertEqual(
            encode_lodata_read(9, 32000), struct.pack(">hhhih", 97, 0, 9, 32000, 32000)
        )

    def test_verified_response_body(self):
        payload = struct.pack(">hih", 0, 3, 3) + b"abc\0"
        self.assertEqual(decode_lodata_response(payload), b"abc")
        self.assertEqual(decode_lodata_response(struct.pack(">hih", 0, 0, 0)), b"")

    def test_response_rejects_direction_and_all_negative_sizes(self):
        for operation in (2, 3, -1):
            with self.assertRaises(SqliProtocolError):
                decode_lodata_response(struct.pack(">hih", operation, 0, 0))
        for operation in (0, 1, 2, 3):
            with self.assertRaises(SqliProtocolError):
                decode_lodata_response(struct.pack(">hi", operation, -1))


class SqliPacketTests(unittest.TestCase):
    def test_unbounded_execute_skips_decoded_byte_accounting(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        output = io.BytesIO()

        with patch.object(
            client, "_require_open", return_value=(io.BytesIO(), output)
        ), patch.object(
            client, "_read_query_group", return_value=(None, [], False)
        ), patch(
            "databricks.labs.community_connector.sources.informix.sqli._retained_size",
            side_effect=AssertionError("must not account"),
        ):
            self.assertEqual(client.execute("SELECT 1 FROM systables"), [])

    def test_execute_routes_non_row_statements_to_command_path(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        with patch.object(client, "execute_command") as execute_command:
            self.assertEqual(
                client.execute("UPDATE app.orders SET state = ?", ("done",)), []
            )

        execute_command.assert_called_once_with(
            "UPDATE app.orders SET state = 'done'"
        )

    def test_execute_routes_commented_non_row_statements_to_command_path(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        with patch.object(client, "execute_command") as execute_command:
            client.execute("/* mutation */\n-- owned SQL\nUPDATE t SET value = ?", (1,))
        execute_command.assert_called_once_with(
            "/* mutation */\n-- owned SQL\nUPDATE t SET value = 1"
        )

    def test_connector_sql_ignores_question_marks_in_literals_and_comments(self):
        sql = "SELECT '?' AS literal /* ? */ FROM t -- ?\nWHERE id = ?"
        self.assertEqual(
            _connector_sql(sql, (7,)),
            "SELECT '?' AS literal /* ? */ FROM t -- ?\nWHERE id = 7",
        )
        self.assertEqual(_statement_keyword("/* note */ -- note\n UPDATE t SET x=1"), "UPDATE")

    def test_non_row_command_uses_sq_command_execute_release_path(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        output = io.BytesIO()
        transcript = (
            struct.pack(">hhhh", 99, 1, 2, 1)
            + struct.pack(">hhiii", 15, 0, 0, 0, 0)
            + struct.pack(">h", 12)
        )
        client._input = io.BytesIO(transcript)

        with patch.object(client, "_require_open", return_value=(client._input, output)):
            client.execute_command("BEGIN WORK")

        self.assertEqual(
            output.getvalue(),
            encode_simple_command("BEGIN WORK", "utf-8", long_sql_length=False),
        )
        self.assertEqual(client._input.tell(), len(transcript))

    def test_non_row_command_consumes_zero_column_description(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        output = io.BytesIO()
        transcript = (
            struct.pack(">hhhihhi", 8, 0, 7, 0, 0, 0, 0)
            + struct.pack(">hhiii", 15, 0, 0, 0, 0)
            + struct.pack(">h", 12)
        )
        client._input = io.BytesIO(transcript)

        with patch.object(client, "_require_open", return_value=(client._input, output)):
            client.execute_command("SET ISOLATION TO REPEATABLE READ")

        self.assertEqual(client._input.tell(), len(transcript))

    def test_execute_enforces_incremental_decoded_result_byte_bound(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        description = ResultDescription(
            2, 7, 4, (ResultColumn("value", 0, 2, 0, 4),)
        )
        responses = (
            (description, [], False),
            (description, [{"value": 1}], True),
        )
        output = io.BytesIO()

        with patch.object(
            client, "_require_open", return_value=(io.BytesIO(), output)
        ), patch.object(client, "_read_query_group", side_effect=responses), patch.object(
            client, "_poison"
        ), self.assertRaisesRegex(
            SqliProtocolError, "max_result_bytes=1"
        ):
            client.execute("SELECT value FROM t", max_result_bytes=1)

    def test_login_deadline_reader_recomputes_remaining_time_per_read(self):
        connection = Mock()
        reader = _DeadlineReader(io.BytesIO(b"ab"), connection, deadline=10.0, maximum=30.0)

        with patch(
            "databricks.labs.community_connector.sources.informix.sqli.time.monotonic",
            side_effect=(2.0, 7.0),
        ):
            self.assertEqual(reader.read(1), b"a")
            self.assertEqual(reader.read(1), b"b")

        self.assertEqual(connection.settimeout.call_args_list[0].args, (8.0,))
        self.assertEqual(connection.settimeout.call_args_list[1].args, (3.0,))
        reader.finish()
        self.assertEqual(connection.settimeout.call_args.args, (30.0,))

    def test_login_deadline_reader_fails_after_absolute_deadline(self):
        reader = _DeadlineReader(io.BytesIO(b"a"), Mock(), deadline=10.0, maximum=30.0)
        with patch(
            "databricks.labs.community_connector.sources.informix.sqli.time.monotonic",
            return_value=10.0,
        ), self.assertRaisesRegex(SqliUnsupportedAuthentication, "deadline exceeded"):
            reader.read(1)

    def test_informix_locale_codec_aliases_are_strict(self):
        client = InformixSqliClient(
            "host", 9088, "db", "user", "password", client_locale="en_US.819"
        )
        self.assertEqual(client._encoding, "iso8859-1")
        client.client_locale = "en_US.57372"
        self.assertEqual(client._encoding, "utf-8")
        client.client_locale = "en_US.unknown"
        with self.assertRaises(SqliProtocolError):
            _ = client._encoding

    def test_bounded_internal_bind_envelope(self):
        encoded = encode_bind(
            (TypedBind(2, 7), TypedBind(52, -9), TypedBind(13, "abc"), TypedBind(0, None))
        )
        self.assertTrue(encoded.startswith(struct.pack(">hh", 5, 4)))
        self.assertIn(struct.pack(">i", 7), encoded)
        self.assertIn(struct.pack(">q", -9), encoded)
        self.assertTrue(encoded.endswith(struct.pack(">hhh", 0, -1, 0)))

    def test_pam_static_frames_and_bounds(self):
        challenge = struct.pack(">hh", 1, 4) + b"PIN?"
        self.assertEqual(decode_pam_challenge(challenge), (1, "PIN?"))
        self.assertEqual(encode_pam_response("1234"), struct.pack(">hh", 130, 4) + b"1234\0\x0c")
        with self.assertRaises(SqliUnsupportedAuthentication):
            encode_pam_response("x" * 513)

    def test_pam_multiround_information_and_accept(self):
        wire = (
            struct.pack(">hhh", 129, 4, 4) + b"info" + struct.pack(">h", 12)
            + struct.pack(">hhh", 129, 1, 9) + b"Password:\0" + struct.pack(">h", 12)
            + struct.pack(">hh", 127, 12)
        )
        client = InformixSqliClient(
            "host", 9088, "db", "user", "secret",
            client_locale="en_US.utf8",
            authentication_mode="pam",
            authentication_provider=PasswordAuthenticationProvider("secret"),
        )
        client._input, client._output = io.BytesIO(wire), io.BytesIO()
        client._authenticate_pam(float("inf"))
        self.assertEqual(
            client._output.getvalue(),
            struct.pack(">hh", 128, 12) + encode_pam_response("secret"),
        )

    def test_pam_fail_closed_states(self):
        cases = (
            (struct.pack(">hhh", 129, 1, 513), SqliProtocolError),
            (struct.pack(">hhh", 129, 1, 1) + b"x\0" + struct.pack(">h", 99), SqliProtocolError),
            (struct.pack(">hh", 56, 12), SqliUnsupportedAuthentication),
        )
        for wire, error in cases:
            client = InformixSqliClient(
                "host", 9088, "db", "user", "secret",
                client_locale="en_US.utf8", authentication_mode="pam",
                authentication_provider=PasswordAuthenticationProvider("secret"),
            )
            client._input, client._output = io.BytesIO(wire), io.BytesIO()
            with self.assertRaises(error):
                client._authenticate_pam(float("inf"))

    def test_pam_encoded_response_bound_and_round_limit(self):
        wire = struct.pack(">hhh", 129, 1, 1) + b"x\0" + struct.pack(">h", 12)
        client = InformixSqliClient(
            "host", 9088, "db", "user", "x" * 513,
            client_locale="en_US.utf8", authentication_mode="pam",
            authentication_provider=PasswordAuthenticationProvider("x" * 513), pam_max_rounds=1,
        )
        client._input, client._output = io.BytesIO(wire), io.BytesIO()
        with self.assertRaises(SqliUnsupportedAuthentication):
            client._authenticate_pam(float("inf"))

    def test_redirect_allowlist_and_blob_chunk_bounds(self):
        self.assertEqual(
            parse_redirect_detail("redirect:x:srv:db.example:9089", {("db.example", 9089)}),
            ("srv", "db.example", 9089),
        )
        with self.assertRaises(SqliUnsupportedAuthentication):
            parse_redirect_detail("redirect:x:srv:evil:9089", {("db.example", 9089)})
        for unsafe in ("label=srv|db.example|service", "label=srv|db.example|9089|extra"):
            with self.assertRaises(SqliUnsupportedAuthentication):
                parse_redirect_detail(unsafe, {("db.example", 9089)})
        chunks = [struct.pack(">h", 3) + b"abc\0", struct.pack(">h", 2) + b"de"]
        self.assertEqual(materialize_blob_chunks(chunks, 8, 8), b"abcde")
        with self.assertRaises(SqliProtocolError):
            materialize_blob_chunks(chunks, 4, 8)

    def test_redirect_resolution_is_stable_and_private_requires_ip_allowlist(self):
        client = InformixSqliClient(
            "origin", 9088, "db", "user", "secret",
            redirect_enabled=True,
            redirect_allowlist=frozenset({("target", 9088)}),
        )
        with patch("socket.getaddrinfo", return_value=[
            (2, 1, 6, "", ("203.0.113.1", 9088)),
            (2, 1, 6, "", ("203.0.113.2", 9088)),
        ]):
            with self.assertRaises(SqliUnsupportedAuthentication):
                client._validate_redirect_destination("target", 9088, True)
        with patch("socket.getaddrinfo", return_value=[
            (2, 1, 6, "", ("127.0.0.1", 9088)),
        ]):
            with self.assertRaises(SqliUnsupportedAuthentication):
                client._validate_redirect_destination("target", 9088, True)
        client.redirect_allowlist = frozenset({("target", 9088), ("127.0.0.1", 9088)})
        with patch("socket.getaddrinfo", return_value=[
            (2, 1, 6, "", ("127.0.0.1", 9088)),
        ]):
            client._validate_redirect_destination("target", 9088, True)

    def test_session_header_round_trip(self):
        packet = encode_session_packet(1, b"request")
        self.assertEqual(read_session_packet(io.BytesIO(packet)), (1, 60, b"request"))
        flagged = bytearray(encode_session_packet(2, b"accepted"))
        flagged[4:6] = b"\x10\x00"
        self.assertEqual(read_session_packet(io.BytesIO(flagged)), (2, 60, b"accepted"))
        flagged[4:6] = b"\x20\x00"
        with self.assertRaises(SqliProtocolError):
            read_session_packet(io.BytesIO(flagged))

    def test_char_padding(self):
        self.assertEqual(encode_char(b"abc"), b"\x00\x03abc\x00")
        self.assertEqual(encode_char(b"abcd"), b"\x00\x04abcd")

    def test_asc_strings_are_nul_terminated_without_even_padding(self):
        self.assertEqual(encode_asc_char("ab"), b"\x00\x03ab\0")
        self.assertEqual(encode_asc_char("abc"), b"\x00\x04abc\0")

    def test_normal_auth_prefix_golden_fields(self):
        prefix = encode_normal_auth_prefix("alice", "secret", "srv", "db")
        self.assertTrue(prefix.startswith(struct.pack(">hhi", 100, 101, 61)))
        self.assertIn(encode_asc_char("alice"), prefix)
        self.assertIn(encode_asc_char("secret"), prefix)
        self.assertIn(struct.pack(">i", 316), prefix)
        self.assertTrue(prefix.endswith(struct.pack(">hhhhh", 0, 0, 0, 0, 0)))
        self.assertNotIn(b"db\0", prefix)

    def test_live_mandatory_environment_is_deterministic(self):
        values = {
            "CLIENT_LOCALE": "en_US.819",
            "CLNT_PAM_CAPABLE": "1",
            "DBPATH": ".",
            "DB_LOCALE": "en_US.819",
            "IFX_UPDDESC": "1",
            "NODEFDAC": "no",
        }
        encoded = encode_asc_environment(values, "iso8859-1")
        self.assertTrue(encoded.startswith(struct.pack(">hh", 106, 6)))
        offsets = [encoded.index(key.encode() + b"\0") for key in sorted(values)]
        self.assertEqual(offsets, sorted(offsets))

    def test_complete_auth_request_and_accept_decode(self):
        request = encode_normal_auth_request(
            "alice", "secret", "srv", "db", {"CLIENT_LOCALE": "en_US.utf8"}
        )
        sl_type, protocol, payload = read_session_packet(io.BytesIO(request))
        self.assertEqual((sl_type, protocol), (1, 60))
        self.assertTrue(payload.endswith(struct.pack(">h", 127)))
        body = (
            struct.pack(">hh", 100, 101)
            + b"\0" * 4
            + struct.pack(">h", 0)
            + struct.pack(">h", 108)
            + b"\0" * 12
            + struct.pack(">h", 3)
            + b"316"
            + struct.pack(">hhiii", 0, 0, 1, 2, 3)
            + b"\0" * 2
            + struct.pack(">hh", 0, 0)
            + b"\0" * 24
            + struct.pack(">h", 127)
        )
        accepted = decode_asc_accept(encode_session_packet(2, body))
        self.assertEqual(
            (accepted.version, accepted.cap_1, accepted.cap_2, accepted.cap_3), ("316", 1, 2, 3)
        )

    def test_accept_and_redirect_states(self):
        accepted = encode_session_packet(2, struct.pack(">hh", 100, 101) + b"caps")
        self.assertEqual(decode_asc_response(accepted), b"caps")
        with self.assertRaises(SqliUnsupportedAuthentication):
            decode_asc_response(encode_session_packet(13, b"redirect"))

    def test_live_initresp_metadata_tail_is_bounded_by_asceot(self):
        fixed = (
            struct.pack(">hh", 100, 101)
            + b"\0" * 4
            + struct.pack(">h", 0)
            + struct.pack(">h", 108)
            + b"\0" * 12
            + struct.pack(">h", 3)
            + b"316"
            + struct.pack(">hhiii", 0, 0, 316, 0, 0)
            + b"\0" * 2
            + struct.pack(">hh", 0, 0)
            + b"\0" * 24
            + struct.pack(">h", 102)
            + b"\0" * 12
            + b"sanitized-server-metadata"
            + struct.pack(">h", 127)
        )
        packet = bytearray(encode_session_packet(2, fixed))
        packet[4:6] = b"\x10\x00"
        self.assertEqual(decode_asc_accept(bytes(packet)).version, "316")

    def test_simple_command_group(self):
        command = encode_simple_command("SELECT 1")
        self.assertTrue(command.startswith(struct.pack(">hh", 1, 0)))
        self.assertTrue(command.endswith(struct.pack(">hhhh", 22, 7, 11, 12)))

    def test_protocol_and_secondary_environment_groups(self):
        self.assertEqual(
            encode_protocol_offer(),
            struct.pack(">hh", 126, 9)
            + bytes.fromhex("ff fc 7f fc 3c 8c aa 97 06")
            + b"\0\x00\x0c",
        )
        info = encode_secondary_info({"CLIENT_LOCALE": "en_US.utf8"})
        self.assertTrue(info.startswith(struct.pack(">hh", 81, 6)))
        self.assertTrue(info.endswith(struct.pack(">hhh", 0, 0, 12)))

    def test_forward_cursor_messages(self):
        self.assertEqual(encode_cursor_open(7, "c")[:6], struct.pack(">hhh", 4, 7, 3))
        self.assertEqual(encode_fetch(7), struct.pack(">hhhihh", 4, 7, 9, 32767, 0, 12))
        self.assertEqual(encode_close_release(7), struct.pack(">hhhh", 4, 7, 10, 12))
        self.assertEqual(encode_close_release(7, True), struct.pack(">hhhh", 4, 7, 11, 12))

    def test_variable_fetch_sends_ret_type_before_nfetch(self):
        description = ResultDescription(2, 7, 20, (ResultColumn("v", 0, 13, 0, 20),))
        self.assertEqual(
            encode_variable_fetch(description, 4096),
            struct.pack(">hhhhhhihihh", 4, 7, 100, 1, 1, 13, 20, 9, 4096, 0, 12),
        )

    def test_variable_fetch_resolves_builtin_extended_types(self):
        description = ResultDescription(
            2,
            7,
            17,
            (
                ResultColumn("text", 0, 40, 1, 16, "informix", "lvarchar"),
                ResultColumn("enabled", 16, 41, 5, 1, "informix", "boolean"),
            ),
        )
        expected = bytearray(struct.pack(">hhhhh", 4, 7, 100, 1, 2))
        expected.extend(struct.pack(">h", 43))
        expected.extend(encode_char(""))
        expected.extend(encode_char(""))
        expected.extend(struct.pack(">i", 16))
        expected.extend(struct.pack(">h", 45))
        expected.extend(encode_char("informix"))
        expected.extend(encode_char("boolean"))
        expected.extend(struct.pack(">i", 1))
        expected.extend(struct.pack(">hihh", 9, 4096, 0, 12))

        self.assertEqual(encode_variable_fetch(description, 4096), bytes(expected))

    def test_variable_fetch_rejects_user_defined_builtin_name(self):
        description = ResultDescription(
            2,
            7,
            16,
            (ResultColumn("value", 0, 40, 9, 16, "application", "lvarchar"),),
        )

        with self.assertRaisesRegex(SqliDescriptorNotImplemented, "result type 40"):
            encode_variable_fetch(description)

    def test_mixed_lvarchar_and_boolean_tuple_decodes_variable_envelopes(self):
        payload = (
            b"\x00"
            + struct.pack(">i", 5)
            + b"hello"
            + b"\x00"
            + struct.pack(">i", 1)
            + b"\x01"
        )
        transcript = struct.pack(">hi", 0, len(payload)) + payload
        if len(payload) & 1:
            transcript += b"\x00"
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(transcript)
        description = ResultDescription(
            2,
            7,
            len(payload),
            (
                ResultColumn("text", 0, 43, 0, 100),
                ResultColumn("enabled", 10, 45, 0, 1),
            ),
        )

        self.assertEqual(
            client._read_tuple(description), {"text": "hello", "enabled": True}
        )

    def test_padded_varchar_does_not_disable_lvarchar_envelope_walking(self):
        payload = (
            b"ab  "
            + b"\x00"
            + struct.pack(">i", 5)
            + b"hello"
            + b"\x00"
            + struct.pack(">i", 1)
            + b"\x01"
        )
        transcript = struct.pack(">hi", 0, len(payload)) + payload
        if len(payload) & 1:
            transcript += b"\x00"
        client = InformixSqliClient(
            "host", 9088, "db", "user", "password", pad_varchar=True
        )
        client._input = io.BytesIO(transcript)
        description = ResultDescription(
            2,
            7,
            len(payload),
            (
                ResultColumn("padded", 0, 13, 0, 4),
                ResultColumn("text", 4, 43, 0, 100),
                ResultColumn("enabled", 14, 45, 0, 1),
            ),
        )

        self.assertEqual(
            client._read_tuple(description),
            {"padded": "ab", "text": "hello", "enabled": True},
        )

    def test_variable_tuple_positions_do_not_bound_runtime_envelopes(self):
        payload = (
            b"\x00"
            + struct.pack(">i", 5)
            + b"hello"
            + b"\x00"
            + struct.pack(">i", 1)
            + b"\x01"
        )
        transcript = struct.pack(">hi", 0, len(payload)) + payload
        if len(payload) & 1:
            transcript += b"\x00"
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(transcript)
        description = ResultDescription(
            2,
            7,
            len(payload),
            (
                ResultColumn("text", 0, 43, 0, 100),
                ResultColumn("enabled", 9, 45, 0, 1),
            ),
        )

        self.assertEqual(
            client._read_tuple(description), {"text": "hello", "enabled": True}
        )

    def test_live_feature62_prepare_and_fixed_open_fetch_goldens(self):
        prepare = encode_prepare("x" * 38, parameter_count=0, long_sql_length=True)
        self.assertEqual(len(prepare), 52)
        self.assertEqual(prepare[:8], struct.pack(">hhi", 2, 0, 38))
        combined = encode_fixed_open_fetch(7, "cursor_name_123456", buffer_size=4096)
        self.assertEqual(len(combined), 42)
        self.assertTrue(combined.endswith(struct.pack(">hhhhihh", 6, 4, 7, 9, 4096, 0, 12)))

    def test_cost_xact_done_close_dispatch(self):
        transcript = (
            struct.pack(">hii", 55, 10, 20)
            + struct.pack(">hhhh", 99, 1, 2, 1)
            + struct.pack(">hhiii", 15, 0, 3, 4, 5)
            + struct.pack(">hh", 10, 12)
        )
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(transcript)
        client._read_status_group()
        self.assertEqual(client._input.tell(), len(transcript))

    def test_query_close_marks_cursor_exhausted(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(struct.pack(">hh", 10, 12))
        _, rows, exhausted = client._read_query_group(None)
        self.assertEqual(rows, [])
        self.assertTrue(exhausted)

    def test_insertdone_consumes_serial_metadata(self):
        description = ResultDescription(6, 7, 0, ())
        for bigint_supported, body in (
            (False, b"s" * 10),
            (True, b"s" * 10 + b"b" * 8),
        ):
            client = InformixSqliClient("host", 9088, "db", "user", "password")
            client.bigint_supported = bigint_supported
            transcript = struct.pack(">h", 94) + body + struct.pack(">h", 12)
            client._input = io.BytesIO(transcript)

            client._read_query_group(description)

            self.assertEqual(client._input.tell(), len(transcript))

    def test_insertdone_outside_insert_fails_closed(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(struct.pack(">h", 94) + b"s" * 10)

        with self.assertRaisesRegex(SqliProtocolError, "outside an INSERT"):
            client._read_query_group(ResultDescription(2, 7, 0, ()))

    def test_status_group_consumes_insertdone_serial_metadata(self):
        for bigint_supported, body in (
            (False, b"s" * 10),
            (True, b"s" * 10 + b"b" * 8),
        ):
            with self.subTest(bigint_supported=bigint_supported):
                done = struct.pack(">hhiii", 15, 0, 1, 0, 0)
                transcript = (
                    struct.pack(">hh", 8, 94)
                    + body
                    + done
                    + struct.pack(">h", 12)
                )
                client = InformixSqliClient("host", 9088, "db", "user", "password")
                client.bigint_supported = bigint_supported
                client._input = io.BytesIO(transcript)
                with patch.object(
                    client,
                    "_read_description",
                    return_value=ResultDescription(6, 7, 0, ()),
                ):
                    client._read_status_group(require_done=True)

                self.assertEqual(client._input.tell(), len(transcript))

    def test_status_group_rejects_insertdone_for_non_insert(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(struct.pack(">hh", 8, 94) + b"s" * 10)
        with patch.object(
            client,
            "_read_description",
            return_value=ResultDescription(4, 7, 0, ()),
        ), self.assertRaisesRegex(SqliProtocolError, "outside an INSERT"):
            client._read_status_group()

    def test_zero_row_done_marks_fetch_exhausted(self):
        done = struct.pack(">hhiii", 15, 0, 0, 0, 0) + struct.pack(">h", 12)
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(done)
        description = ResultDescription(2, 7, 0, ())
        _, rows, exhausted = client._read_query_group(description)
        self.assertEqual(rows, [])
        self.assertTrue(exhausted)

    def test_done_without_tuple_marks_fetch_exhausted_even_with_cumulative_count(self):
        done = struct.pack(">hhiii", 15, 0, 1, 0, 0) + struct.pack(">h", 12)
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client._input = io.BytesIO(done)
        description = ResultDescription(2, 7, 0, ())
        _, rows, exhausted = client._read_query_group(description)
        self.assertEqual(rows, [])
        self.assertTrue(exhausted)

    def test_lodata_dispatch_group(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client.state = ConnectionState.DATABASE_OPEN
        client._input = io.BytesIO(
            struct.pack(">hhih", 97, 0, 3, 3) + b"abc\0" + struct.pack(">h", 12)
        )
        client._output = io.BytesIO()
        self.assertEqual(client.read_lodata(4, 10), b"abc")
        self.assertEqual(
            client._output.getvalue(), encode_lodata_read(4, 10) + struct.pack(">h", 12)
        )

    def test_lodata_dispatch_rejects_unknown_direction_and_negative_size(self):
        for operation, size in ((2, 0), (3, 0), (2, -1), (0, -1)):
            client = InformixSqliClient("host", 9088, "db", "user", "password")
            client.state = ConnectionState.DATABASE_OPEN
            client._input = io.BytesIO(struct.pack(">hhi", 97, operation, size))
            client._output = io.BytesIO()
            with self.assertRaises(SqliProtocolError):
                client.read_lodata(4, 10)

    def test_lodata_decodes_informix_error_body(self):
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client.state = ConnectionState.DATABASE_OPEN
        client._input = io.BytesIO(
            struct.pack(">hhhh", 13, -23197, -255, 7) + encode_char("TLS login failed")
        )
        client._output = io.BytesIO()

        with self.assertRaisesRegex(
            SqliProtocolError, r"-23197/-255 at 7 during LODATA: TLS login failed"
        ):
            client.read_lodata(4, 10)

    def test_describe_and_tuple_query_group(self):
        def descriptor(position, type_code, length):
            return (
                struct.pack(">iih", 0, position, type_code)
                + struct.pack(">i", 0)
                + encode_char("")
                + encode_char("")
                + struct.pack(">hhiii", 0, 0, type_code, length, 0)[:-4]
            )

        names = b"a\0b\0"
        describe = (
            struct.pack(">h", 8)
            + struct.pack(">hhihhi", 1, 7, 1, 8, 2, len(names))
            + descriptor(0, 2, 4)
            + descriptor(4, 13, 4)
            + names
        )
        payload = struct.pack(">i", 42) + b"\x03abc"
        transcript = (
            describe
            + struct.pack(">hh", 12, 12)
            + struct.pack(">hhi", 14, 0, len(payload))
            + payload
            + struct.pack(">hhhh", 13, 100, 0, 0)
            + encode_char("")
            + struct.pack(">hhh", 12, 12, 12)
        )
        client = InformixSqliClient("host", 9088, "db", "user", "password")
        client.state = ConnectionState.DATABASE_OPEN
        client._input, client._output = io.BytesIO(transcript), io.BytesIO()
        self.assertEqual(client.execute("SELECT ?", ("x'y",)), [{"a": 42, "b": "abc"}])
        self.assertIn(b"SELECT 'x''y'", client._output.getvalue())

    def test_full_synthetic_connect_state_machine(self):
        asc_body = (
            struct.pack(">hh", 100, 101)
            + b"\0" * 4
            + struct.pack(">h", 0)
            + struct.pack(">h", 108)
            + b"\0" * 12
            + struct.pack(">h", 3)
            + b"316"
            + struct.pack(">hhiii", 0, 0, 1, 2, 3)
            + b"\0" * 2
            + struct.pack(">hh", 0, 0)
            + b"\0" * 24
            + struct.pack(">h", 127)
        )
        transcript = (
            encode_session_packet(2, asc_body)
            + struct.pack(">hh", 126, 9)
            + bytes.fromhex("ff fc 7f fc 3c 8c aa 97 06")
            + b"\0"
            + struct.pack(">hhh", 12, 12, 15)
            + struct.pack(">hqqi", 0, 0, 0, 0)
            + struct.pack(">h", 12)
        )

        class FakeSocket:
            def __init__(self):
                self.input, self.output = io.BytesIO(transcript), io.BytesIO()

            def settimeout(self, _value):
                pass

            def setsockopt(self, *_args):
                pass

            def makefile(self, mode, buffering=0):
                return self.input if "r" in mode else self.output

            def close(self):
                pass

        fake = FakeSocket()

        class FakeContext:
            def wrap_socket(self, raw, server_hostname=None):
                return raw

        client = InformixSqliClient(
            "host",
            9088,
            "db",
            "user",
            "password",
            server_name="srv",
            db_locale="en_US.utf8",
            client_locale="en_US.utf8",
            ssl_context=FakeContext(),
        )
        with patch("socket.create_connection", return_value=fake):
            self.assertIs(client.connect(), client)
        self.assertEqual(client.state, ConnectionState.DATABASE_OPEN)
        self.assertTrue(client.remove_64k_limit)
        self.assertTrue(client.large_tuple_size)
        self.assertTrue(client.long_row_id)
        self.assertEqual(fake.output.getvalue()[2], 1)


if __name__ == "__main__":
    unittest.main()
