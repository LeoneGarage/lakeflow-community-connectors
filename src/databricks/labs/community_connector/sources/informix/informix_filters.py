"""Row filters, column transforms and renames, and the Spark types of Informix columns."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import re
import struct
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal, localcontext
from typing import Any, Iterable

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
    TimestampType,
)

from databricks.labs.community_connector.sources.informix.cdc_protocol import interval_layout
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _EXPRESSION_FUNCTIONS,
    _FLOAT_KINDS,
    _ROW_FILTER_KEYWORDS,
    _ROW_FILTER_LIKE_ESCAPE,
    _ROW_FILTER_TEMPORAL,
    _ROW_FILTER_TOKEN,
    _expression_text,
    _framework_value,
    _is_variable_scale_decimal,
    _parse_key_columns,
    _row_filter_literal_text,
    _variable_decimal_target,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _IDENTIFIER,
    _UNLOAD_FLOAT_TYPES,
    COMMIT_LSN,
    COMMIT_TIME,
    CURSOR,
    OP,
    TX_ID,
    InformixError,
    _m,
    _sql_name,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _STATE_VALIDATION_COORDINATOR,
    Column,
    Table,
)


class _RowFilterParser:
    # Docstring: Recursive-descent parser turning row_filter text into a hashable tree.
    #
    # Nodes: ("and"|"or", children), ("not", child), ("cmp", operand, op, literal),
    # ("in", operand, negated, literals), ("between", operand, negated, low, high),
    # ("like", operand, negated, pattern, escape), and ("null", operand, negated). An
    # operand is a column name or a function call ("fn", NAME, (operand, *literals)) --
    # see _EXPRESSION_FUNCTIONS. A literal is (kind, value) with kind number, string,
    # bool, date, or timestamp; a LIKE pattern is a string literal with its trailing
    # blanks removed, since Informix ignores them, and its escape is the one character
    # ESCAPE names (backslash if none). transform_columns parses one operand per column
    # with the same grammar (``option``/``noun`` only change the error text).

    def __init__(self, text: str, option: str = "row_filter", noun: str = "filter") -> None:
        self.option, self.noun = option, noun
        self.tokens: list[tuple[str, str]] = []
        self.index = 0
        text = text.strip()
        if "\x00" in text:
            # Informix SQL can't be given one: a filter's snapshot failed on it after the
            # stream had taken it, and a transform could put one in a landed string.
            self.fail("it holds a NUL character")
        position = 0
        while position < len(text):
            match = _ROW_FILTER_TOKEN.match(text, position)
            if match is None or match.lastgroup is None:
                self.fail(f"unexpected character at position {position + 1}")
            self.tokens.append((match.lastgroup, match.group(match.lastgroup)))
            position = match.end()

    def fail(self, message: str) -> None:
        # 112: Option '{}' is not a valid {}: {}
        raise ValueError(_m(112, self.option, self.noun, message))

    def peek(self) -> tuple[str | None, str | None]:
        return self.tokens[self.index] if self.index < len(self.tokens) else (None, None)

    def take(self) -> tuple[str | None, str | None]:
        token = self.peek()
        self.index += 1
        return token

    def at_keyword(self, word: str) -> bool:
        kind, value = self.peek()
        return kind == "word" and str(value).upper() == word

    def accept_keyword(self, word: str) -> bool:
        if self.at_keyword(word):
            self.index += 1
            return True
        return False

    def expect_keyword(self, word: str) -> None:
        if not self.accept_keyword(word):
            self.fail(f"expected {word}")

    def expect_punct(self, character: str) -> None:
        if self.take() != ("punct", character):
            self.fail(f"expected '{character}'")

    def parse(self) -> tuple:
        if not self.tokens:
            self.fail("it is empty")
        tree = self.parse_or()
        if self.index != len(self.tokens):
            self.fail(f"unexpected {self.peek()[1]!r}")
        return tree

    def parse_or(self) -> tuple:
        terms = [self.parse_and()]
        while self.accept_keyword("OR"):
            terms.append(self.parse_and())
        return terms[0] if len(terms) == 1 else ("or", tuple(terms))

    def parse_and(self) -> tuple:
        terms = [self.parse_not()]
        while self.accept_keyword("AND"):
            terms.append(self.parse_not())
        return terms[0] if len(terms) == 1 else ("and", tuple(terms))

    def parse_not(self) -> tuple:
        if self.accept_keyword("NOT"):
            return ("not", self.parse_not())
        if self.peek() == ("punct", "("):
            self.index += 1
            tree = self.parse_or()
            self.expect_punct(")")
            return tree
        return self.parse_predicate()

    def parse_operand(self) -> str | tuple:
        kind, name = self.take()
        if kind == "quoted":
            # A delimited name is a column's name as written, never a function or a keyword.
            name = str(name)[1:-1].replace('""', '"')
            if not _IDENTIFIER.fullmatch(name):
                self.fail(f"expected a column name, found {name!r}")
            return name
        if kind != "word" or not _IDENTIFIER.fullmatch(str(name)):
            self.fail(f"expected a column name, found {name!r}")
        upper = str(name).upper()
        if self.peek() == ("punct", "(") and upper not in _EXPRESSION_FUNCTIONS:
            self.fail(f"{upper}() is not a supported function")
        if self.peek() == ("punct", "(") and upper in _EXPRESSION_FUNCTIONS:
            self.index += 1
            arguments = [self.parse_operand()]
            while self.peek() == ("punct", ","):
                self.index += 1
                arguments.append(self.parse_literal())
            self.expect_punct(")")
            return ("fn", upper, tuple(arguments))
        if upper in _ROW_FILTER_KEYWORDS:
            self.fail(f"expected a column name, found {name!r}")
        return str(name)

    def parse_expression(self) -> str | tuple:
        if not self.tokens:
            self.fail("it is empty")
        operand = self.parse_operand()
        if self.index != len(self.tokens):
            self.fail(f"unexpected {self.peek()[1]!r}")
        return operand

    def parse_predicate(self) -> tuple:
        operand = self.parse_operand()
        column = _expression_text(operand)
        if self.accept_keyword("IS"):
            negated = self.accept_keyword("NOT")
            self.expect_keyword("NULL")
            return ("null", operand, negated)
        negated = self.accept_keyword("NOT")
        if self.accept_keyword("IN"):
            self.expect_punct("(")
            values = [self.parse_literal()]
            while self.peek() == ("punct", ","):
                self.index += 1
                values.append(self.parse_literal())
            self.expect_punct(")")
            return ("in", operand, negated, tuple(values))
        if self.accept_keyword("BETWEEN"):
            low = self.parse_literal()
            self.expect_keyword("AND")
            return ("between", operand, negated, low, self.parse_literal())
        if self.accept_keyword("LIKE"):
            return self.parse_like(operand, column, negated)
        if negated:
            self.fail(f"expected IN, BETWEEN, or LIKE after NOT for {column}")
        kind, op = self.take()
        if kind != "op":
            self.fail(f"expected a comparison after {column}")
        return ("cmp", operand, "<>" if op == "!=" else op, self.parse_literal())

    def parse_like(self, operand: str | tuple, column: str, negated: bool) -> tuple:
        kind, pattern = self.parse_literal()
        if kind != "string":
            self.fail(f"LIKE for {column} needs a quoted pattern")
        escape = _ROW_FILTER_LIKE_ESCAPE
        if self.accept_keyword("ESCAPE"):
            kind, escape = self.parse_literal()
            if kind != "string" or len(escape) != 1 or escape in "%_ ":
                self.fail(f"ESCAPE for {column} must be one character other than %, _, or a space")
        pattern = pattern.rstrip(" ")
        index = 0
        while index < len(pattern):
            if pattern[index] == escape:
                if index + 1 == len(pattern):
                    # Informix matches such a pattern inconsistently, so it is refused.
                    self.fail(f"LIKE pattern for {column} ends with its escape character")
                index += 1
            index += 1
        return ("like", operand, negated, ("string", pattern), escape)

    def parse_literal(self) -> tuple:
        kind, value = self.take()
        text = str(value)
        if kind == "number":
            return ("number", Decimal(text))
        if kind == "string":
            return ("string", text[1:-1].replace("''", "'"))
        upper = text.upper()
        if kind == "word" and upper in {"TRUE", "FALSE"}:
            return ("bool", upper == "TRUE")
        if kind == "word" and upper in {"DATE", "TIMESTAMP"}:
            quoted_kind, quoted = self.take()
            match = (
                _ROW_FILTER_TEMPORAL.fullmatch(str(quoted)[1:-1])
                if quoted_kind == "string"
                else None
            )
            if match is None or (upper == "DATE" and match.group(4) is not None):
                self.fail(f"expected {upper} 'YYYY-MM-DD{'' if upper == 'DATE' else ' HH:MM:SS'}'")
            year, month, day, hour, minute, second, fraction = match.groups()
            try:
                if upper == "DATE":
                    return ("date", date(int(year), int(month), int(day)))
                micro = int((fraction + "000000")[:6]) if fraction else 0
                return (
                    "timestamp",
                    datetime(
                        int(year),
                        int(month),
                        int(day),
                        int(hour or 0),
                        int(minute or 0),
                        int(second or 0),
                        micro,
                    ),
                )
            except ValueError:
                self.fail(f"{upper} {quoted} is not a valid calendar value")
        if kind == "word" and upper == "NULL":
            self.fail("compare with NULL using IS NULL or IS NOT NULL")
        self.fail(f"expected a value, found {value!r}")
        raise AssertionError  # unreachable: fail() raises


def _row_filter_text(tree: tuple) -> str:
    # Docstring: Canonical row_filter text: fully parenthesised with normalised literals.
    #
    # Formatting-only edits (spacing, keyword case, ``!=`` for ``<>``, ``1.50`` for
    # ``1.5``) therefore keep the fingerprint, and the text parses back to the same tree.

    tag = tree[0]
    if tag in ("and", "or"):
        return f" {tag.upper()} ".join(f"({_row_filter_text(child)})" for child in tree[1])
    if tag == "not":
        return f"NOT ({_row_filter_text(tree[1])})"
    column = _expression_text(tree[1])
    if tag == "null":
        return f"{column} IS {'NOT ' if tree[2] else ''}NULL"
    if tag == "cmp":
        return f"{column} {tree[2]} {_row_filter_literal_text(tree[3])}"
    negation = "NOT " if tree[2] else ""
    if tag == "in":
        values = ", ".join(_row_filter_literal_text(value) for value in tree[3])
        return f"{column} {negation}IN ({values})"
    if tag == "like":
        pattern = _row_filter_literal_text(tree[3])
        escape = _row_filter_literal_text(("string", tree[4]))
        return f"{column} {negation}LIKE {pattern} ESCAPE {escape}"
    low, high = _row_filter_literal_text(tree[3]), _row_filter_literal_text(tree[4])
    return f"{column} {negation}BETWEEN {low} AND {high}"


def _snapshot_filter(options: dict[str, str]) -> str | None:
    # Docstring: The table's snapshot.filter, parsed in row_filter's grammar and written out
    # in its canonical form, or None.
    #
    # It was an SQL predicate taken as given, which a pipeline author can set for each
    # table: its subqueries read tables the connection's include and exclude lists leave
    # out, and its function calls ran routines, as the connection's user. In row_filter's
    # grammar it compares the table's own columns with literals, through the functions
    # row_filter allows; _snapshot_predicate checks it against the table.

    value = options.get("snapshot.filter")
    if value is None:
        return None
    predicate = value.strip()
    if not predicate:
        return None
    if any(ord(character) < 32 for character in predicate):
        # 74: Option 'snapshot.filter' cannot contain control characters
        raise ValueError(_m(74))
    canonical = _row_filter_text(_RowFilterParser(predicate, "snapshot.filter").parse())
    if max(len(predicate), len(canonical)) > 8192:
        # 73: Option 'snapshot.filter' cannot exceed 8192 characters
        raise ValueError(_m(73))
    return canonical


_ROW_FILTER_CACHE: dict[str, tuple] = {}


def _parse_row_filter(text: str) -> tuple:
    # Docstring: Parse ``text`` with :class:`_RowFilterParser`, caching the tree by text.

    tree = _ROW_FILTER_CACHE.get(text)
    if tree is None:
        tree = _RowFilterParser(text).parse()
        if len(_ROW_FILTER_CACHE) >= 64:
            _ROW_FILTER_CACHE.clear()
        _ROW_FILTER_CACHE[text] = tree
    return tree


_ROW_FILTER_OPTION = "row_filter"


def _expression_column(operand: str | tuple) -> str:
    # Docstring: The column an operand reads (every function applies to one operand).

    while not isinstance(operand, str):
        operand = operand[2][0]
    return operand


# Functions whose result is an integer whatever their input; the others keep the type of
# what they apply to (strings stay strings, ABS/ROUND/TRUNC/MOD/NVL keep a number's type).
_INTEGER_RESULT_FUNCTIONS = frozenset({"YEAR", "MONTH", "DAY", "WEEKDAY", "LENGTH", "CHAR_LENGTH"})

_STRING_FUNCTION_NAMES = frozenset(
    {
        "UPPER",
        "LOWER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "LENGTH",
        "CHAR_LENGTH",
        "SUBSTR",
        "REPLACE",
        "LPAD",
        "RPAD",
        "MASK",
    }
)


def _variable_decimal_spark_type(kind: str, precision: int | None, scale: int | None):
    if kind == "string":
        return StringType()
    if kind == "double":
        return DoubleType()
    if kind == "integer":
        return LongType()
    return DecimalType(precision, scale)


def _spark_type(column: Column, options: dict[str, str] | None = None):
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
        precision = (
            column.precision if column.precision is not None else (19 if name == "MONEY" else 38)
        )
        scale = column.scale if column.scale is not None else (2 if name == "MONEY" else 0)
        if name in {"DECIMAL", "NUMERIC"} and scale == 0xFF:
            # Variable-scale DECIMAL(p) is decimal floating point: p is the count
            # of significant digits and the exponent floats, so the magnitude is
            # not bounded by p. Map it to the target chosen by the
            # decimal.variable.type / decimal.variable.column.type options.
            kind, target_precision, target_scale = _variable_decimal_target(column, options or {})
            return _variable_decimal_spark_type(kind, target_precision, target_scale)
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


def _transform_spark_type(operand: str | tuple, column: "Column", options: dict[str, str]):
    # Docstring: The Spark type a transform lands as, given the column it reads.

    if isinstance(operand, str):
        return _spark_type(column, options)
    name = operand[1]
    if name in _INTEGER_RESULT_FUNCTIONS:
        # Before the string functions: LENGTH and CHAR_LENGTH read a string, give an int.
        return IntegerType()
    if name in ("HASH", "MASK") or name in _STRING_FUNCTION_NAMES:
        return StringType()
    inner = _transform_spark_type(operand[2][0], column, options)
    if name == "ROUND" and isinstance(inner, DecimalType):
        # Rounding up can carry into one more integer digit (999.99 to 1000).
        if inner.precision < 38:
            return DecimalType(inner.precision + 1, inner.scale)
        # Spark has no 39th digit. Rounding to the scale or past it takes nothing off and
        # carries nothing; rounding to fewer places frees the digits it takes off, so the
        # type keeps those places only and has room for the carry (a DECIMAL(38,18) of
        # twenty nines and a half rounded to 0 places is 10^20, which needs 39 digits at
        # scale 18).
        literals = operand[2][1:]
        digits = int(literals[0][1]) if literals else 0
        if digits >= inner.scale:
            return inner
        return DecimalType(38, max(digits, 0))
    if name == "ROUND" and isinstance(inner, IntegerType):
        # To a negative digit it can pass the largest INTEGER (2147483647 to the
        # thousand is 2147484000): the column lands as a BIGINT then.
        literals = operand[2][1:]
        if literals and int(literals[0][1]) < 0:
            return LongType()
    return inner


def _check_function_literals(name: str, values: list, option: str) -> None:
    # Docstring: Refuse literal arguments outside the range each function was measured on.

    def whole(value, low, high) -> bool:
        return value == value.to_integral_value() and low <= value <= high

    if name == "SUBSTR" and not (
        whole(values[0], 1, 32767) and (len(values) == 1 or whole(values[1], 0, 32767))
    ):
        # 105: Option '{}': SUBSTR takes a start from 1 and a length from 0
        raise InformixError(_m(105, option))
    if name in ("LPAD", "RPAD") and not (whole(values[0], 0, 32767) and values[1]):
        # 106: Option '{}': {} takes a length from 0 and a pad string
        raise InformixError(_m(106, option, name))
    if name == "REPLACE" and not values[0]:
        # 107: Option '{}': REPLACE needs a non-empty string to replace
        raise InformixError(_m(107, option))
    if name == "MOD" and not (whole(values[0], -(1 << 31), (1 << 31) - 1) and values[0]):
        # 108: Option '{}': MOD takes a whole, non-zero divisor
        raise InformixError(_m(108, option))
    if name in ("ROUND", "TRUNC") and values and not whole(values[0], -32, 32):
        # 109: Option '{}': {} takes whole digits from -32 to 32
        raise InformixError(_m(109, option, name))
    if name == "MASK" and (
        (values and not whole(values[0], 0, 32767)) or (len(values) > 1 and len(values[1]) != 1)
    ):
        # 110: Option '{}': MASK takes a count from 0 and one character
        raise InformixError(_m(110, option))


_ROW_FILTER_NUMERIC_TYPES = frozenset(
    {
        "SMALLINT",
        "INT2",
        "INTEGER",
        "INT",
        "SERIAL",
        "BIGINT",
        "INT8",
        "BIGSERIAL",
        "SERIAL8",
        "DECIMAL",
        "NUMERIC",
        "MONEY",
    }
)
_ROW_FILTER_STRING_TYPES = frozenset({"CHAR", "NCHAR", "VARCHAR", "LVARCHAR", "NVARCHAR"})


def _row_filter_kind(column: Column) -> str | None:
    # Docstring: The literal kind a row_filter may compare ``column`` with, or None if none.

    name = column.type_name.split("(", 1)[0].strip()
    if name in _ROW_FILTER_NUMERIC_TYPES:
        return "number"
    if name in _UNLOAD_FLOAT_TYPES:
        return "smallfloat" if name in {"SMALLFLOAT", "REAL"} else "float"
    if name in _ROW_FILTER_STRING_TYPES:
        return "nstring" if name in {"NCHAR", "NVARCHAR"} else "string"
    if name in {"BOOLEAN", "BOOL"}:
        return "bool"
    if name == "DATE":
        return "date"
    if name.startswith("DATETIME"):
        # Only a YEAR-anchored qualifier down to at least DAY decodes to a datetime.
        start, end = ((column.length or 0) >> 8) & 0xF, (column.length or 0) & 0xF
        if start == 0 and end >= 4:
            return "timestamp"
    return None


def _decimal_literal(value: Decimal, option: str, name: str) -> None:
    # Docstring: Refuse a number past an Informix DECIMAL's range. Written out in full, a
    # filter's is a literal Informix can't read; an NVL fallback of 1e250 failed the first
    # NULL as rounding past 200 digits, and 1e400 landed as an infinite DOUBLE.

    if value and not Decimal("1e-130") <= value.copy_abs() <= Decimal("1e125"):
        # 50: Option '{}' gives {} the number {}, outside what an Informix DECIMAL holds: 0,
        # or a magnitude from 1e-130 to 1e125
        raise InformixError(_m(50, option, name, value))


def _operand_functions(operand: str | tuple) -> list[str]:
    # Docstring: The functions an operand applies, outermost first.

    names = []
    while not isinstance(operand, str):
        names.append(operand[1])
        operand = operand[2][0]
    return names


def _float32(value: float) -> float:
    # Docstring: ``value`` rounded to the nearest 32-bit float, as a Python float.

    return struct.unpack("f", struct.pack("f", value))[0]


def _float_literal(value: Decimal, kind: str, option: str, name: str) -> None:
    # Docstring: Refuse a number a FLOAT or SMALLFLOAT operand ``name`` cannot take exactly.
    #
    # Informix reads a literal into a double correctly rounded but for one within about
    # 1e-16 of a step of halfway between two (30 or more digits) and at the bottom of the
    # double range, so 17 digits, which name any double, and the range of an Informix
    # DECIMAL literal; a SMALLFLOAT compares its 32 bits, so a normal one of those (checked
    # live on 14.10 for <, <=, >, >=, =, <>, BETWEEN and IN).

    digits = len("".join(map(str, value.as_tuple().digits)).strip("0"))
    if digits > 17:
        # 480: Option '{}' gives {} the number {}, which has more than 17 significant digits; a
        # FLOAT or SMALLFLOAT takes at most 17, which name any of its values
        raise InformixError(_m(480, option, name, value))
    if kind == "smallfloat":
        low, high = "1.17549435e-38", "3.40282347e38"
        try:
            # Informix refuses one past the largest (1222), and a subnormal.
            rounded = abs(_float32(float(value)))
            fits = not value or 1.1754943508222875e-38 <= rounded < math.inf
        except OverflowError:
            fits = False
    else:
        low, high = "1e-130", "1e125"
        fits = not value or Decimal(low) <= abs(value) <= Decimal(high)
    if not fits:
        # 481: Option '{}' gives {} the number {}, outside what a {} compares exactly: 0, or a
        # magnitude from {} to {}
        raise InformixError(_m(481, option, name, value, kind.upper(), low, high))


# Integer types by width; a change to a higher rank widens. INT8 and BIGINT share a rank:
# both are 64-bit, so a change between them is a different encoding, not a widening.
_INTEGER_RANKS = {
    "SMALLINT": 1,
    "INT2": 1,
    "INTEGER": 2,
    "INT": 2,
    "SERIAL": 2,
    "BIGINT": 3,
    "INT8": 3,
    "BIGSERIAL": 3,
    "SERIAL8": 3,
}


def _expression_kind(
    operand: str | tuple,
    columns: dict[str, "Column"],
    option: str,
    table_name: str,
    *,
    filtering: bool,
    options: dict[str, str] | None = None,
    outermost: bool = True,
) -> str:
    # Docstring: The kind ``operand`` evaluates to, failing on anything ill-typed.
    # ``outermost`` is for the expression a transform lands as (see the NVL bounds).
    #
    # A column of a type a filter cannot compare has kind "other", which only HASH and
    # NULLIFY accept; in a filter such a column fails outright. ``options``, the table's,
    # give a variable-scale DECIMAL the type NVL's fallback is checked against; without
    # them that type is unknown and not checked.

    if isinstance(operand, str):
        column = columns.get(operand)
        if column is None:
            # 98: Option '{}' uses column {}, which '{}' does not have or does not select
            raise InformixError(_m(98, option, operand, table_name))
        kind = _row_filter_kind(column)
        if kind is None and filtering:
            # 99: Option '{}' cannot filter on {} column {}
            raise InformixError(_m(99, option, column.type_name, operand))
        return kind or "other"
    _, name, arguments = operand
    inputs, literal_kinds, result, in_filters = _EXPRESSION_FUNCTIONS[name]
    if filtering and not in_filters:
        # 100: Option '{}' cannot use {}, which only transform_columns applies
        raise InformixError(_m(100, option, name))
    kind = _expression_kind(
        arguments[0],
        columns,
        option,
        table_name,
        filtering=filtering,
        options=options,
        outermost=False,
    )
    floating = kind in _FLOAT_KINDS and name in ("NVL", "COALESCE") and not filtering
    if kind not in inputs and not floating:
        # A column no function reads, such as a FLOAT (an INTEGER may widen to one), by type.
        if kind in ("other", *_FLOAT_KINDS):
            kind = columns[_expression_column(arguments[0])].type_name
        # 101: Option '{}' cannot apply {} to a {} value
        raise InformixError(_m(101, option, name, kind))
    literals = arguments[1:]
    required = sum(1 for expected in literal_kinds if not expected.endswith("?"))
    if not required <= len(literals) <= len(literal_kinds):
        # 102: Option '{}' gives {} {} argument(s); it takes {}{}
        raise InformixError(
            _m(
                102,
                option,
                name,
                len(literals) + 1,
                required + 1,
                f" to {len(literal_kinds) + 1}" if required < len(literal_kinds) else "",
            )
        )
    for expected, literal in zip(literal_kinds, literals):
        expected = expected.rstrip("?")
        if expected == "same":
            expected = {"nstring": "string", "float": "number", "smallfloat": "number"}.get(
                kind, kind
            )
        if literal[0] != expected:
            # 103: Option '{}' gives {} a {} where it takes a {}
            raise InformixError(_m(103, option, name, literal[0], expected))
        if floating:
            _float_literal(literal[1], kind, option, _expression_text(operand))
    if name in ("NVL", "COALESCE") and not filtering and kind == "number":
        # A transform's fallback lands in the column's own type (see _fallback_value), so
        # an integer column, or an integer-result function, takes a whole number only.
        column = columns[_expression_column(operand)]
        # Judged by the type the inner expression lands as, which the fallback lands as
        # too, not the column's: NVL(LENGTH(name), 5000000000) on a VARCHAR passed and
        # then failed a batch as an int, and NVL(ROUND(int_col, -3), 2147484000), which
        # lands as a BIGINT, was refused. A variable-scale DECIMAL lands as the table's
        # decimal options give it (decimal.variable.type: integer is a BIGINT), and ROUND
        # of one can free integer digits (to DECIMAL(38,0) from the default 38,18).
        inner = _transform_spark_type(arguments[0], column, options or {})
        integral = (
            isinstance(inner, (IntegerType, LongType))
            or column.type_name in _INTEGER_RANKS
            or any(
                function in _INTEGER_RESULT_FUNCTIONS
                for function in _operand_functions(arguments[0])
            )
        )
        fallback = literals[0][1]
        _decimal_literal(fallback, option, _expression_text(operand))
        if integral and fallback != fallback.to_integral_value():
            # 104: Option '{}' gives {} the fallback {} for {}, which holds whole numbers only
            raise InformixError(_m(104, option, name, fallback, column.name))
        # And one that type can hold. Left unchecked, the mistake showed only when a row
        # with a NULL arrived, as a failed batch. A variable-scale DECIMAL's type is the
        # decimal options' to say, so it is checked only when they are known.
        # An integer type holds one more below zero than above: an INT8's -2^63 was refused.
        if isinstance(inner, IntegerType):
            limit = Decimal((1 << 31) - 1)
        elif isinstance(inner, LongType):
            limit = Decimal((1 << 63) - 1)
        elif isinstance(inner, DecimalType) and (
            options is not None or not _is_variable_scale_decimal(column)
        ):
            # Past Python's 28 digits: at them DECIMAL(38,18)'s 10^20 - 10^-18 is 10^20.
            with localcontext() as decimal_context:
                decimal_context.prec = 80
                limit = Decimal(10) ** (inner.precision - inner.scale) - Decimal(1).scaleb(
                    -inner.scale
                )
        else:
            limit = None
        # copy_abs, not abs(): that rounds to the context's 28 digits as well, as a sum
        # with the limit would.
        if (
            limit is not None
            and fallback.copy_abs() > limit
            # Only as the value that lands: under ABS, -2^31 became 2^31, which an INT
            # column can't hold, and failed in Spark.
            and not (
                outermost
                and isinstance(inner, (IntegerType, LongType))
                and fallback == -limit - 1
            )
        ):
            # 450: Option '{}' gives {} the fallback {} for {}, which is more than a {}
            # column holds
            raise InformixError(
                _m(450, option, name, fallback, column.name, inner.simpleString().upper())
            )
    _check_function_literals(name, [literal[1] for literal in literals], option)
    return kind if result == "same" else result


def _row_filter_datetime_sql(value: datetime, column: Column) -> str:
    # Docstring: Render ``value`` as an Informix DATETIME literal in ``column``'s qualifier.
    #
    # Fails when ``value`` carries a non-zero field finer than the qualifier holds, which
    # could match in Python yet never in SQL.

    end = (column.length or 0) & 0xF
    digits = end - 10 if end > 10 else 0
    scale = 10 ** (6 - digits)
    kept = value.replace(
        hour=value.hour if end >= 6 else 0,
        minute=value.minute if end >= 8 else 0,
        second=value.second if end >= 10 else 0,
        microsecond=(value.microsecond // scale) * scale if digits else 0,
    )
    if kept != value:
        # 113: Option '{}' value TIMESTAMP '{}' is more precise than DATETIME column {}
        raise InformixError(_m(113, _ROW_FILTER_OPTION, value, column.name))
    text = f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
    fields = {6: f" {value.hour:02d}", 8: f":{value.minute:02d}", 10: f":{value.second:02d}"}
    text += "".join(part for code, part in fields.items() if end >= code)
    if digits:
        text += "." + f"{value.microsecond:06d}"[:digits]
    names = {4: "DAY", 6: "HOUR", 8: "MINUTE", 10: "SECOND"}
    qualifier = f"FRACTION({digits})" if digits else names[end]
    return f"DATETIME({text}) YEAR TO {qualifier}"


def _exact_literal(literal: tuple, name: str, option: str = _ROW_FILTER_OPTION) -> None:
    # Docstring: Refuse a filter number Informix reads otherwise than the stream's filter:
    # past 32 significant digits it reads only the first 32 (live-checked: 1.5 equals
    # 1.50000000000000000000000000000000001), where the stream compares the whole number.

    if literal[0] == "number":
        with localcontext() as context:
            context.prec = max(200, len(literal[1].as_tuple().digits))
            if len(literal[1].normalize().as_tuple().digits) > 32:
                # 484: Option '{}' compares {} with {}, which has more than the 32
                # significant digits Informix reads of a number; the snapshot would filter
                # otherwise than the stream
                raise InformixError(_m(484, option, name, literal[1]))
        _decimal_literal(literal[1], option, name)


def _operand_literals(operand: str | tuple):
    # Docstring: Every literal argument inside an operand's function calls.

    while not isinstance(operand, str):
        yield from operand[2][1:]
        operand = operand[2][0]


def _check_row_filter(tree: tuple, table: Table, option: str = _ROW_FILTER_OPTION) -> None:
    # Docstring: Fail unless every predicate in ``tree`` compares a well-typed operand over
    # selected, filterable columns of ``table`` with values of its kind; ``option`` is the
    # one the errors name (row_filter, or snapshot.filter, which takes its grammar).

    columns = {column.name: column for column in table.columns}

    def check(node: tuple) -> None:
        tag = node[0]
        if tag in ("and", "or"):
            for child in node[1]:
                check(child)
            return
        if tag == "not":
            check(node[1])
            return
        operand = node[1]
        name = _expression_text(operand)
        column = columns.get(_expression_column(operand))
        kind = _expression_kind(operand, columns, option, table.exposed_name, filtering=True)
        # Typed literals render in the qualifier of the column the operand reads.
        described = column.type_name if isinstance(operand, str) else kind
        if not isinstance(operand, str):
            for literal in _operand_literals(operand):
                if literal[0] == "timestamp":
                    _row_filter_datetime_sql(literal[1], column)
                _exact_literal(literal, name, option)
        if tag == "null":
            return
        if tag == "like":
            if kind not in ("string", "nstring"):
                # 114: Option '{}' can only apply LIKE to a string column, not {} {}
                raise InformixError(_m(114, option, described, name))
            return
        literals = node[3] if tag == "in" else (node[3],) if tag == "cmp" else node[3:5]
        expected = {"nstring": "string", "float": "number", "smallfloat": "number"}.get(kind, kind)
        for literal in literals:
            if literal[0] != expected:
                raise InformixError(
                    f"Option '{option}' compares {described} column {name} "
                    f"with a {literal[0]} value"
                    if isinstance(operand, str)
                    else f"Option '{option}' compares {name}, a {kind}, with a {literal[0]} value"
                )
            if kind == "timestamp":
                _row_filter_datetime_sql(literal[1], column)
            if kind in _FLOAT_KINDS:
                _float_literal(literal[1], kind, option, name)
            else:
                _exact_literal(literal, name, option)
        ordered = tag == "between" or (tag == "cmp" and node[2] not in ("=", "<>"))
        if ordered and kind in ("nstring", "bool"):
            raise InformixError(
                f"Option '{option}' can only test {described} column {name} for equality"
                if isinstance(operand, str)
                else f"Option '{option}' can only test {name} for equality"
            )

    check(tree)


def _expression_sql(operand: str | tuple, columns: dict[str, "Column"], literal) -> str:
    # Docstring: Render a filter operand as Informix SQL; ``literal`` renders its literals.

    if isinstance(operand, str):
        return _sql_name(columns[operand].name)
    _, name, arguments = operand
    base = columns[_expression_column(operand)]
    parts = [_expression_sql(arguments[0], columns, literal)]
    parts += [literal(value, base) for value in arguments[1:]]
    # COALESCE pads an LVARCHAR result to its declared width on Informix; with the one
    # fallback allowed here NVL is the same function without that, so it is sent instead.
    return f"{'NVL' if name == 'COALESCE' else name}({', '.join(parts)})"


def _row_filter_sql(table: Table, text: str | None = None) -> str | None:
    # Docstring: ``table``'s row_filter as an Informix SQL predicate, or None; or ``text``,
    # a filter checked against ``table`` in the same grammar (snapshot.filter's).

    text = table.row_filter if text is None else text
    if text is None:
        return None
    columns = {column.name: column for column in table.columns}

    def literal(value: tuple, column: Column) -> str:
        kind, item = value
        if kind == "number":
            # A FLOAT's in the exponent form the comparisons were checked in on Informix.
            return format(item, "e" if _row_filter_kind(column) in _FLOAT_KINDS else "f")
        if kind == "string":
            return "'" + item.replace("'", "''") + "'"
        if kind == "bool":
            return "'t'" if item else "'f'"
        if kind == "date":
            # MDY() builds the DATE without depending on the server's DBDATE format.
            return f"MDY({item.month}, {item.day}, {item.year})"
        return _row_filter_datetime_sql(item, column)

    def render(node: tuple) -> str:
        tag = node[0]
        if tag in ("and", "or"):
            return f" {tag.upper()} ".join(f"({render(child)})" for child in node[1])
        if tag == "not":
            return f"NOT ({render(node[1])})"
        column = columns[_expression_column(node[1])]
        name = _expression_sql(node[1], columns, literal)
        if tag == "null":
            return f"{name} IS {'NOT ' if node[2] else ''}NULL"
        if tag == "cmp":
            return f"{name} {node[2]} {literal(node[3], column)}"
        negation = "NOT " if node[2] else ""
        if tag == "in":
            values = ", ".join(literal(value, column) for value in node[3])
            return f"{name} {negation}IN ({values})"
        if tag == "like":
            escape = literal(("string", node[4]), column)
            return f"{name} {negation}LIKE {literal(node[3], column)} ESCAPE {escape}"
        low, high = literal(node[3], column), literal(node[4], column)
        return f"{name} {negation}BETWEEN {low} AND {high}"

    return render(_parse_row_filter(text))


def _snapshot_predicate(snapshot_filter: str | None, table: Table) -> str | None:
    # Docstring: The WHERE predicate a snapshot read applies: snapshot.filter, checked against
    # ``table`` as row_filter is, and row_filter.

    row_filter = _row_filter_sql(table)
    if snapshot_filter:
        _check_row_filter(_parse_row_filter(snapshot_filter), table, "snapshot.filter")
        snapshot_filter = _row_filter_sql(table, snapshot_filter)
    if snapshot_filter and row_filter:
        return f"({snapshot_filter}) AND ({row_filter})"
    return snapshot_filter or row_filter


_INCLUDE_COLUMNS_OPTION = "include_columns"
_EXCLUDE_COLUMNS_OPTION = "exclude_columns"


def _column_selection(options: dict[str, str]) -> tuple[str, tuple[str, ...]] | None:
    # Docstring: The table's include_columns / exclude_columns choice as (mode, sorted names).
    #
    # Named as in Lakeflow Connect's managed connectors. Either takes a comma-separated
    # list or a JSON array, as primary.keys does; setting both is ambiguous, so rejected.
    # include_columns keeps a column added to the source later out, exclude_columns lets
    # it in.

    include = options.get(_INCLUDE_COLUMNS_OPTION)
    exclude = options.get(_EXCLUDE_COLUMNS_OPTION)
    if include is None and exclude is None:
        return None
    if include is not None and exclude is not None:
        raise ValueError(
            f"Set '{_INCLUDE_COLUMNS_OPTION}' or '{_EXCLUDE_COLUMNS_OPTION}', not both"
        )
    mode, option, raw = (
        ("include", _INCLUDE_COLUMNS_OPTION, include)
        if include is not None
        else ("exclude", _EXCLUDE_COLUMNS_OPTION, exclude)
    )
    names = _parse_key_columns(raw, option)
    if not names:
        # 70: Option '{}' is set but lists no columns
        raise ValueError(_m(70, option))
    if len(names) != len(set(names)):
        # 72: Option '{}' lists a duplicate column
        raise ValueError(_m(72, option))
    return mode, tuple(sorted(names))


def _apply_column_selection(table: Table, selection: tuple[str, tuple[str, ...]] | None) -> Table:
    # Docstring: Narrow ``table`` to the selected columns; see ``Table.column_selection``.
    #
    # Every listed name must be a column of the table, so a typo fails instead of quietly
    # ingesting a column exclude_columns was meant to keep out. After a listed column is
    # dropped at the source, take it out of the list; the schema transition accepts that
    # edit. A nullable-index key candidate that loses a column is no candidate any more.

    if selection is None:
        return table
    mode, names = selection
    option = _INCLUDE_COLUMNS_OPTION if mode == "include" else _EXCLUDE_COLUMNS_OPTION
    catalog = table.catalog_columns or table.columns
    known = {column.name for column in catalog}
    missing = [name for name in names if name not in known]
    if missing:
        # 77: Option '{}' names column(s) that '{}' does not have: {}
        raise InformixError(_m(77, option, table.exposed_name, (", ".join(missing))))
    chosen = set(names)
    columns = tuple(column for column in catalog if (column.name in chosen) == (mode == "include"))
    if not columns:
        # 78: Option '{}' leaves '{}' with no columns
        raise ValueError(_m(78, option, table.exposed_name))
    keep_index = set(table.nullable_index_key) <= {column.name for column in columns}
    return replace(
        table,
        columns=columns,
        catalog_columns=catalog,
        column_selection=selection,
        nullable_index_key=table.nullable_index_key if keep_index else (),
        nullable_index_descending=table.nullable_index_descending if keep_index else (),
    )


def _unsupported_column_list(columns: Iterable[Column]) -> str:
    # Docstring: ``name (TYPE)`` for each column, noting a DATETIME or INTERVAL qualifier.

    return ", ".join(
        f"{column.name} ({column.type_name}"
        + (" with an unsupported qualifier" if column.type_name in {"DATETIME", "INTERVAL"} else "")
        + ")"
        for column in columns
    )


def _datetime_qualifier_supported(qualifier: int) -> bool:
    start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
    fields = {0, 2, 4, 6, 8, 10}
    return start in fields and end in {*fields, 11, 12, 13, 14, 15} and end >= start


def _unsupported_column(column: Column) -> bool:
    # Docstring: Whether ``column`` has a type whose ordinary SQLI row representation is not
    # implemented.

    unsupported = {
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    return (
        column.type_name in unsupported
        or (
            column.type_name == "DATETIME" and not _datetime_qualifier_supported(column.length or 0)
        )
        or (column.type_name == "INTERVAL" and interval_layout(column.length or 0) is None)
    )


def _snapshot_unsupported_columns(table: Table) -> tuple[Column, ...]:
    # Docstring: Return types whose ordinary SQLI row representation is not implemented.

    return tuple(column for column in table.columns if _unsupported_column(column))


def _leave_out_unsupported_columns(table: Table, key: tuple[str, ...] | None) -> Table:
    # Docstring: Leave the columns of a type the connector can't read out of ``table``.
    #
    # BYTE, TEXT, BLOB, CLOB, collection, ROW and opaque columns, and DATETIME or INTERVAL
    # columns whose qualifier the decoder doesn't handle, can be neither snapshotted nor
    # captured. Rather than rejecting the table, the connector reads the rest of it, as if
    # exclude_columns listed them, and warns once per process naming what it left out.
    # Runs after the column selection, so a column it already excludes draws no warning,
    # and include_columns naming one fails: that asks for a column the connector can't land.
    #
    # ``key`` is the key the table will merge and delete by (see _requested_key), or None
    # for a primary.keys "*" wildcard, which keys on whatever is ingested. A key column
    # can't be left out, so then the table is returned as it is and fails as it always
    # did, once _ensure_materializable finds the column.

    unsupported = _snapshot_unsupported_columns(table)
    if not unsupported:
        return table
    if key is not None and {column.name for column in unsupported} & set(key):
        return table
    if table.column_selection is not None and table.column_selection[0] == "include":
        # 79: Option '{}' names column(s) of '{}' whose type the connector can't read: {}
        raise InformixError(
            _m(
                79,
                _INCLUDE_COLUMNS_OPTION,
                table.exposed_name,
                _unsupported_column_list(unsupported),
            )
        )
    names = {column.name for column in unsupported}
    columns = tuple(column for column in table.columns if column.name not in names)
    if not columns:
        # 80: Table '{}' has no column the connector can read: {}
        raise InformixError(_m(80, table.exposed_name, _unsupported_column_list(unsupported)))
    warning = (table.identity, tuple(sorted(names)))
    coordinator = _STATE_VALIDATION_COORDINATOR
    with coordinator.condition:
        first = warning not in coordinator.unsupported_column_warnings
        coordinator.unsupported_column_warnings.add(warning)
    if first:
        # 81: Informix table '{}' has column(s) of a type the connector can't read: {}. They are
        # left out; the rest of the table is ingested without them.
        logging.getLogger(__name__).warning(
            _m(81, table.exposed_name, _unsupported_column_list(unsupported)),
        )
    keep_index = not set(table.nullable_index_key) & names
    return replace(
        table,
        columns=columns,
        catalog_columns=table.catalog_columns or table.columns,
        unsupported_columns=unsupported,
        nullable_index_key=table.nullable_index_key if keep_index else (),
        nullable_index_descending=table.nullable_index_descending if keep_index else (),
    )


def _ensure_selected_keys(table: Table) -> None:
    # Docstring: Fail when the column selection leaves out a key column.
    #
    # The pipeline merges and deletes by the key, so it must be ingested. Checked after the
    # key options, so a primary.keys override naming other columns can stand in for an
    # excluded catalog key.

    if table.column_selection is None:
        return
    selected = {column.name for column in table.columns}
    missing = [key for key in table.primary_keys if key not in selected]
    if missing:
        # 82: Key column(s) {} of '{}' must be selected; add them to '{}' or remove them from '{}'
        raise InformixError(
            _m(
                82,
                (", ".join(missing)),
                table.exposed_name,
                _INCLUDE_COLUMNS_OPTION,
                _EXCLUDE_COLUMNS_OPTION,
            )
        )


_RENAME_COLUMNS_OPTION = "rename_columns"
# rename_columns lands chosen columns under new names: a JSON object such as
# {"cust_nm": "customer_name"}, or "cust_nm:customer_name, ..." pairs. Every other
# option still names source columns. The renames apply together, so two columns may
# swap names, and a destination name may not clash (ignoring case, as Delta does) with
# another reported column or a connector metadata column.
_RENAME_FORBIDDEN = re.compile(r"[ ,;{}()\n\t=]")


def _column_renames(options: dict[str, str]) -> tuple[tuple[str, str], ...] | None:
    # Docstring: The table's rename_columns as sorted (source, destination) pairs, or None.

    raw = options.get(_RENAME_COLUMNS_OPTION)
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip()
    option = _RENAME_COLUMNS_OPTION
    if text.startswith("{"):
        try:
            pairs = json.loads(text, object_pairs_hook=list)
        except ValueError as error:
            # 83: Option '{}' is not a valid JSON object
            raise ValueError(_m(83, option)) from error
        if not all(isinstance(value, str) for pair in pairs for value in pair):
            # 84: Option '{}' must map column names to column names
            raise ValueError(_m(84, option))
    else:
        pairs = []
        for item in text.split(","):
            source, separator, target = item.partition(":")
            if not separator:
                # 85: Option '{}' takes a JSON object or 'source:destination' pairs
                raise ValueError(_m(85, option))
            pairs.append((source, target))
    pairs = [(source.strip(), target.strip()) for source, target in pairs]
    sources = [source for source, _ in pairs]
    if len(sources) != len(set(sources)):
        # 86: Option '{}' renames a column twice
        raise ValueError(_m(86, option))
    for source, target in pairs:
        if not source or not target:
            # 87: Option '{}' has an empty column name
            raise ValueError(_m(87, option))
        if _RENAME_FORBIDDEN.search(target):
            # 88: Option '{}' destination name {!r} contains a space or one of ,;{{}}()= that Delta
            # column names can't hold
            raise ValueError(_m(88, option, target))
    return tuple(sorted((source, target) for source, target in pairs if source != target)) or None


def _apply_column_renames(table: Table, renames: tuple[tuple[str, str], ...] | None) -> Table:
    # Docstring: Validate ``renames`` against ``table``'s selected columns and record them.

    if renames is None:
        return table if table.column_renames is None else replace(table, column_renames=None)
    selected = {column.name for column in table.columns}
    missing = [source for source, _ in renames if source not in selected]
    if missing:
        # 89: Option '{}' renames column(s) that '{}' does not have or does not select: {}
        raise InformixError(
            _m(89, _RENAME_COLUMNS_OPTION, table.exposed_name, (", ".join(missing)))
        )
    mapping = dict(renames)
    names = [mapping.get(column.name, column.name) for column in table.columns]
    names += [CURSOR, COMMIT_LSN, TX_ID, OP, COMMIT_TIME]
    folded = [name.casefold() for name in names]
    clashes = sorted({name for name, fold in zip(names, folded) if folded.count(fold) > 1})
    if clashes:
        # 90: Option '{}' gives '{}' more than one column named {}
        raise InformixError(
            _m(90, _RENAME_COLUMNS_OPTION, table.exposed_name, (", ".join(clashes)))
        )
    return table if table.column_renames == renames else replace(table, column_renames=renames)


_TRANSFORM_COLUMNS_OPTION = "transform_columns"
# transform_columns replaces chosen columns' values as rows leave the connector: an
# object from column name to an expression over that column, e.g. {"ssn": "MASK(ssn, 4)",
# "email": "HASH(LOWER(email))"}. Expressions use row_filter's grammar and functions plus
# MASK, HASH, and NULLIFY, and run in Python only, on the values the connector emits
# (CHAR without its padding, dates and timestamps as ISO text), so raw values never
# reach Spark. A key column takes only HASH(column), which keeps keys one-to-one so
# deletes still match. row_filter, the column selection, and the key options keep
# seeing source values; renames apply after.
_TRANSFORM_CACHE: dict[str, tuple] = {}


def _column_transforms(options: dict[str, str]) -> tuple[tuple[str, str, Any], ...] | None:
    # Docstring: The table's transform_columns as sorted (column, canonical text, operand).

    raw = options.get(_TRANSFORM_COLUMNS_OPTION)
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip()
    cached = _TRANSFORM_CACHE.get(text)
    if cached is not None:
        return cached
    option = _TRANSFORM_COLUMNS_OPTION
    try:
        pairs = json.loads(text, object_pairs_hook=list)
    except ValueError as error:
        # 83: Option '{}' is not a valid JSON object
        raise ValueError(_m(83, option)) from error
    if not isinstance(pairs, list) or not all(
        isinstance(pair, tuple) and all(isinstance(value, str) for value in pair) for pair in pairs
    ):
        # 91: Option '{}' must map column names to expressions
        raise ValueError(_m(91, option))
    columns = [column for column, _ in pairs]
    if len(columns) != len(set(columns)):
        # 92: Option '{}' transforms a column twice
        raise ValueError(_m(92, option))
    parsed = []
    for column, expression in pairs:
        operand = _RowFilterParser(expression, option, "transform").parse_expression()
        if isinstance(operand, str):
            # 93: Option '{}' for {} applies no function
            raise ValueError(_m(93, option, column))
        parsed.append((column, _expression_text(operand), operand))
    result = tuple(sorted(parsed, key=lambda item: item[0])) or None
    if len(_TRANSFORM_CACHE) >= 64:
        _TRANSFORM_CACHE.clear()
    _TRANSFORM_CACHE[text] = result
    return result


def _transform_key_id(key: str | None) -> str | None:
    # Docstring: A non-secret id of the HASH key, fingerprinted so a new key fails closed.
    #
    # An HMAC under the key itself, so it says nothing about the key without it.

    if not key:
        return None
    digest = hmac.new(key.encode("utf-8"), b"lakeflow-informix-transform-key-id", hashlib.sha256)
    return digest.hexdigest()[:16]


# The connection option holding transform_columns' HMAC key (see HASH).
_TRANSFORM_HASH_KEY_OPTION = "transform.hash.key"


def _apply_column_transforms(
    table: Table,
    transforms: tuple | None,
    key: str | None,
    *,
    key_id: str | None = None,
    options: dict[str, str] | None = None,
) -> Table:
    # Docstring: Validate ``transforms`` against ``table`` and record their canonical form.
    #
    # ``key`` is the transform.hash.key connection option, required when HASH is used;
    # a schema refresh, which carries a validated table forward, passes its ``key_id``.
    # ``options``, the table's options, give a variable-scale DECIMAL its type (see
    # _expression_kind); a schema refresh has none to pass.

    option = _TRANSFORM_COLUMNS_OPTION
    if transforms is None:
        if table.column_transforms is None:
            return table
        return replace(table, column_transforms=None, transform_key_id=None)
    columns = {column.name: column for column in table.columns}
    uses_hash = False
    for name, canonical, operand in transforms:
        if name not in columns:
            # 94: Option '{}' transforms column {}, which '{}' does not have or does not select
            raise InformixError(_m(94, option, name, table.exposed_name))
        if _expression_column(operand) != name:
            # 95: Option '{}' for {} reads {}; an expression may only read the column it replaces
            raise InformixError(_m(95, option, name, _expression_column(operand)))
        _expression_kind(
            operand, columns, option, table.exposed_name, filtering=False, options=options
        )
        uses_hash = uses_hash or "HASH" in _operand_functions(operand)
        # By the parsed expression, not its text: a name canonical text quotes ("date",
        # "a+b") made HASH("date") differ from HASH(date), and refused a hashed key.
        if name in table.primary_keys and operand != ("fn", "HASH", (name,)):
            # 96: Option '{}' may only HASH key column {}: any other change could map two keys to
            # one, and deletes would remove the wrong rows
            raise InformixError(_m(96, option, name))
    if uses_hash and key_id is None:
        if not key or len(key) < 16:
            # 97: Option '{}' uses HASH, which needs the connection option '{}' set to a secret of
            # at least 16 characters
            raise InformixError(_m(97, option, _TRANSFORM_HASH_KEY_OPTION))
        key_id = _transform_key_id(key)
    recorded = tuple((name, canonical) for name, canonical, _ in transforms)
    key_id = key_id if uses_hash else None
    if table.column_transforms == recorded and table.transform_key_id == key_id:
        return table
    return replace(table, column_transforms=recorded, transform_key_id=key_id)


_ASCII_UPPER = str.maketrans("abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _case_map(text: str, upper: bool) -> str:
    # Docstring: UPPER/LOWER as Informix applies them: to the ASCII letters only. Measured
    # on an ISO 8859-1 database, where accented letters (\u00e0, \u00c9) stay as they are.

    if upper:
        return text.translate(_ASCII_UPPER)
    return text.translate(_ASCII_LOWER)


def _temporal(value: Any) -> date:
    # Docstring: A date or datetime from a decoded value or its ISO text at the boundary.

    if isinstance(value, (date, datetime)):
        return value
    text = str(value)
    return datetime.fromisoformat(text) if len(text) > 10 else date.fromisoformat(text)


def _hash_number_text(value: int | float | Decimal) -> str:
    # Docstring: One text per numeric value, whichever Python type carries it: 5, 5.0, and
    # Decimal("5.00") all give "5". A key then keeps its digest when its column widens
    # (INTEGER to DECIMAL or FLOAT, which the stream adopts without a full refresh), and a
    # variable-scale DECIMAL hashes the same from a file ("1.5") and the change stream
    # ("1.50").

    if isinstance(value, float):
        if not math.isfinite(value):
            return repr(value)
        value = Decimal(repr(value))
    elif isinstance(value, int):
        return str(value)
    if not value.is_finite():
        return str(value)
    if not value:
        return "0"
    with localcontext() as context:
        context.prec = 80
        return format(value.normalize(), "f")


def _hash_text(value: Any) -> str:
    # Docstring: The text HASH digests: one form per value, whichever path decoded it.
    #
    # Rows reach the output boundary with dates and timestamps already in ISO text (see
    # _framework_value) and CHAR padding removed, identically from snapshots, files, and
    # CDC, so the text of those values is stable; the other cases fix one spelling each.

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        # As the value lands (see _framework_value): a column's year-1 value reaches here
        # as that text, zone and all, and one a transform made (the fallback of an NVL)
        # must hash the same.
        return _framework_value(value.replace(tzinfo=None))
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, (int, float, Decimal)):
        return _hash_number_text(value)
    return str(value)
