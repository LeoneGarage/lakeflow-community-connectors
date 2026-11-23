"""CDC transactions: buffering, spilling to the Volume, and the values a change's row falls back
to.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import random
import re
import shutil
import stat
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, localcontext
from typing import Any, Callable, Iterator

from pyspark.sql.types import DecimalType, IntegerType, LongType, StringType

from databricks.labs.community_connector.sources.informix.informix_filters import (
    _INTEGER_RANKS,
    _INTEGER_RESULT_FUNCTIONS,
    _ROW_FILTER_OPTION,
    _STRING_FUNCTION_NAMES,
    _TRANSFORM_HASH_KEY_OPTION,
    _case_map,
    _check_row_filter,
    _expression_column,
    _float32,
    _hash_text,
    _operand_functions,
    _parse_row_filter,
    _row_filter_kind,
    _row_filter_text,
    _spark_type,
    _temporal,
    _transform_spark_type,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _EXPRESSION_FUNCTIONS,
    _NULL_BYTE_OPTION,
    _coerce_variable_decimal_value,
    _framework_row,
    _framework_value,
    _is_variable_scale_decimal,
    _null_byte_mode,
    _variable_decimal_target,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CDC_SPILL_PAGE_RECORDS,
    _DATA_OPS,
    _LSN_DECIMAL_WIDTH,
    _PIPELINE_SCOPE,
    _UNLOAD_FLOAT_TYPES,
    COMMIT_LSN,
    COMMIT_TIME,
    CURSOR,
    OP,
    TX_ID,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _SHARED_STATE_MOUNT_RETRY_MAX_SECONDS,
    _SHARED_STATE_OPEN_ATTEMPTS,
    Column,
    SharedStateAccessUnavailable,
    Table,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import redirect_host_key


# pylint: disable-next=too-many-branches
def _apply_function(name: str, value: Any, literals: list, context: dict) -> Any:
    # Docstring: Evaluate one function on an already-evaluated operand ``value``.
    #
    # NULL in gives NULL out, except NVL/COALESCE, which replace it, and NULLIFY, which
    # always gives NULL. ``context`` holds the byte encoding LENGTH counts in and the
    # HMAC key HASH uses.

    if name == "NULLIFY":
        return None
    if name in ("NVL", "COALESCE"):
        return literals[0] if value is None else value
    if value is None:
        return None
    if name == "HASH":
        key = context.get("hash_key")
        if not key:
            # 111: HASH needs the connection option '{}'
            raise InformixError(_m(111, _TRANSFORM_HASH_KEY_OPTION))
        return hmac.new(key, _hash_text(value).encode("utf-8"), hashlib.sha256).hexdigest()
    if name in _STRING_FUNCTION_NAMES:
        text = str(value)
        if name in ("UPPER", "LOWER"):
            return _case_map(text, name == "UPPER")
        if name == "TRIM":
            return text.strip(" ")
        if name == "LTRIM":
            return text.lstrip(" ")
        if name == "RTRIM":
            return text.rstrip(" ")
        if name == "LENGTH":
            # Bytes, without trailing blanks, as Informix counts.
            return len(text.rstrip(" ").encode(context.get("encoding", "latin-1"), "replace"))
        if name == "CHAR_LENGTH":
            return len(text)
        if name == "SUBSTR":
            # A start past the end gives NULL, as on Informix; an in-range empty slice is ''.
            start = int(literals[0]) - 1
            if start >= len(text):
                return None
            return text[start:] if len(literals) == 1 else text[start : start + int(literals[1])]
        if name == "REPLACE":
            return text.replace(literals[0], literals[1] if len(literals) > 1 else "")
        if name in ("LPAD", "RPAD"):
            width, pad = int(literals[0]), literals[1]
            if len(text) >= width:
                return text[:width]
            fill = (pad * width)[: width - len(text)]
            return fill + text if name == "LPAD" else text + fill
        # MASK keeps the last ``keep`` characters; a value no longer than that is
        # masked whole, so a short value does not pass through unmasked.
        keep = int(literals[0]) if literals else 0
        character = literals[1] if len(literals) > 1 else "*"
        if len(text) <= keep:
            return character * len(text)
        return character * (len(text) - keep) + (text[len(text) - keep :] if keep else "")
    if name in ("YEAR", "MONTH", "DAY", "WEEKDAY"):
        moment = _temporal(value)
        if name == "WEEKDAY":
            return moment.isoweekday() % 7  # Informix: 0 is Sunday
        return getattr(moment, name.lower())
    with localcontext() as decimal_context:
        # Wide enough for a DECIMAL(p) with no scale, whose values run from 1E-130 to
        # 1E+126: rounding one to a whole number takes over a hundred digits.
        decimal_context.prec = 200
        # A float by the digits it shows (1.15, not its binary 1.1499...), as
        # _hash_number_text reads one, so a half rounds as the documentation says.
        number = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
        if name == "ABS":
            result = abs(number)
        elif name == "MOD":
            # Informix truncates both operands to integers; the sign follows the dividend.
            whole = number.to_integral_value(rounding=ROUND_DOWN)
            result = whole % Decimal(literals[0]).to_integral_value(rounding=ROUND_DOWN)
        else:
            digits = int(literals[0]) if literals else 0
            result = number.quantize(
                Decimal(1).scaleb(-digits),
                rounding=ROUND_HALF_UP if name == "ROUND" else ROUND_DOWN,
            )
            # Negative digits leave a positive exponent (1.2E+4): write it out in full.
            result = result.quantize(Decimal(1)) if digits < 0 else result
    # Back in the type the column's values arrive as, which the schema declares: a
    # variable-scale DECIMAL can arrive as a float or a fixed-point string.
    if isinstance(value, str):
        # Zero unsigned, as Informix and the decimal decoders write it ("-0" hashed apart).
        return format(result or abs(result), "f")
    if isinstance(value, float):
        return float(result)
    return result if isinstance(value, Decimal) else int(result)


def _pad_char(text: str, width: int, encoding: str) -> str:
    # Docstring: ``text`` blank-padded as Informix stores a CHAR: to ``width`` bytes of the
    # database code set, so a multibyte character takes more than one of them.

    missing = width - len(text.encode(encoding, "replace"))
    return text + " " * missing if missing > 0 else text


def _rounded_half_up(value: Decimal, places: int) -> Decimal:
    # Docstring: ``value`` at ``places``, a half away from zero as ROUND rounds one: under
    # the default half-even, NVL(ROUND(vcol, 0), 2.5) landed 2 where ROUND lands 3.

    with localcontext() as decimal_context:
        # As _apply_function's: a DECIMAL(p) fallback of 1e100 at 2 places is 103 digits.
        decimal_context.prec = 200
        return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def _number_text_places(operand: str | tuple) -> int | None:
    # Docstring: The places a number function's text values carry (see _apply_function):
    # ROUND's and TRUNC's digits, none for MOD, ABS those of what it reads; None for the
    # column's own text, which drops trailing zeros.

    while not isinstance(operand, str):
        _, name, arguments = operand
        if name in ("ROUND", "TRUNC"):
            return max(int(arguments[1][1]) if len(arguments) > 1 else 0, 0)
        if name == "MOD":
            return 0
        operand = arguments[0]
    return None


def _number_for_column(value: Decimal, column: "Column", options: dict[str, str]) -> Any:
    # Docstring: ``value`` as the Python number ``column``'s decoded values are.

    if column.type_name in _INTEGER_RANKS:
        return int(value)
    if column.type_name in _UNLOAD_FLOAT_TYPES:
        # As the column's own values: a SMALLFLOAT's 32 bits, so HASH reads its digits alike.
        smallfloat = column.type_name in {"SMALLFLOAT", "REAL"}
        return _float32(float(value)) if smallfloat else float(value)
    if column.type_name in {"DECIMAL", "NUMERIC", "MONEY"}:
        if _is_variable_scale_decimal(column):
            target = _variable_decimal_target(column, options)
            if target[0] == "decimal":
                # Rounded as below, not cut as the column's own values are.
                value = _rounded_half_up(value, target[2] or 0)
            return _coerce_variable_decimal_value(value, column, target)
        if column.scale is not None:
            scale = column.scale
        else:
            scale = 2 if column.type_name == "MONEY" else 0
        # Past the default 28 digits (a DECIMAL holds up to 32), and as ROUND rounds.
        return _rounded_half_up(value, scale)
    return value


def _fallback_value(literal: Any, operand: str | tuple, context: dict) -> Any:
    # Docstring: NVL/COALESCE's fallback in the representation of what it stands in for.
    #
    # In a filter a CHAR column's fallback is padded to the column's width, as Informix
    # pads it. In a transform a number literal, parsed as a Decimal, becomes the Python
    # type the operand's other values have -- an int for an integer column or an
    # integer-result function, a Decimal at the column's scale, or the configured
    # variable-scale form -- so every row of a transformed column carries one type.
    # ``context["columns"]`` supplies the columns, on demand when it is callable.

    if isinstance(literal, str):
        if isinstance(operand, str):
            width = context.get("pad", {}).get(operand)
            if width:
                return _pad_char(literal, width, context.get("encoding", "latin-1"))
        return literal
    if not isinstance(literal, Decimal):
        return literal
    if any(name in _INTEGER_RESULT_FUNCTIONS for name in _operand_functions(operand)):
        return int(literal)
    columns = context.get("columns")
    if callable(columns):
        columns = context["columns"] = {column.name: column for column in columns()}
    column = (columns or {}).get(_expression_column(operand))
    if column is None:
        return literal
    options = context.get("options") or {}
    if not isinstance(operand, str):
        # A function can land in another type than its column (see _transform_spark_type),
        # and the fallback lands as the function's values do: ROUND(vcol, 0) is a
        # DECIMAL(38,0) where vcol is a DECIMAL(38,18), and 10^20 fits the one, not the
        # other; ROUND(int_col, -3) can be a BIGINT. So is ROUND's carry digit: under
        # decimal(5,2), NVL(ROUND(vcol, 2), 1000) was checked against the DECIMAL(6,2)
        # and then failed the first NULL against the column's decimal(5,2).
        inner = _transform_spark_type(operand, column, options)
        if isinstance(inner, (IntegerType, LongType)):
            return int(literal)
        places = None
        if isinstance(inner, DecimalType) and inner != _spark_type(column, options):
            places = inner.scale
        elif isinstance(inner, StringType) and _is_variable_scale_decimal(column):
            # As a string, at the places the function's values have: ROUND(vcol, 2)
            # writes 12.30, so the fallback 12.3 must too, or HASH gave the two digests.
            # Only padded: a string holds 12.345 as given.
            places = _number_text_places(operand)
            if places is not None:
                places = max(places, -literal.as_tuple().exponent)
        if places is not None:
            result = _rounded_half_up(literal, places)
            if isinstance(inner, DecimalType):
                return result
            return format(result or abs(result), "f")
    return _number_for_column(literal, column, options)


def _expression_value(operand: str | tuple, row: dict, context: dict) -> Any:
    # Docstring: Evaluate an operand on ``row``. In a filter, ``context["pad"]`` names the
    # CHAR columns and their widths: Informix applies functions to the blank-padded
    # value, which decoding stripped, so a CHAR operand is padded back first.

    if isinstance(operand, str):
        value = row.get(operand)
        width = context.get("pad", {}).get(operand)
        if width and value is not None:
            value = _pad_char(str(value), width, context.get("encoding", "latin-1"))
        return value
    _, name, arguments = operand
    value = _expression_value(arguments[0], row, context)
    literals = [literal[1] for literal in arguments[1:]]
    if name in ("NVL", "COALESCE") and value is None:
        return _fallback_value(literals[0], arguments[0], context)
    return _apply_function(name, value, literals, context)


def _transformed_rows(rows: Iterator[dict], transforms: tuple | None, context: dict):
    # Docstring: ``rows`` with each transformed column's value replaced (source names).

    if not transforms:
        return rows

    def apply(row: dict) -> dict:
        out = dict(row)
        for name, _, operand in transforms:
            if name in out:
                value = _expression_value(operand, out, context)
                if (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and not -(1 << 63) <= value < (1 << 63)
                ):
                    # ABS of the lowest INT8, or a BIGINT rounded up past the highest:
                    # no 64-bit column holds it, and Informix refuses it too.
                    # 451: Transform of column {} gives {}, which is outside the range of
                    # a 64-bit integer
                    raise InformixError(_m(451, name, value))
                if isinstance(value, Decimal) and value.is_finite() and value.adjusted() >= 38:
                    # ROUND to a negative digit of a DECIMAL(38,0) (decimal.variable.type:
                    # decimal(38,0)) has no digit left for the carry: 38 nines to the ten
                    # is 10^38, which failed the batch in Spark.
                    # 466: Transform of column {} gives {}, which has more integer digits
                    # than a DECIMAL(38), Spark's largest, holds
                    raise InformixError(_m(466, name, value))
                out[name] = _framework_value(value)
        return out

    return (apply(row) for row in rows)


def _renamed_rows(rows: Iterator[dict], renames: tuple[tuple[str, str], ...] | None):
    # Docstring: ``rows`` with their keys under the destination names ``renames`` gives.

    if not renames:
        return rows
    mapping = dict(renames)
    return ({mapping.get(key, key): value for key, value in row.items()} for row in rows)


_ROW_FILTER_LIKE_CACHE: dict[tuple[str, str], re.Pattern] = {}


def _transform_may_null(operand: str | tuple) -> bool:
    # Docstring: Whether a transform can turn a non-NULL value into NULL: NULLIFY always does
    # and SUBSTR does past the end of the string. Every other function keeps a value, so a
    # NOT NULL column stays NOT NULL through HASH, MASK, UPPER, ABS, and the rest.

    return any(name in ("NULLIFY", "SUBSTR") for name in _operand_functions(operand))


def _apply_row_filter(table: Table, text: str | None) -> Table:
    # Docstring: Validate ``text`` against ``table`` and record its canonical form on the table.

    if text is None or not str(text).strip():
        return table if table.row_filter is None else replace(table, row_filter=None)
    if len(str(text)) > 8192:
        # 115: Option '{}' cannot exceed 8192 characters, as given or written out in full
        raise ValueError(_m(115, _ROW_FILTER_OPTION))
    tree = _parse_row_filter(str(text))
    _check_row_filter(tree, table)
    canonical = _row_filter_text(tree)
    if len(canonical) > 8192:
        # The canonical form is what a schema refresh checks again: longer, it passed here
        # and then failed the refresh.
        # 115: Option '{}' cannot exceed 8192 characters, as given or written out in full
        raise ValueError(_m(115, _ROW_FILTER_OPTION))
    return table if table.row_filter == canonical else replace(table, row_filter=canonical)


def _filter_column_names(node: tuple):
    # Docstring: The columns a row_filter tree reads.

    if node[0] in ("and", "or"):
        for child in node[1]:
            yield from _filter_column_names(child)
    elif node[0] == "not":
        yield from _filter_column_names(node[1])
    else:
        yield _expression_column(node[1])


def _operand_kind(operand: str | tuple, kinds: dict) -> str | None:
    # Docstring: The kind a validated operand evaluates to (see _expression_kind).

    if isinstance(operand, str):
        return kinds[operand]
    result = _EXPRESSION_FUNCTIONS[operand[1]][2]
    return _operand_kind(operand[2][0], kinds) if result == "same" else result


def _row_filter_like(pattern: str, escape: str) -> re.Pattern:
    # Docstring: Compile a LIKE pattern (trailing blanks already removed) to a regex.

    key = (pattern, escape)
    compiled = _ROW_FILTER_LIKE_CACHE.get(key)
    if compiled is None:
        parts = []
        index = 0
        while index < len(pattern):
            character = pattern[index]
            if character == escape:
                index += 1
                parts.append(re.escape(pattern[index]))
            elif character == "%":
                parts.append(".*")
            elif character == "_":
                parts.append(".")
            else:
                parts.append(re.escape(character))
            index += 1
        compiled = re.compile("".join(parts), re.DOTALL)
        if len(_ROW_FILTER_LIKE_CACHE) >= 256:
            _ROW_FILTER_LIKE_CACHE.clear()
        _ROW_FILTER_LIKE_CACHE[key] = compiled
    return compiled


def _row_filter_compare(value: Any, op: str, literal: Any, kind: str | None) -> bool:
    if kind in ("string", "nstring"):
        # Pad to equal width: SQL compares CHAR and VARCHAR as if blank-padded.
        value = str(value)
        width = max(len(value), len(literal))
        value, literal = value.ljust(width), literal.ljust(width)
    elif kind == "timestamp" and isinstance(value, datetime) and value.tzinfo is not None:
        # The decoder tags year-1 values UTC only so PySpark accepts them.
        value = value.replace(tzinfo=None)
    elif kind == "float":
        # As Informix compares: the literal read as a double, against the value's.
        literal = float(literal)
    elif kind == "smallfloat":
        # In single precision, as Informix compares a SMALLFLOAT: the literal through a
        # double to 32 bits, against the value's 32 bits (a .unl file gives 9 digits of it).
        value, literal = _float32(value), _float32(float(literal))
    if op == "=":
        return value == literal
    if op == "<>":
        return value != literal
    if op == "<":
        return value < literal
    if op == "<=":
        return value <= literal
    if op == ">":
        return value > literal
    return value >= literal


def _row_filter_value(node: tuple, row: dict, kinds: dict, context: dict) -> bool | None:
    tag = node[0]
    if tag in ("and", "or"):
        # AND stops at the first False, OR at the first True; a NULL otherwise wins.
        stop = tag == "or"
        result: bool | None = not stop
        for child in node[1]:
            value = _row_filter_value(child, row, kinds, context)
            if value is stop:
                return stop
            if value is None:
                result = None
        return result
    if tag == "not":
        value = _row_filter_value(node[1], row, kinds, context)
        return None if value is None else not value
    operand = node[1]
    if isinstance(operand, str):
        value, kind = row.get(operand), kinds[operand]
    else:
        value = _expression_value(operand, row, context)
        kind = _operand_kind(operand, kinds)
    if tag == "null":
        return (value is None) != node[2]
    if value is None:
        return None
    if tag == "cmp":
        return _row_filter_compare(value, node[2], node[3][1], kind)
    if tag == "in":
        hit = any(_row_filter_compare(value, "=", literal[1], kind) for literal in node[3])
        return hit != node[2]
    if tag == "like":
        hit = _row_filter_like(node[3][1], node[4]).fullmatch(str(value).rstrip(" ")) is not None
        return hit != node[2]
    inside = _row_filter_compare(value, ">=", node[3][1], kind) and _row_filter_compare(
        value, "<=", node[4][1], kind
    )
    return inside != node[2]


def _row_filter_matches(predicate: tuple, row: dict) -> bool:
    # Docstring: Whether ``row`` -- decoded Python values by column -- passes the filter.
    # Unknown (NULL) counts as no match, exactly as in a SQL WHERE clause.

    tree, kinds, context = predicate
    return _row_filter_value(tree, row, kinds, context) is True


def _filtered_pages(consume: Callable, predicate: tuple, max_rows: int = 0) -> Callable:
    # Docstring: ``consume`` given only the rows of each page that pass ``predicate``, its
    # pages numbered on without a gap where none does, and no more than ``max_rows`` of them
    # (0, any number): counted before the filter, a snapshot whose rows passed it read as
    # too many, unlike the snapshot-only read's (see _read_snapshot_only).

    kept = [0, 0]

    def consumer(lsn: int, _index: int, rows: list) -> None:
        rows = [row for row in rows if _row_filter_matches(predicate, row)]
        kept[1] += len(rows)
        if max_rows and kept[1] > max_rows:
            # 52: Initial snapshot exceeds snapshot.max.rows={}
            raise InformixError(_m(52, max_rows))
        if rows:
            consume(lsn, kept[0], rows)
            kept[0] += 1

    return consumer


def _row_filter_predicate(table: Table, encoding: str = "latin-1") -> tuple | None:
    # Docstring: ``table``'s parsed row_filter with what evaluating it needs -- each
    # column's kind and, for functions, the CHAR widths to re-pad and the byte encoding
    # LENGTH counts in -- ready for :func:`_row_filter_matches`, or None without a filter.

    if table.row_filter is None:
        return None
    kinds = {column.name: _row_filter_kind(column) for column in table.columns}
    pad = {
        column.name: int(column.length)
        for column in table.columns
        if column.type_name in {"CHAR", "NCHAR"} and column.length
    }
    return _parse_row_filter(table.row_filter), kinds, {"pad": pad, "encoding": encoding}


def _validated_volume_location(location: str, option_name: str, options: dict[str, str]) -> str:
    if not os.path.isabs(location):
        # 116: Option '{}' must be an absolute path
        raise ValueError(_m(116, option_name))
    normalized = os.path.normpath(location)
    if normalized != location.rstrip("/") or any(
        part in {".", ".."} for part in location.split("/")
    ):
        # 117: Option '{}' must not contain traversal
        raise ValueError(_m(117, option_name))
    if options.get("hostname"):
        parts = normalized.split("/")
        if len(parts) < 5 or parts[1] != "Volumes" or any(not part for part in parts[2:5]):
            # 118: Option '{}' must be a Unity Catalog Volume path under
            # /Volumes/<catalog>/<schema>/<volume>
            raise ValueError(_m(118, option_name))
    return normalized


def _newest_created_entry_mtime(path: str, recursive: bool = False) -> float:
    # Docstring: Return the newest storage-assigned timestamp without trusting directory updates.
    #
    # Considers only the creation timestamps of the *files* found beneath ``path``.
    # Every artifact this inspects is created with O_EXCL and never rewritten, so a
    # file's mtime is its creation time.
    #
    # Directory mtimes are excluded because their meaning is filesystem-specific:
    # a local POSIX filesystem advances them when entries are *removed*, while a
    # Volume settles them to a creation-like whole second but briefly surfaces a
    # transient post-mutation value first. Either way, cleaning an artifact's
    # contents could make the artifact look newly created to a caller sampling at
    # the wrong moment, keeping a stale lease alive. The scanned directory's own
    # timestamp is used only when it contains no files at all, where it bounds the
    # age rather than extending it.

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = os.open(path, flags)
    newest: float | None = None
    stack = [root]
    try:
        # Inside: a mount that dropped between the open and the stat left the root open.
        root_mtime = os.fstat(root).st_mtime
        while stack:
            descriptor = stack.pop()
            try:
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        metadata = entry.stat(follow_symlinks=False)
                        if not stat.S_ISDIR(metadata.st_mode):
                            created_at = metadata.st_mtime
                            newest = created_at if newest is None else max(newest, created_at)
                        elif recursive:
                            stack.append(os.open(entry.name, flags, dir_fd=descriptor))
            finally:
                os.close(descriptor)
        return root_mtime if newest is None else newest
    except BaseException:
        for descriptor in stack:
            os.close(descriptor)
        raise


def _created_since(path: str, cutoff: float) -> bool:
    # Docstring: Whether anything beneath ``path`` was created at or after ``cutoff``, by the
    # rule of _newest_created_entry_mtime (files only; a tree with none goes by its own
    # directory), stopping at the first such file. Directories are told apart by the listing
    # where it carries the entry type, and only files are statted, so an active run of
    # thousands of pages costs two listings and one stat, not a stat of every page over the
    # mount.

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = os.open(path, flags)
    stack = [root]
    files = False
    try:
        root_mtime = os.fstat(root).st_mtime
        while stack:
            descriptor = stack.pop()
            try:
                directories = []
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            directories.append(entry.name)
                        elif entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                            return True
                        else:
                            files = True
                for name in directories:
                    stack.append(os.open(name, flags, dir_fd=descriptor))
            finally:
                os.close(descriptor)
        return not files and root_mtime >= cutoff
    finally:
        for descriptor in stack:
            os.close(descriptor)


def _unchanged_directories(cutoff: float, *paths: str) -> list[str]:
    # Docstring: Those of ``paths`` that no entry was added to or taken from since ``cutoff``.
    #
    # The sweeps took the runs, scans and stages out and left the directories that held
    # them, one scope directory for every update that ever staged, each listed over the
    # mount again by every later sweep. A sweep removes those it empties that were
    # unchanged before it began on them: one that changed within the retention window may
    # be where a reader of that update is staging.

    unchanged = []
    for path in paths:
        with contextlib.suppress(OSError):
            if os.stat(path, follow_symlinks=False).st_mtime < cutoff:
                unchanged.append(path)
    return unchanged


def _staging_subdirectories(path: str) -> list[str]:
    # Docstring: Immediate subdirectory paths of ``path``; empty if it is absent or unreadable.
    #
    # Best-effort helper for the retention sweep: a missing path (nothing staged) is not
    # an error, and a scan failure must never fail a read -- the sweep is a background
    # safety net.

    try:
        with os.scandir(path) as entries:
            return [entry.path for entry in entries if entry.is_dir(follow_symlinks=False)]
    except FileNotFoundError:
        return []
    except OSError:
        # 119: Cannot scan Informix snapshot staging path: {}
        logging.getLogger(__name__).warning(_m(119, path), exc_info=True)
        return []


def _scandir(descriptor: int) -> os.ScandirIterator:
    # Docstring: os.scandir of an open directory, which it closes if that fails (a dropped
    # mount): it is on no stack yet, which would close it.

    try:
        return os.scandir(descriptor)
    except BaseException:
        os.close(descriptor)
        raise


def _remove_candidate_tree_at(parent_descriptor: int, name: str) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = os.open(name, flags, dir_fd=parent_descriptor)
    stack: list[tuple[int, os.ScandirIterator, str | None]] = [(root, _scandir(root), None)]
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
                stack.append((child, _scandir(child), entry.name))
            else:
                os.unlink(entry.name, dir_fd=descriptor)
    finally:
        while stack:
            descriptor, entries, _ = stack.pop()
            entries.close()
            os.close(descriptor)


def _path_present(path: str) -> bool:
    # Docstring: Whether something may still be at ``path``: anything but its absence counts.
    #
    # Not os.path.lexists, which reads a dropped mount as nothing there: a stage was then
    # recorded as cleaned with its pages left.

    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def _is_path(path: str, *, directory: bool = False) -> bool:
    # Docstring: Whether ``path`` is a regular file (a directory with ``directory``).
    #
    # Not os.path.isfile or isdir, which read every OSError as False: a dropped Volume mount
    # (ENOTCONN) passed for a missing file, and a read the mount retry would have carried
    # failed for good instead (a snapshot.source with "no .unl files").

    try:
        mode = os.stat(path).st_mode
    except (FileNotFoundError, NotADirectoryError):
        return False
    return stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)


def _remove_tree(path: str) -> None:
    # Docstring: Remove a directory tree, a missing one counting as removed. Any other
    # failure raises: rmtree's ignore_errors kept the records of a scan under a dropped
    # mount, for a later read to take up as its own.

    with contextlib.suppress(FileNotFoundError):
        shutil.rmtree(path)


def _dropped_mount_error(error: BaseException) -> OSError | None:
    # Docstring: Return the ENOTCONN in ``error``'s cause chain, if a dropped mount caused it.
    #
    # Snapshot page staging is the only remaining Volume user, but it still touches
    # the mount from several calls -- open, makedirs, rename, listdir -- and only
    # ``os.open`` goes through the retry helper. When the mount drops, whichever
    # call runs first raises ENOTCONN, so no single site can be the place this is
    # handled; production saw it surface from two different sites in consecutive
    # updates.
    #
    # Classifying at the read boundary instead covers every site at once. The
    # caller converts this into the same empty-batch checkpoint retry it already
    # uses for a transiently inaccessible Volume, which is exactly what a remount
    # needs: the mount returns within seconds and the next read proceeds.

    seen: set[int] = set()
    cursor: BaseException | None = error
    while cursor is not None and id(cursor) not in seen:
        seen.add(id(cursor))
        if isinstance(cursor, OSError) and cursor.errno == errno.ENOTCONN:
            return cursor
        cursor = cursor.__cause__ or cursor.__context__
    return None


def _open_state_entry_with_retry(path: str, flags: int, *, dir_fd: int | None = None) -> int:
    # Docstring: Open one state entry, retrying transient Volume permission and mount failures.

    last_error: OSError | None = None
    for attempt in range(_SHARED_STATE_OPEN_ATTEMPTS):
        try:
            return os.open(path, flags, dir_fd=dir_fd)
        except OSError as error:
            if error.errno not in {errno.EPERM, errno.EACCES, errno.ENOTCONN}:
                raise
            if error.errno == errno.ENOTCONN:
                # The Volume's FUSE mount dropped: the filesystem itself is
                # briefly gone rather than this entry being denied. Observed in
                # production taking down 23 flows at once, every one failing on
                # the shared-state root, while the mount was healthy again
                # moments later. A remount settles on a scale of seconds, not the
                # milliseconds a permission blip needs, so this backs off harder
                # (and skips the symlink check below, which exists only to
                # disambiguate EPERM and cannot stat an absent filesystem).
                last_error = error
                if attempt + 1 < _SHARED_STATE_OPEN_ATTEMPTS:
                    delay = min(0.25 * (2**attempt), _SHARED_STATE_MOUNT_RETRY_MAX_SECONDS)
                    time.sleep(random.uniform(delay / 2, delay))
                continue
            # Some filesystems report EPERM rather than ELOOP for O_NOFOLLOW.
            # Preserve the security distinction between a transiently denied
            # Volume entry and a replaceable symlink.
            try:
                entry = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
            except OSError:
                entry = None
            if entry is not None and stat.S_ISLNK(entry.st_mode):
                # 120: Informix shared-state path traverses symlink: '{}'
                raise InformixError(_m(120, path)) from error
            last_error = error
            if attempt + 1 < _SHARED_STATE_OPEN_ATTEMPTS:
                delay = min(0.01 * (2**attempt), 0.25)
                time.sleep(random.uniform(delay / 2, delay))
    # 121: Informix shared state is temporarily inaccessible: '{}'
    raise SharedStateAccessUnavailable(_m(121, path)) from last_error


def _open_state_directory(root: str, path: str) -> int:
    # Docstring: Open a directory beneath root without following a replaceable symlink.
    #
    # Both are normalised first: commonpath keeps "..", so a path that climbed out of the
    # root passed and was walked out of it a part at a time, and a root given with a
    # trailing slash refused every path beneath it.

    root, path = os.path.normpath(root), os.path.normpath(path)
    if os.path.commonpath((root, path)) != root:
        # 122: Informix shared-state path escapes '{}': '{}'
        raise InformixError(_m(122, root, path))
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)

    def entry(name: str, **kwargs: Any) -> int:
        try:
            return _open_state_entry_with_retry(name, flags, **kwargs)
        except OSError as error:
            if error.errno in {errno.ELOOP, errno.ENOTDIR}:
                # 123: Informix shared-state path traverses symlink or non-directory: '{}'
                raise InformixError(_m(123, path)) from error
            raise

    descriptor = entry(root)
    try:
        relative = os.path.relpath(path, root)
        for part in () if relative == "." else relative.split(os.sep):
            child = entry(part, dir_fd=descriptor)
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
            return _open_state_entry_with_retry(
                os.path.basename(path),
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory,
            )
        except OSError as error:
            if error.errno == errno.ELOOP:
                # 124: Informix shared-state path traverses symlink '{}'
                raise InformixError(_m(124, path)) from error
            raise
    finally:
        os.close(directory)


def _redirect_allowlist(value: str) -> frozenset[tuple[str, int]]:
    # Docstring: The allow-listed redirect destinations as redirect_host_key compares them.
    #
    # A host name matches whatever case the Connection Manager names it in. An IPv6 address
    # goes in brackets: unbracketed, fd00::1:9088 could be an address with no port.
    result: set[tuple[str, int]] = set()
    for item in filter(None, (part.strip() for part in value.split(","))):
        host, separator, port_text = item.rpartition(":")
        if (
            not separator
            or not host
            or not port_text.isdigit()
            or not 1 <= int(port_text) <= 65535
            or not _redirect_host_form(host)
        ):
            # 125: redirect.allowlist entries must be exact host:numeric-port pairs, an IPv6
            # address in brackets ([fd00::1]:9088)
            raise ValueError(_m(125))
        result.add((redirect_host_key(host), int(port_text)))
    return frozenset(result)


def _redirect_host_form(host: str) -> bool:
    # Docstring: Whether an allow-list entry's host is written as one: a name or IPv4
    # address bare, an IPv6 address in brackets and only that.
    if not (host.startswith("[") and host.endswith("]")):
        return ":" not in host
    try:
        return ipaddress.ip_address(host[1:-1]).version == 6
    except ValueError:
        return False


def _operation(record: dict[str, Any]) -> str:
    return str(record.get("op") or record.get("operation") or record.get("type") or "").upper()


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not (
        isinstance(value, int) or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value))
    ):
        # 335: {} must be an integer, not {!r}
        raise ValueError(_m(335, name, value))
    return int(value)


def _strict_lsn(value: object, name: str) -> int:
    result = _strict_int(value, name)
    if not 0 <= result < 1 << 64:
        # 336: {} is outside the unsigned 64-bit LSN domain
        raise ValueError(_m(336, name))
    return result


def _lsn(record: dict[str, Any]) -> int:
    value = record.get("lsn", record.get("sequence", record.get("sequence_id")))
    if value is None:
        raise InformixError(f"CDC record has no LSN: {record!r}")
    try:
        return _strict_lsn(value, "CDC record LSN")
    except ValueError as error:
        # 343: CDC record has an invalid LSN: {!r}
        raise InformixError(_m(343, record)) from error


@dataclass(eq=False, repr=False)
class Transaction:
    tx_id: int
    begin_lsn: int
    records: list[dict[str, Any]] = field(default_factory=list)
    pending_before: dict[str, Any] | None = None
    # One SPILLED record per table whose changes were written to staged pages (_CdcSpill).
    spilled: list[dict[str, Any]] = field(default_factory=list)
    # One SPILLING record per table while it is open: its changes went to pages, so they no
    # longer show which tables it touches.
    spilling: list[dict[str, Any]] = field(default_factory=list)
    last_lsn: int = field(init=False)

    def __post_init__(self) -> None:
        self.last_lsn = self.begin_lsn

    def advance(self, record: dict[str, Any]) -> int:
        lsn = _lsn(record)
        if lsn < self.last_lsn:
            # 134: CDC LSN regressed in transaction {}: {} < {}
            raise InformixError(_m(134, self.tx_id, lsn, self.last_lsn))
        self.last_lsn = lsn
        return lsn

    def append(self, record: dict[str, Any]) -> None:
        self.advance(record)
        op = _operation(record)
        if op == "BEFORE_UPDATE":
            if self.pending_before is not None:
                # 135: Unpaired BEFORE_UPDATE in transaction {}
                raise InformixError(_m(135, self.tx_id))
            self.pending_before = record
            return
        if op == "AFTER_UPDATE":
            if self.pending_before is None:
                # 136: AFTER_UPDATE without BEFORE_UPDATE in transaction {}
                raise InformixError(_m(136, self.tx_id))
            merged = dict(record)
            merged["op"] = "UPDATE"
            merged["before"] = self.pending_before.get("before", self.pending_before.get("row"))
            merged["after"] = record.get("after", record.get("row"))
            # The merged record is sequenced at the after-image. Keep the before-image's
            # own, strictly earlier, log position too: the append path's old-key
            # tombstone for a key-changing UPDATE is sequenced there, so it never ties
            # with the new key's after-image on the cursor.
            merged["before_lsn"] = _lsn(self.pending_before)
            # The columns string.null.byte changed in either image (see decode_frame): the
            # after-image's alone left a before-image's changed index value unseen.
            nul = [*self.pending_before.get("nul", ()), *record.get("nul", ())]
            if nul:
                merged["nul"] = nul
            self.pending_before = None
            self.records.append(merged)
            return
        self.records.append(record)

    def discard(self, lsn: int) -> None:
        # DISCARD carries the rollback cutoff, not the forward position of the
        # DISCARD control record. It may legitimately precede the latest data LSN.
        self.records = [r for r in self.records if _lsn(r) < lsn]
        # A SPILLING record is at its table's earliest change left (see _CdcSpill.start), so
        # one at or past the savepoint holds the table open no longer, as no record would.
        self.spilling = [r for r in self.spilling if _lsn(r) < lsn]
        if self.pending_before is not None and _lsn(self.pending_before) >= lsn:
            self.pending_before = None


@dataclass(frozen=True, eq=False, repr=False)
class CommittedTransaction:
    tx_id: int
    begin_lsn: int
    commit_lsn: int
    restart_lsn: int
    records: tuple[dict[str, Any], ...]
    # UTC epoch seconds from the COMMIT record (kind 2); None if the record carried none.
    # Stamped onto every row of the transaction as ``_informix_commit_time``.
    commit_time: int | None = None
    # The SPILLED records naming the staged pages that hold the rest of its changes.
    spilled: tuple[dict[str, Any], ...] = ()


def _strict_tx_id(value: object, name: str = "tx_id") -> int:
    result = _strict_int(value, name)
    if not -(1 << 31) <= result < 1 << 32:
        # 337: {} is outside the native 32-bit transaction-ID domain
        raise ValueError(_m(337, name))
    return result + (1 << 32) if result < 0 else result


def _tx_id(record: dict[str, Any]) -> int:
    value = record.get("tx_id", record.get("transaction_id"))
    if value is None:
        # 346: CDC record has no transaction ID: {!r}
        raise InformixError(_m(346, record))
    try:
        return _strict_tx_id(value, "CDC transaction ID")
    except ValueError as error:
        # 347: CDC record has an invalid transaction ID: {!r}
        raise InformixError(_m(347, record)) from error


def _normalise_record(raw: dict[str, Any]) -> dict[str, Any]:
    record = dict(raw)
    record["op"] = _operation(record)
    if record["op"] not in {"METADATA", "ERROR"}:
        record["lsn"] = _lsn(record)
    if record["op"] not in {"TIMEOUT", "METADATA", "ERROR"}:
        record["tx_id"] = _tx_id(record)
    return record


class TransactionBuffer:
    # Docstring: Buffering for interleaved Informix transactions.

    def __init__(self) -> None:
        self.open: dict[int, Transaction] = {}

    def feed(self, raw: dict[str, Any]) -> CommittedTransaction | None:
        record = _normalise_record(raw)
        op = _operation(record)
        if op in {"TIMEOUT", "METADATA"}:
            return None
        if op == "ERROR":
            detail = record.get("message") or record.get("payload") or ""
            # 137: Informix CDC error {} with flags {}: {}
            raise InformixError(
                _m(137, (record.get("error", "unknown")), (record.get("flags", "unknown")), detail)
            )
        tx_id = _tx_id(record)
        if op == "BEGIN":
            if tx_id in self.open:
                # 138: Duplicate CDC BEGIN for transaction {}
                raise InformixError(_m(138, tx_id))
            self.open[tx_id] = Transaction(tx_id, _lsn(record))
            return None
        tx = self.open.get(tx_id)
        if tx is None:
            # 139: CDC {} for unknown transaction {}
            raise InformixError(_m(139, op, tx_id))
        if op in _DATA_OPS:
            tx.append(record)
            return None
        if op == "SPILLED":
            tx.spilled.append(record)
            return None
        if op == "SPILLING":
            tx.spilling.append(record)
            return None
        if op == "DISCARD":
            tx.discard(_lsn(record))
            return None
        if op == "ROLLBACK":
            tx.advance(record)
            del self.open[tx_id]
            return None
        if op != "COMMIT":
            # 140: Unknown Informix CDC operation {!r}
            raise InformixError(_m(140, op))
        if tx.pending_before is not None:
            # 141: Transaction {} committed with unpaired BEFORE_UPDATE
            raise InformixError(_m(141, tx_id))
        end = tx.advance(record)
        del self.open[tx_id]
        restart = min((item.begin_lsn for item in self.open.values()), default=end)
        commit_time = record.get("timestamp")
        return CommittedTransaction(
            tx_id, tx.begin_lsn, end, restart, tuple(tx.records), commit_time, tuple(tx.spilled)
        )


def _spill_scope(scope: Any) -> str | None:
    # Docstring: The scope spilled pages are named under, or None without a valid scope.
    #
    # The pipeline's, not the update's: a later update reading a transaction again finds the
    # pages and the Lakebase record an earlier one left, so a cancelled update leaves no copy
    # behind. A scope outside a pipeline (random per registration) stays as it is.

    if not isinstance(scope, str) or _PIPELINE_SCOPE.fullmatch(scope) is None:
        return None
    pipeline, separator, _update = scope.partition("_@_")
    return pipeline.replace("-", "") if separator else scope


def _spill_run(layout: str, begin: int, tx_id: int, record: dict[str, Any]) -> str:
    # Docstring: A run's name: its table's capture layout, its transaction's BEGIN LSN and ID,
    # and a digest of the table's first change in it. A reinitialized log reuses positions, so
    # the digest keeps a later transaction from taking an earlier one's pages or record.

    first = json.dumps(
        [record.get("lsn"), record.get("op"), record.get("row")], sort_keys=True, default=str
    )
    return f"{layout}-{begin}-{tx_id}-{hashlib.sha256(first.encode()).hexdigest()[:12]}"


def _record_matches(record: dict[str, Any], table: Table) -> bool:
    identity = str(record.get("table") or record.get("identity") or "")
    return not identity or identity in {
        table.name,
        table.exposed_name,
        table.identity,
        table.native_identity,
    }


def _refuse_altered_changes(records, keys, table: "Table", options: dict[str, str]) -> None:
    # Docstring: Refuse ``table``'s changes whose ``keys`` values string.null.byte changed
    # (decode_frame's "nul"). An index-ordered snapshot places a change by the value read,
    # not the one Informix sorts: settled as staged, or under a later page, an insert was
    # dropped, and a delete was sent for a row no page had. The pages refuse such values too
    # (_unaltered). A shared shard's transaction holds other tables' changes as well.

    altered = sorted(
        {
            name
            for r in records
            if _record_matches(r, table)
            for name in r.get("nul", ())
            if name in keys
        }
    )
    if altered:
        mode = _null_byte_mode(options)
        # 486: Informix table '{}' holds a NUL byte in key or index column {}, which
        # string.null.byte={} changes, so the read can't page on or look up the value
        # Informix holds; set snapshot.scan=cursor to read the table without paging on it
        raise InformixError(_m(486, table.exposed_name, ", ".join(altered), mode))


def _spill_bytes(record: dict[str, Any]) -> int:
    # Docstring: A change's weight on its page: wide text or byte columns end a page early,
    # well inside the staged-page bound.

    return 64 + sum(
        len(value) if isinstance(value, (str, bytes, bytearray)) else 16
        for value in (record.get("row") or {}).values()
    )


def _spill_run_consumed(record: dict[str, Any]) -> bool:
    # Docstring: Whether every channel a run's record requires has committed past it.

    required, done = record.get("required"), record.get("done") or {}
    return isinstance(required, list) and bool(required) and all(done.get(c) for c in required)


class _CdcSpill:
    # Docstring: One poll's writer for open transactions that outgrow cdc.spill.records.
    #
    # Once an open transaction holds more changes than ``threshold``, those changes and
    # every later one go to staged pages of up to _CDC_SPILL_PAGE_RECORDS, a run per table,
    # instead of the poll's records. A page never ends between an UPDATE's before- and
    # after-image, so each page shapes on its own. At COMMIT the transaction gets one
    # SPILLED record per table naming its run, which the transaction buffer keeps in place
    # of the changes; a ROLLBACK deletes the runs. A DISCARD (rollback to a savepoint) drops
    # the changes it covers still in memory and records the LSN range for the pages, which
    # readers skip. A table's run is named by its capture layout, the BEGIN LSN, the
    # transaction and its first change (see _spill_run), so a poll that reads the
    # transaction again, the other channel's, or a later update's, names the same pages.

    def __init__(self, threshold, write, delete, layouts, scope, natives, commit=None) -> None:
        self.threshold = threshold
        self._write = write  # (native identity, table, run, page index, records)
        self._delete = delete  # (native identity, run)
        self._commit = commit  # (native identity, run, commit LSN), or None
        self._layouts = layouts  # logical identity -> capture layout hash
        self._scope = scope
        self._natives = natives  # logical identity -> native identity
        self._open: dict[int, tuple[int, dict[str, dict[str, Any]]]] = {}

    def holds(self, tx_id: int) -> bool:
        return tx_id in self._open

    def start(self, tx_id: int, begin_lsn: int, records: list[dict[str, Any]]) -> dict[str, int]:
        # Docstring: Start spilling a transaction with the changes it holds; return the tables
        # with a change left, each at the earliest.

        self._open[tx_id] = (begin_lsn, {})
        # In order, a DISCARD where it came: a run's pages are its table's changes paged
        # alike in every poll and channel that spills the transaction, wherever each began
        # to, as the other channel's pages are kept, not written again. Rolled-back
        # changes left out before paging gave one run two layouts, and a channel read the
        # other's pages without the rollback (a run left with no change is deleted).
        for record in records:
            if record["op"] == "DISCARD":
                self.discard(tx_id, int(record["lsn"]))
            else:
                self.add(record)
        return {t: p["live"] for t, p in self._open[tx_id][1].items() if p["live"] is not None}

    def add(self, record: dict[str, Any]) -> bool:
        # Docstring: Add one change; return whether it is its table's earliest left, so its
        # run's first or the first after rollbacks took back the rest.

        tx_id = int(record.get("tx_id") or 0)
        begin, parts = self._open[tx_id]
        table = str(record.get("table") or "")
        part = parts.get(table)
        if part is None:
            run = _spill_run(self._layouts.get(table, ""), begin, tx_id, record)
            part = parts[table] = {
                "run": run,
                "pages": 0,
                "rows": [],
                "bytes": 0,
                # The earliest change no rollback has taken back, or None.
                "live": None,
                "last": 0,
                "discarded": [],
                "ops": set(),
            }
        # A capture label is the table's place in one shard's capture list: the other
        # channel's shard can list it elsewhere, and both write the same pages.
        part["rows"].append(
            {key: value for key, value in record.items() if key not in ("label", "capture_label")}
        )
        part["ops"].add(record["op"])
        part["last"] = int(record["lsn"])
        began = part["live"] is None
        if began:
            part["live"] = part["last"]
        part["bytes"] += _spill_bytes(record)
        if record["op"] != "BEFORE_UPDATE" and (
            len(part["rows"]) >= _CDC_SPILL_PAGE_RECORDS or part["bytes"] >= 32 << 20
        ):
            self._flush(table, part)
        return began

    def _flush(self, table: str, part: dict[str, Any]) -> None:
        native = self._natives.get(table, table)
        self._write(native, table, part["run"], part["pages"], part["rows"])
        part["pages"] += 1
        part["rows"] = []
        part["bytes"] = 0

    def discard(self, tx_id: int, lsn: int) -> None:
        for part in self._open[tx_id][1].values():
            part["rows"] = [record for record in part["rows"] if int(record["lsn"]) < lsn]
            # Of what is left: the taken back ended a page early, and named operations, a
            # DELETE's, that a channel then read the pages for.
            part["bytes"] = sum(map(_spill_bytes, part["rows"]))
            if not part["pages"]:
                part["ops"] = {record["op"] for record in part["rows"]}
            if part["pages"] and part["last"] >= lsn:
                part["discarded"].append([lsn, part["last"]])
            # A rollback takes back everything from its savepoint on, and changes come in
            # LSN order: one at or before the earliest left leaves none.
            if part["live"] is not None and part["live"] >= lsn:
                part["live"] = None

    def drop(self, tx_id: int) -> None:
        _begin, parts = self._open.pop(tx_id)
        for table, part in parts.items():
            if part["pages"]:
                self._delete(self._natives.get(table, table), part["run"])

    def finish(self, tx_id: int, commit_lsn: int | None = None) -> list[dict[str, Any]]:
        # Docstring: The SPILLED records of a transaction at its COMMIT (``commit_lsn``), which
        # each run's record also learns.

        _begin, parts = self._open.pop(tx_id)
        spilled = []
        for table, part in parts.items():
            native = self._natives.get(table, table)
            if part["live"] is None:
                # Rollbacks to savepoints took back every change of the run, in one or in
                # several: named, a table with none failed an index-ordered scan's stream.
                if part["pages"]:
                    self._delete(native, part["run"])
                continue
            if part["rows"]:
                self._flush(table, part)
            if part["pages"]:
                if self._commit is not None and commit_lsn is not None:
                    self._commit(native, part["run"], commit_lsn)
                spilled.append(
                    {
                        "op": "SPILLED",
                        "tx_id": tx_id,
                        "lsn": part["last"],
                        "table": table,
                        "native_identity": native,
                        "pipeline_scope": self._scope,
                        "run": part["run"],
                        "pages": part["pages"],
                        "discarded": part["discarded"],
                        "ops": sorted(part["ops"]),
                    }
                )
        return spilled


def _before_lsn(record: dict[str, Any]) -> int:
    # Docstring: The log position of an assembled UPDATE's before-image.
    #
    # Set when the transaction assembler pairs BEFORE_UPDATE with AFTER_UPDATE; the
    # merged record's own ``lsn`` is the after-image's position.

    value = record.get("before_lsn")
    if value is None:
        # 344: CDC update has no before-image LSN: {!r}
        raise InformixError(_m(344, record))
    try:
        return _strict_lsn(value, "CDC before-image LSN")
    except ValueError as error:
        # 345: CDC update has an invalid before-image LSN: {!r}
        raise InformixError(_m(345, record)) from error


def _key(row: dict[str, Any] | None, table: Table) -> tuple[Any, ...]:
    if row is None:
        # 349: Missing before/after image for {}
        raise InformixError(_m(349, table.exposed_name))
    return tuple(row.get(pk) for pk in table.primary_keys)


def _refuse_null_key(row: dict[str, Any], table: Table) -> None:
    # Docstring: Fail on a NULL in a NOT NULL key column of a keyed table's snapshot row or
    # upsert, as the delete channel does (_shape_delete).
    #
    # Under string.null.byte=null a key string holding a NUL byte reads as NULL: "a\x00"
    # and "b\x00" both landed as the key NULL, one target row, and only a later delete
    # of one of them failed.

    nullable: set[str] | None = None
    for pk in table.primary_keys:
        if pk in row and row[pk] is None:
            if nullable is None:
                nullable = {column.name for column in table.columns if column.nullable}
            if pk not in nullable:
                # 463: A row of '{}' has NULL in NOT NULL key column {}, which would merge
                # distinct keys into one: '{}=null' turned a string holding a NUL byte into
                # NULL, or the row image is incomplete
                raise InformixError(_m(463, table.exposed_name, pk, _NULL_BYTE_OPTION))


def _commit_time_value(epoch_seconds: int | None) -> datetime | None:
    # Docstring: A UTC ``datetime`` for the ``_informix_commit_time`` column, or None.
    #
    # The source supplies commit time as epoch seconds (Informix CDC BEGIN/COMMIT records
    # and ``DBINFO('utc_current')``); TimestampType wants a ``datetime``. None stays None so
    # a row with no source time (e.g. a file-source extract without a manifest boundary_time)
    # lands as NULL.

    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc)


def _sortable_lsn(value: int) -> str:
    lsn = int(value)
    if not 0 <= lsn < 1 << 64:
        # 364: Informix LSN {} is outside the unsigned 64-bit decimal domain
        raise InformixError(_m(364, lsn))
    return f"{lsn:0{_LSN_DECIMAL_WIDTH}d}"


def _shape_change(row, record, tx, op, *, lsn: int | None = None):
    # Docstring: Shape one full-row change. ``lsn`` overrides the record's own cursor position.

    if row is None:
        # 350: CDC upsert has no after image
        raise InformixError(_m(350))
    result = _framework_row(row)
    result.update(
        {
            CURSOR: _sortable_lsn(_lsn(record) if lsn is None else lsn),
            COMMIT_LSN: _sortable_lsn(tx.commit_lsn),
            TX_ID: tx.tx_id,
            OP: op,
            COMMIT_TIME: _commit_time_value(tx.commit_time),
        }
    )
    return result


def _shape_tombstone(row, table, record, tx, *, lsn: int | None = None):
    # Docstring: A full before-image as an append-path ``d`` row.
    #
    # Used for a DELETE and for the old key of a key-changing UPDATE; the latter passes
    # ``lsn``, the before-image's own position. Not :func:`_shape_delete`, which nulls
    # every non-key column and needs a primary key: an append destination does not
    # merge, so the tombstone carries the whole removed row and is told apart from a
    # live row only by ``_informix_op``.

    if row is None:
        # 351: CDC delete on append-only table {} has no before image; full-row logging is required
        # to emit a delete tombstone
        raise InformixError(_m(351, table.exposed_name))
    return _shape_change(row, record, tx, "d", lsn=lsn)
