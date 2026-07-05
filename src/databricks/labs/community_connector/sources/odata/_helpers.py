"""Shared helpers used by ``odata.py``, ``_contained.py``, and
``_partition.py``.

These functions live in a separate module so the flat-path, contained-path,
and partition read code can share them without ``_contained``/``_partition``
having to import from ``odata`` (which would close a cycle — ``odata``
already mixes both mixins in at class definition time).
"""

import json
import re
from datetime import datetime, timezone
from typing import Any

_ISO_FRACTION_RE = re.compile(r"\.(\d+)")


def _fraction_digits(value: Any) -> str:
    """The fractional-second digit run of an ISO-rendered string, or ``""``.
    Used by :func:`cursor_newer`'s tie-break — see there for why the digits
    are compared zero-padded rather than as raw text."""
    if isinstance(value, str):
        match = _ISO_FRACTION_RE.search(value)
        if match:
            return match.group(1)
    return ""


def parse_iso8601(value: str) -> datetime:
    """``datetime.fromisoformat`` with version-uniform fraction handling.

    The connector's floor is Python 3.10 (DBR 13.3 LTS), where
    ``fromisoformat`` accepts fractional seconds of EXACTLY 3 or 6 digits —
    while real servers render value-dependent digit counts (Olingo/SAP trim
    trailing zeros → ``.5``; nanosecond servers emit 7+ digits, which even
    3.10 rejects). Left unnormalized, a ``…00.5Z`` watermark on a 3.10
    runtime fails the ISO sniff (→ ``odata_literal`` QUOTES it → wire 400
    every batch) and falls out of the chronological comparisons (→ back to
    the lexical silent-loss ordering those comparisons exist to prevent).
    Normalizing the fraction to exactly 6 digits (pad short, truncate
    long — sub-microsecond precision only affects ordering ties, which are
    duplicate-safe) makes parsing identical on every supported version.
    Also maps the ``Z`` designator 3.10 can't parse. Raises ``ValueError``
    like ``fromisoformat`` for non-ISO input."""
    s = value.replace("Z", "+00:00")
    frac = _ISO_FRACTION_RE.search(s)
    if frac:
        digits = frac.group(1)
        s = s[: frac.start(1)] + digits[:6].ljust(6, "0") + s[frac.end(1) :]
    return datetime.fromisoformat(s)


def cursor_sort_key(value: Any) -> Any:
    """Chronological sort key for one cursor value.

    The server orders cursor columns chronologically, but the client-side
    comparisons (re-filters, watermark maxes, probe dirty checks) receive the
    server's *rendered text* — and OData's JSON format makes fractional
    seconds optional per value (real stacks — Olingo, SAP — trim trailing
    zeros), so one column legitimately renders both ``…T23:00:00Z`` and
    ``…T23:00:00.5Z``. Python string order puts the LATER ``.5Z`` instant
    first (``.`` < ``Z``), which silently drops server-returned rows at the
    ``<= since`` re-filters and regresses watermark maxes. ISO-8601-looking
    strings therefore parse to a datetime for ordering (naive values are
    pinned to UTC so mixed naive/aware renderings still totally order);
    anything else orders as itself. Offsets and ``$filter`` literals keep the
    server's raw text — this key is for COMPARISON only."""
    if isinstance(value, str):
        try:
            dt = parse_iso8601(value)
        except (ValueError, TypeError):
            return value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return value


def cursor_newer(a: Any, b: Any) -> bool:
    """Whether ``a`` is strictly newer (greater) than ``b`` in cursor order —
    chronological where both render as ISO-8601 (see
    :func:`cursor_sort_key`), raw ordering otherwise. A shape-mixed column
    (one value parses, the other doesn't) falls back to comparing the raw
    values, preserving the pre-helper behavior for such data.

    Key TIES between different raw texts break on the FRACTION DIGITS,
    zero-padded to a common width: Python datetimes hold microseconds, so
    :func:`parse_iso8601` truncates sub-microsecond digits — two
    chronologically DIFFERENT 100ns-precision cursors (SQL Server
    ``datetime2(7)`` sources emit 7-digit fractions) would otherwise tie,
    and a tie at the ``<= since`` re-filter drops a strictly-newer row the
    server correctly returned. Padding before comparing matters: raw-text
    comparison inverts when digit counts differ (the shorter fraction's
    terminator ``Z``/``+`` sorts above any digit), whereas equal-width
    digit strings compare numerically. Genuinely equal instants rendered
    two ways (``…Z`` vs ``…+00:00``, trailing zeros) fall through to an
    arbitrary-but-consistent raw order that errs only in the
    duplicate-safe direction."""
    key_a, key_b = cursor_sort_key(a), cursor_sort_key(b)
    try:
        if key_a > key_b:
            return True
        if key_b > key_a:
            return False
    except TypeError:
        return a > b
    if a == b:
        return False
    # Exact-key tie with different texts: sub-microsecond digits (or two
    # renderings of one instant). Compare fractions numerically first.
    frac_a, frac_b = _fraction_digits(a), _fraction_digits(b)
    width = max(len(frac_a), len(frac_b))
    frac_a, frac_b = frac_a.ljust(width, "0"), frac_b.ljust(width, "0")
    if frac_a != frac_b:
        return frac_a > frac_b
    try:
        return a > b  # same instant — consistent arbitrary order
    except TypeError:
        return False


def cursor_le(a: Any, b: Any) -> bool:
    """Whether ``a <= b`` in cursor order — the exact complement of
    :func:`cursor_newer` under its strict total order (including the
    raw-text tie-break), so the re-filter and the watermark max can never
    disagree about a boundary row."""
    return not cursor_newer(a, b)


def cursor_max(values: Any) -> Any:
    """Max of an iterable of non-``None`` cursor values in cursor order.
    Pairwise via :func:`cursor_newer` (not ``max(key=…)``) so a shape-mixed
    iterable degrades per-pair instead of raising. With the raw-text
    tie-break in :func:`cursor_newer`, only IDENTICAL texts (or
    incomparable pairs) still tie — for those the first-seen value wins,
    matching ``max``'s tie behavior."""
    result = sentinel = object()
    for value in values:
        if result is sentinel or cursor_newer(value, result):
            result = value
    if result is sentinel:
        raise ValueError("cursor_max() arg is an empty sequence")
    return result


def trim_to_distinct_cursor_boundary(
    records: list[dict],
    cursor_field: str,
) -> list[dict]:
    """Drop trailing records sharing the boundary cursor value.

    Walks back from the tail until the cursor value changes, leaving a
    clean boundary that the next call's ``cursor gt <last>`` filter
    will pick up cleanly. Drops the boundary record itself — we can't
    tell whether the next page (or a concurrent insert before the next
    call) holds more records sharing that cursor value, so we
    surrender the whole group and let ``cursor gt <prev_distinct>``
    re-fetch them.

    Reads the **real** cursor column, never a ``cursor_nulls=coalesce``
    synthetic. That's deliberate: a same-cursor cohort is re-readable
    via ``cursor gt`` next call, but null-cursor rows are excluded by
    ``gt`` server-side, so they must not be trimmed — a batch of only
    null cursors trims to empty (every real value is the same ``None``)
    and the caller keeps the rows as-is.

    Returns an empty list when every record shares one cursor value;
    the caller decides whether that's recoverable (natural exhaustion)
    or a hard failure (truncated batch with too-small cap).
    """
    if not records:
        return records
    boundary = records[-1].get(cursor_field)
    trim_idx = len(records)
    while trim_idx > 0 and records[trim_idx - 1].get(cursor_field) == boundary:
        trim_idx -= 1
    return records[:trim_idx]


def jsonify_complex_values(row: dict) -> dict:
    """Render structured (dict/list) property values in an emitted row as
    JSON text.

    The connector's schema maps complex-typed / ``Collection(...)`` /
    enum / untyped CSDL properties to ``StringType``, and the framework's
    string parser stringifies via ``str()`` — which for a dict/list
    produces a Python **repr** (``{'City': 'X'}``, single quotes) that
    downstream ``from_json`` can't parse. Every structured value still
    present in a row at the emit boundary is by construction destined for
    a string column (the connector never declares struct/array columns,
    and nav-collection structures are consumed by the expand flattener
    before emit), so serializing them here is schema-independent and
    lossless. Rows with only scalar values pass through untouched."""
    if any(isinstance(v, (dict, list)) for v in row.values()):
        return {
            k: json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else v
            for k, v in row.items()
        }
    return row


def parse_max_records(table_options: dict | None) -> int:
    """Parse the ``max_records_per_batch`` table option (default 10000) with
    curated validation. The cap counts EMITTED rows per batch, so ``0`` or a
    negative value would make every walk park (or livelock) without emitting
    a single row — reject it up front instead of silently reading nothing."""
    raw = (table_options or {}).get("max_records_per_batch", "10000")
    try:
        value = int(str(raw).strip())
    except (ValueError, TypeError):
        raise ValueError(
            f"Invalid max_records_per_batch={raw!r}: expected a positive integer."
        ) from None
    if value < 1:
        raise ValueError(
            f"Invalid max_records_per_batch={raw!r}: must be >= 1 — the cap "
            f"bounds rows emitted per batch, and a non-positive cap would "
            f"emit nothing forever."
        )
    return value


def max_or(a: Any, b: Any) -> Any:
    """Max of two values in CURSOR order (see :func:`cursor_newer`) where
    either may be ``None``. Returns the other when one is ``None``; ``None``
    only if both are. ``a`` wins ties (matching ``max``'s first-arg-wins)."""
    if a is None:
        return b
    if b is None:
        return a
    return b if cursor_newer(b, a) else a
