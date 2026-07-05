"""Shared helpers used by both ``odata.py`` and ``_contained.py``.

These functions live in a third module so the flat-path and contained-path
read code can share them without ``_contained`` having to import from
``odata`` (which would close a cycle — ``odata`` already mixes in
``ContainedNavMixin`` at class definition time).
"""

from datetime import datetime, timezone
from typing import Any


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
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
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
    values, preserving the pre-helper behavior for such data."""
    key_a, key_b = cursor_sort_key(a), cursor_sort_key(b)
    try:
        return key_a > key_b
    except TypeError:
        return a > b


def cursor_le(a: Any, b: Any) -> bool:
    """Whether ``a <= b`` in cursor order (see :func:`cursor_newer`)."""
    key_a, key_b = cursor_sort_key(a), cursor_sort_key(b)
    try:
        return key_a <= key_b
    except TypeError:
        return a <= b


def cursor_max(values: Any) -> Any:
    """Max of an iterable of non-``None`` cursor values in cursor order.
    Pairwise via :func:`cursor_newer` (not ``max(key=…)``) so a shape-mixed
    iterable degrades per-pair instead of raising, and the FIRST maximal
    value wins ties — matching ``max``'s tie behavior, so an equal-instant
    pair rendered two ways (``…Z`` vs ``…+00:00``) keeps the earlier-seen
    text as the watermark."""
    result = _SENTINEL = object()
    for value in values:
        if result is _SENTINEL or cursor_newer(value, result):
            result = value
    if result is _SENTINEL:
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


def max_or(a: Any, b: Any) -> Any:
    """Max of two values in CURSOR order (see :func:`cursor_newer`) where
    either may be ``None``. Returns the other when one is ``None``; ``None``
    only if both are. ``a`` wins ties (matching ``max``'s first-arg-wins)."""
    if a is None:
        return b
    if b is None:
        return a
    return b if cursor_newer(b, a) else a
