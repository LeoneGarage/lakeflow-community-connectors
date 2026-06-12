"""Shared helpers used by both ``odata.py`` and ``_contained.py``.

These functions live in a third module so the flat-path and contained-path
read code can share them without ``_contained`` having to import from
``odata`` (which would close a cycle — ``odata`` already mixes in
``ContainedNavMixin`` at class definition time).
"""

from typing import Any


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
    """Max of two values where either may be ``None``. Returns the other
    when one is ``None``; ``None`` only if both are."""
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)
