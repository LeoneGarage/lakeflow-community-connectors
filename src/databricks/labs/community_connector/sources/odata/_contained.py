"""Contained-navigation-property support for the OData v4 connector.

OData v4 ``<NavigationProperty ContainsTarget="true">`` declares a
parent-owned collection accessed as ``GET Parent(<key>)/ContainedNavProp``.
The connector exposes these as double-underscore-pathed tables
(``Parent__Child__...__Leaf``) — slash isn't valid in Spark SQL
identifiers — with ``<seg>_<pk>`` ancestor-FK
columns prepended onto each row. The split keeps the main connector
file under its line cap; the methods here are mixed into
``ODataLakeflowConnect`` via ``ContainedNavMixin``.

All ``ContainedNavMixin`` methods call back into the main connector
class through ``self`` (URL building, HTTP fetch, metadata resolution),
so the mixin requires no abstract-method declarations — it duck-types
against the concrete class.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator
from xml.etree import ElementTree as ET

from pyspark.sql.types import StringType, StructField


# Path-segment separator. ``__`` (double underscore), not ``/``, so
# the framework can interpolate slash-free table names directly into
# Spark SQL identifiers (view names, temp views). The OData URL path
# still uses ``/`` — that's hardcoded in ``_build_contained_path``.
CONTAINED_PATH_SEP = "__"
# Inside generated OData request URLs the segment separator is always
# a forward slash (the wire format the spec mandates).
_URL_SEGMENT_SEP = "/"
# Cap on path depth. Prevents pathological discovery walks on services
# with cyclic containment graphs; cycles within the cap are also
# detected via target-type tracking.
MAX_CONTAINED_DEPTH = 5


def join_url(base: str, suffix: str) -> str:
    """Append ``suffix`` to ``base`` with at most one slash."""
    return f"{base}{suffix}" if base.endswith("/") else f"{base}/{suffix}"


def looks_like_iso8601(s: str) -> bool:
    """Cheap ISO-8601 sniff used by ``odata_literal`` to render bare timestamps."""
    if len(s) < 10 or s[4] != "-" or s[7] != "-":
        return False
    try:
        datetime.fromisoformat(s.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def odata_literal(value: Any) -> str:
    """Render a Python value as an OData v4 literal for $filter."""
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return str(value)
    s = str(value)
    if looks_like_iso8601(s):
        return s
    return "'" + s.replace("'", "''") + "'"


# Re-export of the EDM namespace prefix used by the main module.
_NS_EDM = "{http://docs.oasis-open.org/odata/ns/edm}"


def parse_contained_path(table_name: str) -> list[str] | None:
    """Split double-underscore-delimited path; ``None`` for flat names."""
    if _URL_SEGMENT_SEP in table_name:
        # OData entity-set names cannot contain ``/`` (CSDL allows only
        # letters/digits/underscores), so a slash here always means the
        # caller used the wrong separator. Spell out the fix — the
        # generic "not found" error otherwise buries the cause under a
        # 200-entry "Available:" list.
        suggested = table_name.replace(_URL_SEGMENT_SEP, CONTAINED_PATH_SEP)
        raise ValueError(
            f"Contained-collection table names use {CONTAINED_PATH_SEP!r} "
            f"(double underscore) as the segment separator, not "
            f"{_URL_SEGMENT_SEP!r} — slashes aren't valid in Spark SQL "
            f"identifiers, which the SDP framework uses for view names. "
            f"Rename {table_name!r} to {suggested!r} in the pipeline "
            f"config."
        )
    if CONTAINED_PATH_SEP not in table_name:
        return None
    segments = table_name.split(CONTAINED_PATH_SEP)
    if any(not s for s in segments):
        raise ValueError(
            f"Empty path segment in contained table name {table_name!r}; "
            "expected 'Parent__Child' or 'Parent__Child__Grandchild' form."
        )
    if len(segments) > MAX_CONTAINED_DEPTH:
        raise ValueError(
            f"Contained path {table_name!r} exceeds max depth "
            f"{MAX_CONTAINED_DEPTH} (got {len(segments)})."
        )
    return segments


def contained_nav_props(entity_type: ET.Element) -> list[tuple[str, str]]:
    """``[(nav_name, target_type_ref), ...]`` for ContainsTarget collection
    nav props declared directly on this type; singletons skipped."""
    out: list[tuple[str, str]] = []
    for np in entity_type.findall(f"{_NS_EDM}NavigationProperty"):
        if (np.get("ContainsTarget") or "").lower() != "true":
            continue
        type_ref = np.get("Type", "")
        if not (type_ref.startswith("Collection(") and type_ref.endswith(")")):
            continue
        out.append((np.get("Name"), type_ref[len("Collection(") : -1]))
    return out


def fk_column_name(segment: str, pk_name: str) -> str:
    """Default ancestor-FK column name: ``<segment>_<pkname>``.

    The actual column the connector writes may be further disambiguated
    (prefixed with leading underscores) if it collides with a leaf
    property or another FK column. See ``ContainedNavMixin._resolve_fk_columns``.
    """
    return f"{segment}_{pk_name}"


class ContainedNavMixin:
    """Mixin providing contained-collection support for the OData connector.

    Plug in via ``class ODataLakeflowConnect(LakeflowConnect,
    SupportsNamespaces, ContainedNavMixin):``. All methods duck-type
    against the concrete class for HTTP/URL/metadata helpers.
    """

    def _all_contained_nav_props(self, entity_type: ET.Element) -> list[tuple[str, str]]:
        """Contained nav props on the type and its base chain (closest-
        descendant wins on name collision)."""
        out: dict[str, str] = {}
        for type_el in self._resolve_base_chain(entity_type):
            for name, ref in contained_nav_props(type_el):
                out.setdefault(name, ref)
        return list(out.items())

    def _enumerate_contained_paths(self, top_level_set: str, namespace: str | None) -> list[str]:
        """BFS contained nav-property graph; cap at MAX_CONTAINED_DEPTH;
        break cycles via target-type set."""
        try:
            root_et = self._flat_entity_type_for(top_level_set, namespace)
        except ValueError:
            return []
        paths: list[str] = []
        root = self._metadata_root()
        type_to_qname: dict[ET.Element, str] = {
            et: f"{schema.get('Namespace') or ''}.{et.get('Name')}"
            for schema in root.iter(f"{_NS_EDM}Schema")
            for et in schema.findall(f"{_NS_EDM}EntityType")
        }
        queue: list[tuple[list[str], ET.Element, set[str]]] = [
            ([top_level_set], root_et, {type_to_qname.get(root_et, "")})
        ]
        while queue:
            segments, et, seen = queue.pop(0)
            if len(segments) >= MAX_CONTAINED_DEPTH:
                continue
            for nav_name, target_ref in self._all_contained_nav_props(et):
                if target_ref in seen:
                    continue
                target_et = self._resolve_type_ref(target_ref)
                if target_et is None:
                    continue
                new_segments = segments + [nav_name]
                paths.append(CONTAINED_PATH_SEP.join(new_segments))
                queue.append((new_segments, target_et, seen | {target_ref}))
        return paths

    # --- option parsing ----------------------------------------------------

    def _expand_contained_active(self, table_options: dict[str, str] | None) -> bool:
        """Parse the boolean ``expand_contained`` table option."""
        raw = ((table_options or {}).get("expand_contained") or "false").strip().lower()
        if raw not in {"true", "false"}:
            raise ValueError(f"Invalid expand_contained={raw!r}. Expected one of: true, false.")
        return raw == "true"

    # --- URL construction --------------------------------------------------

    def _format_key_predicate(self, pk_values: dict[str, Any]) -> str:
        """``(value)`` for single key; ``(K1=v1,K2=v2)`` for composite."""
        if len(pk_values) == 1:
            return f"({odata_literal(next(iter(pk_values.values())))})"
        return "(" + ",".join(f"{k}={odata_literal(v)}" for k, v in pk_values.items()) + ")"

    def _build_contained_path(self, segments: list[str], key_chain: list[dict[str, Any]]) -> str:
        """``A(1)/B('x')/C`` — leaf segment has no key; ``key_chain`` len = N-1."""
        if len(key_chain) != len(segments) - 1:
            raise ValueError(
                f"key_chain length {len(key_chain)} does not match "
                f"non-leaf segment count {len(segments) - 1}"
            )
        return _URL_SEGMENT_SEP.join(
            f"{seg}{self._format_key_predicate(key_chain[i])}" if i < len(key_chain) else seg
            for i, seg in enumerate(segments)
        )

    def _build_contained_url(
        self,
        segments: list[str],
        key_chain: list[dict[str, Any]],
        table_options: dict[str, str],
        extra_filter: str | None = None,
        order_by: str | None = None,
    ) -> str:
        """Full URL for a contained-collection read at one parent tuple."""
        base = join_url(self.service_url, self._build_contained_path(segments, key_chain))
        return f"{base}?{self._format_query_params(table_options, extra_filter, order_by)}"

    def _build_expand_url(
        self,
        segments: list[str],
        table_options: dict[str, str],
        extra_filter: str | None = None,
        order_by: str | None = None,
    ) -> str:
        """``A?...&$expand=B($expand=C($expand=D))`` for the full chain."""
        top, *children = segments
        base = join_url(self.service_url, top)
        query = self._format_query_params(table_options, extra_filter, order_by)
        if not children:
            return f"{base}?{query}"
        expand = ""
        for child in reversed(children):
            expand = f"{child}($expand={expand})" if expand else child
        return f"{base}?{query}&$expand={expand}"

    # --- read paths --------------------------------------------------------

    def _resolve_fk_columns(
        self, segments: list[str], namespace: str | None
    ) -> dict[tuple[str, str], str]:
        """Map ``(segment, pk_name) → unique FK column name`` for every
        non-leaf ancestor.

        OData v4 §13.4.3 makes contained-entity keys unique only within
        their immediate parent, so the destination composite key needs
        the full ancestor chain to be globally unique. Default name is
        ``<segment>_<pk>``; collisions get a leading ``_`` until unique.
        Empty mapping for flat tables.
        """
        if len(segments) < 2:
            return {}
        leaf_field_names = {
            f.name
            for f in self._own_fields_for_et(
                self._entity_type_for(CONTAINED_PATH_SEP.join(segments), namespace)
            )
        }
        used = set(leaf_field_names)
        resolved: dict[tuple[str, str], str] = {}
        for idx in range(len(segments) - 1):
            ancestor_et = self._entity_type_for(
                CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace
            )
            seg = segments[idx]
            for pk in self._own_primary_keys_for_et(ancestor_et):
                candidate = fk_column_name(seg, pk)
                while candidate in used:
                    candidate = "_" + candidate
                resolved[(seg, pk)] = candidate
                used.add(candidate)
        return resolved

    def _tag_with_ancestor_fks(
        self,
        row: dict,
        segments: list[str],
        chain: list[dict[str, Any]],
        fk_columns: dict[tuple[str, str], str],
    ) -> None:
        """Write ancestor primary-key values onto ``row`` under the
        resolved FK column names. Only ancestors present in
        ``fk_columns`` are materialized — ``_resolve_fk_columns`` decides
        which (just the immediate parent by default; every ancestor
        when ``include_ancestor_ids=true``)."""
        for idx, ancestor_keys in enumerate(chain):
            seg = segments[idx]
            for pk_name, pk_val in ancestor_keys.items():
                col = fk_columns.get((seg, pk_name))
                if col is not None:
                    row[col] = pk_val

    def _iter_parent_key_chains(
        self,
        segments: list[str],
        namespace: str | None,
        table_options: dict[str, str] | None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield every ancestor key chain (len = len(segments) - 1) reaching
        the leaf. Each level fetched with ``$select=<pks>``; user ``filter``
        not forwarded."""

        def _walk(level: int, chain: list[dict[str, Any]]):
            if level == len(segments) - 1:
                yield list(chain)
                return
            sub_segments = segments[: level + 1]
            ancestor_et = self._entity_type_for(CONTAINED_PATH_SEP.join(sub_segments), namespace)
            ancestor_pks = self._own_primary_keys_for_et(ancestor_et)
            if not ancestor_pks:
                raise ValueError(
                    f"Cannot walk contained path: segment {segments[level]!r} "
                    f"has no primary key declared in $metadata."
                )
            opts = {
                "page_size": (table_options or {}).get("page_size", "1000"),
                "select": ",".join(ancestor_pks),
            }
            url = (
                self._build_url(segments[0], opts)
                if level == 0
                else self._build_contained_url(sub_segments, chain, opts)
            )
            for row in self._fetch_pages(url):
                chain.append({pk: row.get(pk) for pk in ancestor_pks})
                yield from _walk(level + 1, chain)
                chain.pop()

        yield from _walk(0, [])

    def _read_contained_snapshot(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Walk the parent-key tree N+1 and emit leaf rows tagged with
        ancestor FKs. Full result in one call."""
        segments = parse_contained_path(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        fk_columns = self._resolve_fk_columns(segments, namespace)
        emitted: list[dict] = []
        for chain in self._iter_parent_key_chains(segments, namespace, table_options):
            for row in self._fetch_pages(self._build_contained_url(segments, chain, table_options)):
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                emitted.append(row)
        return iter(emitted), {}

    def _read_contained_expand(
        self, table_name: str, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single GET with nested ``$expand``; flatten the response into
        leaf rows tagged with ancestor FKs. Server depth caps surface as
        HTTP 4xx — no client-side fallback."""
        segments = parse_contained_path(table_name) or [table_name]
        if len(segments) < 2:
            raise ValueError(f"expand_contained requires a contained path; {table_name!r} is flat.")
        namespace = (table_options or {}).get("namespace")
        pks_per_level: list[list[str]] = []
        for idx in range(len(segments) - 1):
            et = self._entity_type_for(CONTAINED_PATH_SEP.join(segments[: idx + 1]), namespace)
            pks = self._own_primary_keys_for_et(et)
            if not pks:
                raise ValueError(
                    f"Cannot $expand contained path: segment {segments[idx]!r} "
                    f"has no primary key declared in $metadata."
                )
            pks_per_level.append(pks)
        fk_columns = self._resolve_fk_columns(segments, namespace)
        emitted: list[dict] = []
        for top_row in self._fetch_pages(self._build_expand_url(segments, table_options)):
            self._flatten_expand_response(
                0, top_row, segments, pks_per_level, [], fk_columns, emitted
            )
        return iter(emitted), {}

    def _flatten_expand_response(
        self,
        level: int,
        row: dict,
        segments: list[str],
        pks_per_level: list[list[str]],
        chain: list[dict[str, Any]],
        fk_columns: dict[tuple[str, str], str],
        out: list[dict],
    ) -> None:
        """Recurse into the nested $expand payload; tag and emit leaf rows."""
        if level == len(segments) - 1:
            clean = {k: v for k, v in row.items() if not k.startswith("@odata.")}
            self._tag_with_ancestor_fks(clean, segments, chain, fk_columns)
            out.append(clean)
            return
        pks = pks_per_level[level]
        chain.append({pk: row.get(pk) for pk in pks})
        for child in row.get(segments[level + 1]) or []:
            self._flatten_expand_response(
                level + 1, child, segments, pks_per_level, chain, fk_columns, out
            )
        chain.pop()

    def _leaf_cursor_order_by(
        self, table_name: str, namespace: str | None, cursor_field: str
    ) -> str:
        """``cursor asc, pk1 asc, ...`` — unique total order so server
        skiptokens don't split same-cursor cohorts."""
        leaf_pks = self._own_primary_keys_for_et(self._entity_type_for(table_name, namespace))
        terms = [f"{cursor_field} asc"]
        terms.extend(f"{pk} asc" for pk in leaf_pks if pk != cursor_field)
        return ",".join(terms)

    def _walk_contained_with_cursor(
        self,
        segments: list[str],
        chains: list[list[dict[str, Any]]],
        parent_idx_start: int,
        table_options: dict[str, str],
        extra_filter: str | None,
        order_by: str,
        cursor_field: str,
        since: Any,
        max_records: int,
        fk_columns: dict[tuple[str, str], str],
    ) -> tuple[list[dict], bool, int]:
        """Drive the per-parent fetch loop; return (rows, truncated, parent_idx)."""
        emitted: list[dict] = []
        truncated = False
        parent_idx = parent_idx_start
        while parent_idx < len(chains):
            chain = chains[parent_idx]
            url = self._build_contained_url(
                segments,
                chain,
                table_options,
                extra_filter=extra_filter,
                order_by=order_by,
            )
            for row in self._fetch_pages(url):
                rec_cursor = row.get(cursor_field)
                if since is not None and rec_cursor is not None and rec_cursor <= since:
                    continue
                self._tag_with_ancestor_fks(row, segments, chain, fk_columns)
                emitted.append(row)
                if len(emitted) >= max_records:
                    truncated = True
                    break
            if truncated:
                break
            parent_idx += 1
        return emitted, truncated, parent_idx

    def _read_contained_incremental(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        cursor_field: str,
    ) -> tuple[Iterator[dict], dict]:
        """Walk every parent tuple with ``$filter=cursor gt since``; track
        global max cursor in the offset. Truncation parks ``parent_idx``
        for next-call resume."""
        segments = parse_contained_path(table_name) or [table_name]
        namespace = (table_options or {}).get("namespace")
        since = (start_offset or {}).get("cursor")
        max_records = int((table_options or {}).get("max_records_per_batch", "100000"))
        order_by = self._leaf_cursor_order_by(table_name, namespace, cursor_field)
        extra_filter = self._cursor_filter(cursor_field, since)
        chains = list(self._iter_parent_key_chains(segments, namespace, table_options))
        emitted, truncated, parent_idx = self._walk_contained_with_cursor(
            segments,
            chains,
            int((start_offset or {}).get("parent_idx", 0)),
            table_options,
            extra_filter,
            order_by,
            cursor_field,
            since,
            max_records,
            self._resolve_fk_columns(segments, namespace),
        )
        if not emitted:
            return iter([]), start_offset or {}
        cursors = [r.get(cursor_field) for r in emitted if r.get(cursor_field) is not None]
        end_offset: dict = {"cursor": max(cursors) if cursors else since}
        if truncated:
            end_offset["parent_idx"] = parent_idx
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset
        return iter(emitted), end_offset
