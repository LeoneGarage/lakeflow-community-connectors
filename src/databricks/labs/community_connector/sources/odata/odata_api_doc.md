# OData v4 Source API Documentation

This connector targets the **OData v4** protocol as a generic data source. There is no fixed table list bundled with the connector — every table, schema, and primary key is derived at runtime from the target service's `$metadata` document. The same connector binary serves Microsoft Graph, Dynamics 365, SAP S/4HANA Cloud, SAP NetWeaver Gateway, Olingo-based self-hosted services, and the canonical `services.odata.org/V4/Northwind` reference service.

## Overview

### What this connector covers

- Any service that conforms to the OData v4 protocol and exposes a `$metadata` CSDL XML document at the service root.
- All entity sets declared by the service (one per `<EntitySet>` element under an `<EntityContainer>`).
- Snapshot ingestion (full refresh per trigger) and incremental CDC ingestion driven by a per-table cursor field.
- Multi-schema services that publish more than one `<Schema Namespace="...">` block — surfaced as Lakeflow namespaces.

### What this connector does not cover

- **OData v2 / v3.** The connector emits `OData-Version: 4.0` / `OData-MaxVersion: 4.0` headers and parses the v4 CSDL namespace `http://docs.oasis-open.org/odata/ns/edm`. Earlier-protocol services (the older `services.odata.org/V2/...` endpoints, classic SAP NetWeaver v2 endpoints) won't parse correctly.
- **OData functions and actions.** Only entity sets are exposed as tables. Bound and unbound function/action invocations are not surfaced.
- **The `cdc_with_deletes` ingestion type.** The connector always reports `ingestion_type` as `snapshot` or `cdc`. Deletes **are** captured when the server supports change tracking and `delta_tracking` is opted in: server-reported removals arrive as in-band tombstone rows (`_deleted=True`, see [Delta tracking](#delta-tracking-contract)). Without delta tracking (cursor/snapshot reads), deletes are not observable — soft-deleted rows must be modeled as updates to a status/`is_deleted` column on the entity itself.
- **Parallel partitioning of flat entity sets.** `@odata.nextLink` skiptokens are opaque to the client, so a single collection's page walk is serial. Contained-collection paths on the N+1 shape **do** partition — top-level rows are bin-packed into `num_partitions` slices, each Spark task walking its own subtrees (`SupportsPartitionedStream`; see the README's `num_partitions` row).

---

## Discovery model

Tables, schemas, and primary keys are not configured statically. They are pulled from the service's `$metadata` endpoint the first time any discovery or schema method is called, then cached for the lifetime of the connector instance.

### What is fetched

```
GET <service_url>$metadata
Accept: application/xml
```

The response is a CSDL XML document with this shape:

```xml
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="NorthwindModel" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key>
          <PropertyRef Name="OrderID"/>
        </Key>
        <Property Name="OrderID" Type="Edm.Int32" Nullable="false"/>
        <Property Name="CustomerID" Type="Edm.String"/>
        <Property Name="OrderDate" Type="Edm.DateTimeOffset"/>
        ...
      </EntityType>
    </Schema>
    <Schema Namespace="ODataWeb.Northwind.Model">
      <EntityContainer Name="NorthwindEntities">
        <EntitySet Name="Orders" EntityType="NorthwindModel.Order"/>
        <EntitySet Name="Customers" EntityType="NorthwindModel.Customer"/>
        ...
      </EntityContainer>
    </Schema>
  </DataServices>
</edmx:Edmx>
```

### What is derived from it

| Lakeflow concept | Derived from |
| --- | --- |
| Namespace list | Distinct `Namespace` attribute on every `<Schema>` that contains an `<EntityContainer>` with entity sets. |
| Table list (per namespace) | `<EntitySet>` `Name` attributes inside each schema's `<EntityContainer>`. |
| Table schema | `<Property>` children of the `<EntityType>` referenced by the entity set's `EntityType` attribute. |
| Primary keys | `<PropertyRef Name="..."/>` children of the entity type's `<Key>` element. |
| Column types | `Type` attribute of each `<Property>`, mapped through the EDM → Spark table below. |
| Column nullability | `Nullable` attribute (default `true` when omitted). |

### Disambiguation

When the same `<EntitySet>` name appears in more than one `<Schema>` namespace, `_entity_type_for(...)` raises:

```
ValueError: Entity set 'Customers' is declared in multiple namespaces:
['HR', 'Sales']. Set 'namespace' in table_options to disambiguate.
```

The pipeline resolves this by passing `namespace` in `table_configuration` for the affected table (see *Per-table options* below). When a name is unique across the entire service, `namespace` may be omitted.

---

## Authentication

Authentication is configured on the Unity Catalog connection. The connector picks an auth method from `auth_type`, or — when `auth_type` is not set — infers `bearer` if a `token` option is present.

| Method | `auth_type` | Required option keys | Optional |
| --- | --- | --- | --- |
| Bearer token | `bearer` | `token` | — |
| HTTP Basic | `basic` | `username`, `password` | — |
| API key in custom header | `api_key` | `api_key` | `api_key_header` (default `x-api-key`) |
| OAuth 2.0 client credentials | `oauth2` | `oauth2_token_url`, `oauth2_client_id`, `oauth2_client_secret` | `oauth2_scope` |
| OAuth 2.0 authorization code | `oauth2` | Same plus `oauth2_refresh_token` | `oauth2_access_token`, `oauth2_scope` |

Notes:

- **Bearer.** Sent as `Authorization: Bearer <token>`. Works for most modern OData APIs (Microsoft Graph, Dynamics 365, SAP S/4HANA Cloud).
- **Basic.** Sent as `Authorization: Basic <base64(user:pass)>` via `requests.auth.HTTPBasicAuth`. Common for on-prem SAP NetWeaver / Gateway.
- **API key.** Sent as `<header-name>: <key>`. The header name defaults to `x-api-key` and is configurable per service via `api_key_header`.
- **OAuth2 (client credentials).** At session-construction time the connector POSTs `grant_type=client_credentials` to `oauth2_token_url` and caches the access token on the session for the run.
- **OAuth2 (authorization code / user-delegated).** The user runs the authorization-code flow once (externally — e.g. via the SDK's OAuth helpers) and supplies the resulting `oauth2_access_token` and `oauth2_refresh_token` on the connection. The connector uses the pre-supplied access token directly; on HTTP 401 from the source it POSTs `grant_type=refresh_token` to `oauth2_token_url` (with `client_id` + `client_secret` for client authentication) and retries the request once. Providers that rotate refresh tokens have the new value tracked in-process for the rest of the run.

Tokens, passwords, API keys, and OAuth client secrets are all declared `secret: true` in `connector_spec.yaml` and are masked by the Unity Catalog connection store.

---

## Connection parameters

These are set on the UC connection (alongside the auth fields above).

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `service_url` | string | Yes | — | OData v4 service root URL. Must end at the service segment; the connector appends entity-set names and `$metadata` directly. Example: `https://services.odata.org/V4/Northwind/Northwind.svc/`. Must **not** embed credentials (`user:pass@host` userinfo — rejected at init, since the URL is echoed in logs and error messages); use the auth options instead. |
| `timeout_seconds` | string | No | `180` | HTTP timeout per request, in seconds. |
| `extra_headers` | string | No | — | Extra request headers as `Key:Value,Key2:Value2`. Useful for tenant IDs, CSRF tokens, or non-standard server discriminators. |
| `metadata_cache_ttl_seconds` | string | No | `60` | TTL of the shared `$metadata` cache (process dict + on-disk pickle). |
| `max_retries` | string | No | `5` | Retry budget for transient failures (429/5xx, network errors) — applies to source requests and the OAuth2 token endpoint alike. |
| `retry_max_delay_seconds` | string | No | `60` | Cap on any single retry sleep (exponential backoff / `Retry-After`). |
| `verbose_http_logging` | string | No | `false` | Log every request and response line at INFO. Source data lands in the log stream — debugging only. |
| `verbose_http_log_body_chars` | string | No | `500` | Response-body prefix length included in verbose logs. |

The connector additionally sends these headers on every request:

```
Accept: application/json
OData-Version: 4.0
OData-MaxVersion: 4.0
```

`$metadata` requests override `Accept` to `application/xml`.

---

## Per-table options

These are passed to the connector via the pipeline's `table_configuration` block. Every key listed below must appear in `external_options_allowlist` on the UC connection (the spec already includes all of them).

| Option | Default | Description |
| --- | --- | --- |
| `namespace` | — | Selects the `<Schema Namespace="...">` block that declares this entity set. Required only when the same entity-set name appears in multiple schemas. |
| `cursor_field` | — | Column to drive incremental reads. Absent → snapshot. Must be naturally ordered by OData `$orderby` (timestamps and monotonic IDs are typical). On flat tables, the column must be a property of the entity. On contained paths, the connector first checks the leaf entity for the column; if missing, it walks leaf→root to find the **closest ancestor** that has it, filters at that ancestor level, and propagates the ancestor's cursor value onto every emitted leaf row. |
| `select` | all properties | Comma-separated `$select` projection. Both the on-wire OData query and the derived Spark schema are filtered to these columns. On contained paths, synthetic ancestor-FK columns (default form `<seg>_<pk>`) are always preserved regardless of `select`. |
| `filter` | — | Additional OData `$filter` expression, AND-ed with any cursor filter the connector generates. Applies to the leaf collection only on contained paths. |
| `page_size` | `1000` | Value of `$top` sent on each HTTP request. Sets the maximum rows per OData page. Some servers cap this server-side (see *Known limits*). |
| `max_records_per_batch` | `10000` | Client-side cap on records returned per `read_table` call. The connector truncates and returns control to the framework once this limit is hit. Independent of `page_size`. |
| `delta_tracking` | `disabled` | Opt-in OData v4 delta queries. `disabled` keeps the existing snapshot / cursor behavior; `auto` probes the server's `Prefer: odata.track-changes` support once per table and falls back if missing; `enabled` requires support and errors on the first read if the server doesn't acknowledge. See [Delta tracking](#delta-tracking-contract). Mutually exclusive with contained-path tables. |
| `expand_contained` | `auto` | For contained-collection paths (`Parent__Child__...`). `auto` (default) preflights the server's nested-`$expand` support and uses a single `GET Parent?$expand=Child(...)` when verified, else the N+1 per-parent traversal; `true` forces the `$expand` read, `false` forces N+1. See [Contained navigation properties](#contained-navigation-properties). |

The table above covers the core options only. The full allowlisted set also
includes `pagination`, `cursor_probe`, `contained_fetch`, `num_partitions`,
`cursor_nulls`, `filter_at_*`, `exclude_ancestor_columns`,
`cursor_lookback_seconds`, `cursor_lookback_factor`, and
`cursor_lookback_max_seconds` — the **authoritative, complete option
reference is the [README's Per-table options table](README.md)**.

`namespace` is consumed by the connector before the request is built; the rest all influence the URL, the per-batch loop, or the request semantics.

---

## Incremental ingestion contract

When a table's `table_configuration` includes `cursor_field`, the connector switches from snapshot mode to CDC mode. This section is the contract for that mode.

### Query shape

Per batch, the connector issues a request of the form:

```
GET <service_url><entity_set>
  ?$top=<page_size>
  &$select=<select>                          (optional)
  &$filter=(<user filter>) and (<cursor filter>)
  &$orderby=<cursor_field> asc, <pk1> asc, <pk2> asc, ...
```

The cursor filter is:

| State | `$filter` clause for the cursor |
| --- | --- |
| First call (no checkpoint) | *(no cursor filter)* — server returns rows from the natural start of the table |
| Resume after checkpoint `since` | `<cursor_field> gt <since>` |

There is **no wall-clock ceiling** on the cursor. `max_records_per_batch` is the only per-call cap. Two consequences:

* **Continuous SDP pipelines work.** A single connector instance can live for the entire stream and still see fresh source state on every micro-batch, because the connector never freezes a "snapshot at startup" timestamp that would shut out later-arriving rows.
* **The cursor column doesn't have to be a timestamp.** Monotonic integer IDs, GUIDs, lexicographic strings — anything the server can order in `$orderby` and compare in `$filter` works the same way. The connector emits the cursor value verbatim using `_odata_literal`, so an `Edm.Int32` cursor produces `OrderID gt 10248` (no quotes), an `Edm.DateTimeOffset` cursor produces `ModifiedAt gt 2024-03-01T00:00:00Z`, and so on.

### Why primary keys are appended to `$orderby`

Without a fully-unique total ordering, OData servers that paginate internally with a value-based skiptoken (the spec allows opaque tokens of any shape) can split a same-cursor cohort across pages: the server's skiptoken applies strict-`>` semantics on the cursor value alone and silently drops the unread tail. Appending every primary-key column to `$orderby` forces the skiptoken to include the key in its tie-break, so no rows are lost mid-cohort.

### Boundary trim

Every batch, after reading up to `max_records_per_batch` rows, the connector inspects the trailing run of records that share the boundary cursor value and **drops the entire trailing cohort** (function `_trim_to_distinct_cursor_boundary`). The next call resumes from the last *distinct* cursor value seen, not the literal last row.

This trim runs on every batch, not just truncated ones, for two reasons:

1. If the trailing cohort is split across pages, dropping it lets the next call's `cursor gt <prev_distinct>` re-fetch the complete cohort, including the un-read tail.
2. If concurrent writers insert sibling rows with the same cursor value before the next call, those siblings would otherwise be lost — a `cursor gt <last>` filter strictly excludes them. Re-fetching from `<prev_distinct>` picks them up.

Re-fetched rows arriving in subsequent batches are deduped at the destination by `apply_changes` doing a MERGE on the primary key. **This is why CDC mode requires a real primary key in the entity type's `<Key>` element.** A service whose entity type has no `<Key>` will surface as `primary_keys=[]` and incremental ingestion to a Delta table will accumulate duplicates.

### Edge case: every record in the batch shares one cursor value

If `max_records_per_batch` is too small to contain even one same-cursor cohort, the trim returns an empty list. Two paths:

- **Truncated batch** (more records exist on the same cursor value): the connector raises a `RuntimeError` instructing the operator to raise `max_records_per_batch` above the largest same-cursor cohort, or choose a higher-cardinality cursor field.
- **Natural exhaustion** (the server returned no `@odata.nextLink`): the records are emitted as-is. A residual race exists for same-cursor rows inserted between this call and the next — unavoidable without finer cursor resolution.

### Implication for low-cardinality cursors

A date-only cursor (`Edm.Date`) or a one-second-resolution timestamp on a busy table tends to produce large same-cursor cohorts. That's fine — the boundary trim and PK-based MERGE handle it — but operators must size `max_records_per_batch` above the largest expected same-cursor cohort. Picking a finer-resolution cursor (`Edm.DateTimeOffset` with sub-second precision, or a monotonic surrogate key) is the cleanest fix when available.

### Snapshot mode

When `cursor_field` is not set, the connector walks `@odata.nextLink` from the initial `$top=<page_size>` request until the server stops returning a next link, streaming rows lazily one page at a time (the full result set is never materialized in memory). No cursor filter is applied. A PK-only `$orderby` **is** sent whenever the entity declares keys — OData v4 §11.2.5.7 gives no stable default ordering across pages, so skiptoken pagination needs an explicit total order.

The OData v4 spec allows `@odata.nextLink` to be either an absolute URL or a relative one resolved against the request URL. Some services (SAP NetWeaver Gateway, certain self-hosted Olingo deployments) return only `Customers?$skiptoken=...`. The connector resolves these via `urllib.parse.urljoin` against `resp.url`, so absolute links pass through unchanged and relative links are prepended with the service root.

### OData control properties

Every row returned to the framework has had OData control properties stripped: keys prefixed with `@odata.` (e.g. `@odata.etag`, `@odata.id`, `@odata.editLink`) are not yielded.

---

## Delta tracking contract

OData v4 §11.3 ("Requesting Changes") defines a server-driven change-tracking protocol. When opted into via `delta_tracking ∈ {auto, enabled}` and supported by the server, the connector takes this path instead of cursor-based filtering.

### Capability detection

`delta_tracking=auto` performs a one-time probe per `(namespace, table)` pair. The probe sends an entity-set GET with `$top=1` and the header `Prefer: odata.track-changes`. The connector inspects the response:

- 200 + `Preference-Applied: odata.track-changes` header → delta supported. Cached.
- 200 + missing `Preference-Applied` header → server silently ignored the prefer. Falls back to whatever cursor/snapshot config is set. Cached.
- non-200 status (400/405 commonly) → server rejected the prefer. Falls back. Cached.
- Hard, non-transient failure (definitive rejection) → falls back. Cached `False`.
- Transient failure (transport error, retryable status incl. 408, non-JSON body) → falls back **for this batch only**; nothing is cached and the next call re-probes.

`delta_tracking=enabled` skips the probe entirely. If the actual bootstrap response is missing `Preference-Applied`, the connector raises a `RuntimeError` pointing the operator at `delta_tracking=disabled` as the fallback.

`delta_tracking=disabled` (the default) never sends the prefer header. Zero behavior change versus pre-delta versions of the connector.

### Offset shape

Three offsets coexist with the existing `{}` (snapshot) and `{"cursor": ...}` (cursor-based) shapes:

- `{"delta_link": "<url>"}` — ready to resume from the server's last-minted delta link.
- `{"next_link": "<url>", "delta_link": "<url>"}` — mid-pagination after a `max_records_per_batch` cap hit. `next_link` is the preferred resume; `delta_link` is the fallback if `next_link` expires.
- `{}` — start a fresh bootstrap (initial run or post-410 reset).

The dispatch in `read_table` recognises any of these and routes through the delta path even if `delta_tracking` is no longer set in `table_options` — checkpointed offsets carry the mode forward across config changes.

### Request shape

Bootstrap (first call, no checkpointed delta state):

```
GET <service_url><entity_set>?$top=<page_size>
Prefer: odata.track-changes
```

Resume (`delta_link` or `next_link` in offset):

```
GET <stored_link>
```

The delta / next links are server-minted opaque URLs; the connector follows them verbatim without re-applying `$filter` / `$orderby` / `$top` from `table_options`.

### Response handling

Each page in the response's `value` array is one of:

- A regular entity → emitted with all `@odata.*` keys stripped, `_deleted=False`, and a fresh `_lc_sequence`.
- An `@removed` entry (shape: `{"@removed": {"reason": "deleted"}, "<key>": <id>}`) → emitted with only the primary-key fields populated, `_deleted=True`, and a fresh `_lc_sequence`. Following the `microsoft_teams` precedent, deletions are surfaced in-band rather than via `cdc_with_deletes` + `read_table_deletes`.

The terminal page carries `@odata.deltaLink` (the next resume point). Intermediate pages carry `@odata.nextLink`.

### Synthetic columns

Two columns are appended to the declared schema for delta-active tables:

| Column | Type | Purpose |
| --- | --- | --- |
| `_deleted` | `BooleanType` (non-null) | In-band tombstone flag. `True` only for `@removed` entries; `False` for adds and changes. |
| `_lc_sequence` | `StringType` (non-null) | `read_table_metadata` reports this as `cursor_field`. Format: `<20-digit zero-padded nanoseconds-since-epoch>_<12-digit counter>`. Strictly monotonic per emit per process, so `apply_changes` MERGE-by-PK picks deterministic winners when the same primary key appears multiple times in one batch. |

### Graph deltaLink-rotation guard

Some servers (notably Microsoft Graph) mint a fresh `@odata.deltaLink` on every response, even when the change set is empty. Without compensation, every trigger would advance the offset and the framework would emit empty Delta commits in perpetuity.

The connector detects this case: if a resume call started with `prev_delta_link != None` and produced zero records, it returns the prior link unchanged so `end_offset == start_offset`. The framework treats that as "no progress this trigger" and `Trigger.AvailableNow` terminates cleanly.

### Token expiry (HTTP 410)

When the server returns 410 Gone on a stored `delta_link` or `next_link`, the connector silently re-bootstraps: a fresh `Prefer: odata.track-changes` GET against the entity set, emit the full snapshot as `_deleted=False` upserts, return a brand-new `delta_link`. MERGE-by-PK at the destination reconciles re-fetched rows with what's already there.

### Sparse-update rejection

OData v4 §11.4 allows delta payloads to return only the *modified* properties on an updated entity. Applying that as-is would write NULLs over good destination values — silent corruption. The connector refuses such payloads.

Detection runs on **every** non-tombstone entry (mixed payloads — full entities for creates, changed-properties-only for updates — are the norm, so first-entry sampling would wave sparse updates through). The expected key set is precomputed once per walk:

- The full declared schema, minus the synthetic `_deleted` / `_lc_sequence` columns.
- Filtered to the `$select` projection if set.

If any expected key is missing from the actual entry, the connector raises `RuntimeError` and points the operator at `delta_tracking=disabled` (or `$select` to narrow the schema).

### Mutual exclusion with `cursor_field`

`delta_tracking=enabled` plus `cursor_field` is a `ValueError` at first metadata-resolution call. The two are conflicting sequencing strategies. `delta_tracking=auto` plus `cursor_field` falls through to the cursor path (cursor wins, no probe).

### Worked example (Microsoft Graph users/delta)

Trigger 1, no offset:

```
GET https://graph.microsoft.com/v1.0/users
Prefer: odata.track-changes
→ 200, Preference-Applied: odata.track-changes
  body: {"value": [...all current users...], "@odata.deltaLink": "https://...users?$deltatoken=A"}
```

Emitted: every user as `_deleted=False` with monotonic `_lc_sequence`.
Offset: `{"delta_link": "https://...users?$deltatoken=A"}`.

Trigger 2, after a user changed their `displayName` and another was deleted:

```
GET https://graph.microsoft.com/v1.0/users?$deltatoken=A
→ 200
  body: {"value": [
    {"id": "u1", "displayName": "New Name", ...},
    {"@removed": {"reason": "deleted"}, "id": "u2"}
  ], "@odata.deltaLink": "https://...users?$deltatoken=B"}
```

Emitted: one row for `u1` with full payload (`_deleted=False`), one row for `u2` carrying only `id` (`_deleted=True`).
Offset: `{"delta_link": "https://...users?$deltatoken=B"}`.

Trigger 3, no changes since `B`:

```
GET https://graph.microsoft.com/v1.0/users?$deltatoken=B
→ 200
  body: {"value": [], "@odata.deltaLink": "https://...users?$deltatoken=C"}
```

Emitted: zero rows. Offset: `{"delta_link": "https://...users?$deltatoken=B"}` — prior link preserved by the rotation guard, so the framework sees no progress and the trigger terminates.

---

## Contained navigation properties

OData v4 §13.4.3 defines `<NavigationProperty ContainsTarget="true">` on an EntityType: a collection that is *owned by* the parent entity rather than declared as a top-level EntitySet. The contained collection is addressed by traversing the parent's key — `GET Parent(<key>)/ContainedNavProp` — and each parent has its own independent contained collection. The protocol allows recursive containment, so a service can declare `Parent → Child → Grandchild → ...` chains.

The connector surfaces these as double-underscore-pathed tables (`__` between segments — slash isn't valid in Spark SQL identifiers, which the framework uses for view names) alongside top-level entity sets, e.g. `Parents__Children__Notes`, up to **10 segments deep** (the depth cap prevents pathological discovery walks on services that declare circular containment; cycles within the cap are also detected and broken). Path parsing rejects empty segments and over-depth paths at `read_table_metadata` / `get_table_schema` time.

### Discovery

`list_tables_in_namespace([<schema>])` returns both:

- Top-level entity sets declared in the schema's `<EntityContainer>`.
- Every contained-collection path reachable from those sets via a BFS through `ContainsTarget="true"` navigation properties (inherited from base types too), capped at `MAX_CONTAINED_DEPTH = 10` and with cycle detection on the type-qualified name set.

Output is deterministic — flat sets sorted first, then contained paths sorted.

### Schema augmentation

For a path with N segments, the leaf entity's own properties are preceded by synthetic FK columns for **every non-leaf ancestor**. OData v4 §13.4.3 makes contained-entity keys unique only within their immediate parent, so the destination composite key must include the full chain to be globally unique. The default name is `<segment>_<pkname>` (no fixed prefix). When that name would collide with a leaf property or with another FK, the connector prepends a leading `_` until the name is unique.

```
<parent_segment>_<parent_pkname...>   ← primary keys of the leaf's IMMEDIATE parent
<leaf's own properties>
```

The composite primary key reported in `read_table_metadata` is the full chain: every ancestor's FK columns followed by the leaf's own primary keys. This is what makes `apply_changes_from_snapshot` see one row per key on tables where leaf IDs only repeat within a grandparent branch (a common case in services like Intergraph SCApi).

When an ancestor has a composite primary key, every key column gets its own `<seg>_<pk>` field. The URL traversal passes through every ancestor's keys (the OData wire path is `A(a)/B(b)/C(c)/D`), and every ancestor's keys are also materialised as columns on the destination D rows.

**Collision example.** If `Items` has its own property `Owners_Id` and the path is `Owners__Items`, the connector emits `_Owners_Id` (FK, leading underscore) and `Owners_Id` (the leaf's own property, untouched). With multiple collisions, more leading underscores are added until unique.

`select` on a contained path filters only the leaf entity's own properties — every ancestor's FK columns are always preserved (the resolved names are compared against the leaf-only set, not against the input `select` list).

### Read modes

Selected via `expand_contained`:

**N+1 traversal (`expand_contained=false`; `auto`'s fallback).** For a path `A/B/C/D`:

1. `GET A?$select=<A_pks>&$top=<page_size>` — enumerate top-level parent keys.
2. For each `A_key`: `GET A(<A_key>)/B?$select=<B_pks>` — enumerate level-2 parents.
3. For each `(A_key, B_key)`: `GET A(<A_key>)/B(<B_key>)/C?$select=<C_pks>` — enumerate level-3.
4. For each `(A_key, B_key, C_key)`: `GET A(<A_key>)/B(<B_key>)/C(<C_key>)/D?<query>` — fetch leaves.

Pagination (`@odata.nextLink`) walks happen *within* each per-parent fetch. Cost is O(product of parent fanouts) HTTP round trips; bandwidth is proportional to leaf row count plus a small overhead for the PK-only enumerations.

Key predicate quoting: single-key parents use the bare form `(value)`; composite-key parents use the named form `(K1=v1,K2=v2)`. String values pass through `_odata_literal` for single-quote escaping; timestamps pass through bare per OData v4 §5.1.1.6.1.

**Single `$expand` chain (`expand_contained=true`; `auto` — the default — on a preflight-verified server).** One HTTP request per pipeline trigger:

```
GET A?$select=...&$top=...&$expand=B($expand=C($expand=D))
```

The connector flattens the nested JSON response recursively: for each top row, descend into the named nav-property array on each level, extracting and propagating ancestor PK values until the leaf level is reached. `@odata.*` control properties are stripped from leaf rows during flattening (the top-level `_fetch_pages` strip is applied only to outermost rows).

Most OData servers cap `$expand` depth at 1; deeper expands surface as HTTP 4xx and propagate verbatim. Known to honor depth ≥ 2: Microsoft Graph (some endpoints), SAP S/4HANA Cloud (per-service configuration). Don't enable against a server you haven't verified.

### Cursor-based incremental on contained paths

Set `cursor_field` to a column on the leaf entity. The connector walks every parent tuple per `read_table` call, applies `$filter=cursor gt since` and `$orderby=cursor asc, leaf_pk asc` to each per-parent fetch, and tracks the global max cursor across all parents in the offset's `cursor` key.

Offset shape: `{"cursor": "<max_seen_value>"}` on natural completion. When truncated mid-walk by `max_records_per_batch` the offset parks the truncated parent's **key chain** (`parent_keys`, plus `parent_cursor` on the ancestor-cursor path) alongside a `running_max` accumulator and a legacy `parent_idx` (downgrade fallback only). The resume re-positions by the enumeration's own ordering keys — churn-stable under parent inserts/deletes between batches — and rows already emitted within the resumed parent are elided by the parked continuation filter.

Termination: when an end_offset equal to the start_offset would be returned (no new rows anywhere), the connector emits zero rows and the same offset, satisfying the framework's "no progress" stop condition.

Truncation handling: when `max_records_per_batch` caps the walk mid-parent, the connector trims the trailing same-cursor cohort *within the truncated parent only* (`_trim_to_distinct_cursor_boundary`), and the returned offset carries a `truncated_chain_cursor` alongside `cursor`, `parent_keys` (the key-based resume position), `running_max`, and the legacy `parent_idx`. The resumed call uses `cursor gt truncated_chain_cursor` for the truncated parent (re-picks up its boundary cohort) and `cursor gt cursor` (the original `since`) for every subsequent parent — per-parent cursor distributions are independent, so a single boundary value can't safely cover them all. After the resumed walk completes naturally the offset collapses back to `{"cursor": <max_seen>}`; subsequent batches may re-emit earlier parents' rows whose cursors lie above `max_seen` from the resume, but `apply_changes` keyed on the composite PK dedupes them at the destination. If a parent's same-cursor cohort exceeds `max_records_per_batch` *and the server returned that parent's whole leaf collection in one page* (no `@odata.nextLink`), the cohort is complete but has no splittable boundary — the connector emits it in full and continues to the next parent (overshooting the cap for that one parent) rather than failing, advancing the watermark exactly as natural completion would.

### Ancestor-cursor fallback

When the leaf entity doesn't declare `cursor_field` as a property but one of its ancestors does, the connector falls through to **ancestor-level filtering**:

1. `_find_cursor_level` walks `segments` leaf → root and returns the index of the closest segment whose entity type has the column.
2. The chain walk at that level includes `cursor_field` in `$select`, applies `$filter=<cursor> gt <since>` (on resume) and `$orderby=<cursor> asc, <pks> asc`. Other ancestor levels still fetch just their PKs.
3. For each matching ancestor tuple, the leaf collection is fetched **unfiltered** (the leaf doesn't have the column to filter by), and every emitted leaf row is stamped with the ancestor's cursor value under `cursor_field`.
4. `get_table_schema` includes `cursor_field` in the leaf schema with the ancestor's declared type (e.g. `TimestampType` for `Edm.DateTimeOffset`).
5. The offset tracks the max ancestor-cursor seen across the batch, same shape as the leaf-cursor case.

If `cursor_field` isn't a property anywhere along the path, `read_table` raises a `ValueError` naming the table.

### Mutex with delta tracking

`delta_tracking=enabled` on a contained path raises `ValueError` at `read_table` dispatch — server-driven change tracking is defined against top-level entity sets in OData v4 §11.3, not parent-keyed traversals. `delta_tracking=auto` silently resolves to disabled on contained paths (the auto-probe is skipped; the URL shape isn't compatible with the probe's GET).

---

## Type mapping

EDM primitive types are mapped to Spark types as follows. Any unrecognized type falls back to `StringType` (the raw JSON representation is preserved on the wire).

| EDM type | Spark type | Notes |
| --- | --- | --- |
| `Edm.String` | `StringType` | |
| `Edm.Boolean` | `BooleanType` | |
| `Edm.Byte` | `IntegerType` | Widened — the framework's `parse_value` doesn't support `ByteType`/`ShortType`, so the narrow EDM widths map to `IntegerType`. |
| `Edm.SByte` | `IntegerType` | Widened (see `Edm.Byte`). |
| `Edm.Int16` | `IntegerType` | Widened (see `Edm.Byte`). |
| `Edm.Int32` | `IntegerType` | |
| `Edm.Int64` | `LongType` | |
| `Edm.Single` | `FloatType` | |
| `Edm.Double` | `DoubleType` | |
| `Edm.Decimal` | `DecimalType(P, S)` | Honours the CSDL-declared `Precision`/`Scale` facets (clamped to Spark's 38-digit max; `Scale` absent with `Precision` declared → scale 0 per the CSDL default). Absent facets or `Scale="variable"` → the wide `DecimalType(38, 18)` fallback. |
| `Edm.Date` | `DateType` | Calendar date, no time component. |
| `Edm.DateTime` | `TimestampType` | OData v2 carryover; some v4 services still emit it. |
| `Edm.DateTimeOffset` | `TimestampType` | The standard v4 timestamp type. |
| `Edm.TimeOfDay` | `StringType` | No native Spark `TimeType`. |
| `Edm.Duration` | `StringType` | ISO 8601 duration text. |
| `Edm.Guid` | `StringType` | |
| `Edm.Binary` | `BinaryType` | Base64-encoded on the wire; downstream callers can use `_decode_binary` to materialize bytes. |

Complex-typed, enum-typed, `Collection(...)`, and TypeDefinition-typed `<Property>` elements are surfaced as `StringType` columns, with structured (object/array) values rendered as **JSON text** at the emit boundary (parseable downstream with `from_json`). Only navigation properties are unsurfaced.

---

## Worked example: Northwind

The canonical public OData v4 reference service is `https://services.odata.org/V4/Northwind/Northwind.svc/`. Its `$metadata` declares two schemas:

- `NorthwindModel` — entity types.
- `ODataWeb.Northwind.Model` — the entity container with entity sets `Customers`, `Orders`, `Order_Details`, `Products`, etc.

Because only one schema (`ODataWeb.Northwind.Model`) contains the `<EntityContainer>` with entity sets, the discovery layer returns a single namespace.

### Connection (UC)

```bash
community-connector create_connection odata northwind_connection \
  -o '{
        "service_url": "https://services.odata.org/V4/Northwind/Northwind.svc/"
      }' \
  --spec ./src/databricks/labs/community_connector/sources/odata/connector_spec.yaml
```

(The public Northwind service requires no auth. Real-world services need one of the auth blocks from the *Authentication* section.)

### Pipeline (`ingest.py`)

```python
from databricks.labs.community_connector.pipeline import build_pipeline
from databricks.labs.community_connector.sources.odata import ODataLakeflowConnect

build_pipeline(
    connector_cls=ODataLakeflowConnect,
    tables=[
        # Snapshot ingest — no cursor.
        {
            "table": {
                "source_table": "Customers",
            }
        },
        # Incremental CDC ingest — cursor on OrderDate.
        {
            "table": {
                "source_table": "Orders",
                "primary_keys": ["OrderID"],
                "table_configuration": {
                    "cursor_field": "OrderDate",
                    "max_records_per_batch": "10000",
                    "page_size": "500",
                },
            }
        },
    ],
)
```

### What happens at runtime for `Orders`

1. `read_table_metadata("Orders", ...)` reads `$metadata`, finds `EntityType="NorthwindModel.Order"`, returns `primary_keys=["OrderID"]`, `cursor_field="OrderDate"`, `ingestion_type="cdc"`.
2. `get_table_schema("Orders", ...)` returns a `StructType` with `OrderID: int`, `CustomerID: string`, `OrderDate: timestamp`, `ShippedDate: timestamp`, etc., derived from the `<Property>` children of `NorthwindModel.Order`.
3. First call to `read_table` has no `start_offset`. The URL is:
   ```
   .../Orders?$top=500
            &$orderby=OrderDate asc, OrderID asc
   ```
   No cursor `$filter` on the first call — the connector pulls from the natural start of the table and lets `max_records_per_batch` (10000) cap the call.
4. Rows stream in via `@odata.nextLink` pagination. The connector accumulates up to 10000 rows.
5. The boundary trim runs. Many Northwind orders share an `OrderDate` (date-precision), so the trailing same-day cohort is dropped. The end offset is the last *distinct* `OrderDate` seen.
6. Next call resumes with `OrderDate gt <prev_distinct>`. The previously-dropped same-day cohort is re-fetched. `apply_changes` MERGEs them by `OrderID`, so the destination has each order exactly once.
7. Continuous mode: when the source grows under the running stream, subsequent calls keep advancing `<prev_distinct>` past the new rows. No timestamp ceiling has to expire for that to happen.

### Why `OrderDate` works as a cursor even though many rows share each date

Northwind `OrderDate` is a date-precision field — dozens of orders can share the same date. Without the boundary trim, a `gt` filter on the next call would skip every order sharing the boundary date. With the trim, the cohort is re-read every batch and MERGE-deduped at the destination. The only sizing requirement is that `max_records_per_batch` (10000 above) exceeds the largest single-day order count — easily true for Northwind.

If `max_records_per_batch` were set to, say, `10`, the connector would raise `RuntimeError` the first time a single `OrderDate` exceeded 10 orders, with a message instructing the operator to either raise the cap or pick a higher-cardinality cursor.

---

## Known limits

- **Server-side `$top` caps.** Some services cap `$top` below the requested value (Microsoft Graph at 999 for most endpoints; certain SAP services at 5000). Under the default `pagination=auto` the connector follows the server's `@odata.nextLink` whenever one is emitted, and when a server page-limits *without* emitting a link it falls back to a keyset seek / `$skip` drain until an empty page — so a smaller effective page size costs throughput, never rows. See the README's `pagination` row for the full mode matrix.
- **Opaque `$skiptoken` stability requires a unique total `$orderby`.** As described in *Incremental ingestion contract*, the connector unconditionally appends every primary-key column to `$orderby` in CDC mode. Snapshot reads under the default `pagination=auto` also send `$top=1000` plus a stable PK `$orderby` where one is needed for the client-driven drain; only `pagination=nextlink` sends a `$top`-free snapshot scan that follows server pagination as-is (a PK-only `$orderby` is still sent whenever the entity declares keys — skiptoken stability needs it).
- **Relative `@odata.nextLink`.** Handled — resolved against the response URL via `urljoin`. Absolute links pass through unchanged.
- **The `cdc_with_deletes` ingestion type is never reported.** Deletes **are** captured under `delta_tracking` (in-band `_deleted=True` tombstone rows — see [Delta tracking](#delta-tracking-contract)); cursor/snapshot reads cannot observe deletes, so there soft-deletes must be modeled as updates to a status column.
- **Flat entity sets read single-partition.** Skiptokens are opaque, so one collection's page walk can't be split. Contained N+1 paths partition across top-level subtrees via `num_partitions` (`SupportsPartitionedStream`).
- **Schema cache.** `$metadata` is cached with a TTL (default 60 s) in a process-wide dict plus an on-disk pickle shared across forked PySpark workers. Schema drift mid-run is not detected; a later trigger picks up the new shape once the TTL lapses.
- **Functions / actions not exposed.** Only `<EntitySet>` declarations become tables. Bound and unbound OData functions and actions are ignored.
- **Cursor field must be a plain orderable property.** The connector sends `$orderby=<cursor_field> asc` literally, so complex-typed properties, navigation properties, and computed expressions are not valid cursors. On flat tables it must live on the entity itself; on contained paths the connector resolves the closest level (leaf → root) that declares it.
- **OData v2 / v3 not supported.** The connector parses the v4 CSDL XML namespace and emits v4 protocol headers.
