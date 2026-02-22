# freshann Architecture

## Core layers

1. V2-only index format enforcement (`FormatVersion=2`)
2. Mutation durability and idempotent replay (`txn_id`, `seq_id`)
3. Pebble V2 storage with doc-ID indirection (`externalID <-> docID`)
4. Segment snapshots + manifest lifecycle
5. Uint64 graph-assisted candidate generation + bounded rerank
6. Maintenance (repair, consolidation/checkpoint) and verification

## On-disk layout

`<root>/<index_id>/`

- `meta.pebble/`
  - index spec and search defaults: `meta/spec/v2`
  - apply metadata: `meta/watermark`, `tok/applied/*`, `meta/applied_count`
  - vector counters: `meta/vector_count`, `meta/next_docid`
  - doc ID maps: `id/e2d/*`, `id/d2e/*`
  - vector rows: `vec/v2/*` (FP32 only)
  - metadata rows: `meta/v2/*` (msgpack partition/tags)
  - filter postings: `post/part/*`, `post/tag/*` (roaring64 chunked bitmaps)
  - graph metadata and pages: `graph/v2/head`, `graph/v2/page/*`
- `manifest.json`
  - active segment pointer and version
- `segments/*.seg`
  - immutable snapshot records

## Mutation flow

1. Validate request
2. Resolve or allocate docID
3. Write vector row + metadata row + doc mappings in one Pebble batch
4. Update partition/tag posting chunks by docID
5. Mark token applied and advance watermark in same batch
6. Track graph-dirty state and repair queue entries

## Query flow

1. Resolve per-query tuning via pluggable `BudgetPolicy` (`adaptive` or `fixed`)
2. Resolve filter universe (partition/tags) to candidate docIDs
3. Gather coarse candidates (if available)
4. Traverse uint64 graph with bounded `EfSearch` / `Beam`
5. Merge pending delta candidates only when small enough
6. Optional exact fallback (disabled by default)
7. Exact-score rerank with worker sharding and return top-k external IDs
8. Return diagnostics (`SearchRequest.Debug=true`) and resolved tuning

### Adaptive policy notes

- Adaptive mode uses dimension/metric priors as the base budget.
- Target recall and budget scale tune search aggressiveness without changing SQL/API shape.
- Filter selectivity can downscale candidate budgets to reduce wasted work on narrow predicates.
- Explicit per-request tuning fields always override adaptive outputs.

## Maintenance flow

- Repair queue removes deleted graph references.
- Flush/maintenance persists graph page generations and segment snapshots.
- Manifest updates are atomic.

## Verification

`Verify(deep=true)` checks:

- vector dimensions
- active segment readability
- graph presence when segment is active
- graph references resolve by docID

## Current constraints

- Input vectors: FP32
- Metrics: cosine, dot, euclidean
- Filters: partition + exact tag matches
- V1 index format is unsupported and must be rebuilt as V2
