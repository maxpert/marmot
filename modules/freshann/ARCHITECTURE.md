# freshann Architecture

## Core layers

1. Mutation durability and idempotent replay (`txn_id`, `seq_id`)
2. Pebble storage with doc-ID indirection (`externalID <-> docID`)
3. Segment snapshots + manifest lifecycle
4. Graph-assisted candidate generation + bounded rerank
5. Maintenance (repair, compaction/rebuild) and verification

## On-disk layout

`<root>/<index_id>/`

- `meta.pebble/`
  - index spec and search defaults
  - `meta/watermark`, `applied/*`
  - doc ID maps: `id/e2d/*`, `id/d2e/*`
  - vector rows: `vecdoc/*`
  - filter postings and hash-to-ID maps
  - graph metadata and adjacency keys
- `manifest.json`
  - active segment pointer and version
- `segments/*.seg`
  - immutable snapshot records

## Mutation flow

1. Validate request
2. Resolve or allocate docID
3. Write vector row + doc mappings in Pebble batch
4. Update filter postings and id-map entries
5. Mark token applied and advance watermark
6. Track graph-dirty state and repair queue entries

## Query flow

1. Resolve per-query tuning via pluggable `BudgetPolicy` (`adaptive` or `fixed`)
2. Resolve filter universe (partition/tags) to candidate docIDs
3. Traverse graph with bounded `EfSearch` / `Beam`
4. Merge pending delta candidates only when small enough
5. Optional exact fallback (disabled by default)
6. Exact-score rerank with worker sharding and return top-k external IDs

### Adaptive policy notes

- Adaptive mode uses dimension/metric priors as the base budget.
- Target recall and budget scale tune search aggressiveness without changing SQL/API shape.
- Filter selectivity can downscale candidate budgets to reduce wasted work on narrow predicates.
- Explicit per-request tuning fields always override adaptive outputs.

## Maintenance flow

- Repair queue removes deleted graph references.
- Flush/maintenance can rebuild graph and persist snapshot state.
- Manifest updates are atomic.

## Verification

`Verify(deep=true)` checks:

- vector dimensions
- active segment readability
- graph presence when segment is active
- graph references resolve for both legacy external-ID and doc-ID graph nodes

## Current constraints

- Input vectors: FP32
- Metrics: cosine, dot, euclidean
- Filters: partition + exact tag matches
- Graph build still needs further work to achieve production recall targets on full Core-6 ANN-Bench
