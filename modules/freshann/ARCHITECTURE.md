# freshann Architecture

`freshann` is a standalone disk-first ANN library with these core layers:

1. **Mutation durability + recovery**
2. **Pebble metadata and filter indexes**
3. **Segment snapshots + manifest lifecycle**
4. **Graph-assisted candidate search + exact rerank**
5. **Maintenance (repair/compaction/rebuild)**

## On-disk layout

`<root>/<index_id>/`

- `meta.pebble/`
  - index spec
  - applied token index (`txn_id`,`seq_id`)
  - watermark
  - vector records
  - partition/tag posting bitmaps
  - hashed-id to external-id map
  - graph metadata/startpoints/adjacency
- `manifest.json`
  - active segment pointer and version
- `segments/*.seg`
  - immutable vector snapshots

## Mutation flow

1. Validate request
2. Apply mutation to Pebble vector state
3. Update posting indexes (partition/tags)
4. Mark token as applied and advance watermark
5. Mark graph dirty and enqueue repair tasks for deletes

## Query flow

1. Resolve filter candidate universe via posting indexes (if provided)
2. Traverse graph for candidate IDs
3. Augment from filtered/unfiltered fallback scan under churn or sparse hits
4. Exact-score rerank (cosine/dot)
5. Return top-k

## Maintenance flow

- **Repair queue:** prunes deleted node references from graph
- **Compaction/rebuild:** materializes snapshot segment, updates manifest atomically, rebuilds graph
- Triggered on flush/close and mutation thresholds

## Verification

`Verify(deep=true)` checks:

- vector dimension consistency
- active segment readability
- graph state presence in Pebble when a segment is active
- graph edge references resolve to existing vectors

## Current constraints

- Input vectors: FP32
- Metrics: cosine, dot, euclidean
- Filters: partition + exact tags
- Graph build currently uses deterministic brute-force k-NN for correctness foundation
