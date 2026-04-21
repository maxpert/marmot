package vecindex

import "github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"

// SegmentGeneration is the active stable on-disk serving snapshot for one
// vector index epoch.
type SegmentGeneration struct {
	Data              *SegmentDataStore
	RowMap            *SegmentRowMap
	Centroids         *kmeans.CentroidSet
	AppliedOverlaySeq uint64
	LayoutHotClusters []int64
}

func (g *SegmentGeneration) Close() error {
	if g == nil {
		return nil
	}
	var firstErr error
	if g.Data != nil {
		if err := g.Data.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if g.RowMap != nil {
		if err := g.RowMap.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
