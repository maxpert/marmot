package vecindex

import (
	"context"
	"errors"
)

// Graduate promotes an index to a larger Nlist tier by retraining centroids.
// targetNlist must not exceed MaxNlist.
func Graduate(ctx context.Context, idx *Index, targetNlist int) error {
	return errors.New("not implemented: Graduate")
}

// Retrain rebuilds all centroids for idx using the given seed and increments
// the epoch to epoch.
func Retrain(ctx context.Context, idx *Index, seed uint64, epoch uint64) error {
	return errors.New("not implemented: Retrain")
}

// CheckSplit decides whether cluster clusterID in idx has grown large enough
// to warrant a centroid split and triggers one if needed.
func CheckSplit(idx *Index, clusterID uint32) error {
	return errors.New("not implemented: CheckSplit")
}

// CheckMerge decides whether cluster clusterID in idx has shrunk below the
// merge threshold (0.25× mean size) and collapses it into a neighbour if so.
func CheckMerge(idx *Index, clusterID uint32) error {
	return errors.New("not implemented: CheckMerge")
}
