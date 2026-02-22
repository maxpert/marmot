package verify

import (
	"fmt"

	"github.com/maxpert/marmot/modules/freshann/pkg/api"
	"github.com/maxpert/marmot/modules/freshann/pkg/segment"
	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
)

func RunBasic(spec api.IndexSpec, store *storage.IndexStore) (api.VerifyReport, error) {
	return RunComprehensive(spec, store, segment.Manifest{}, "", false)
}

func RunComprehensive(spec api.IndexSpec, store *storage.IndexStore, manifest segment.Manifest, indexDir string, deep bool) (api.VerifyReport, error) {
	report := api.VerifyReport{Healthy: true}

	var dimMismatch int
	err := store.IterateVectors(func(_ []byte, rec storage.VectorRecord) error {
		if len(rec.VectorFP32) != spec.Dim {
			dimMismatch++
		}
		return nil
	})
	if err != nil {
		return api.VerifyReport{}, err
	}
	checkDim := api.VerifyCheck{Name: "vector_dimension", OK: dimMismatch == 0}
	if dimMismatch > 0 {
		checkDim.Details = fmt.Sprintf("%d vectors with wrong dimension", dimMismatch)
		report.Healthy = false
	}
	report.Checks = append(report.Checks, checkDim)

	if manifest.ActiveSegment != "" {
		segmentRoot := "."
		if indexDir != "" {
			segmentRoot = indexDir
		}
		_, err := segment.ReadSnapshot(segmentRoot, manifest.ActiveSegment)
		ok := err == nil
		check := api.VerifyCheck{Name: "active_segment_readable", OK: ok}
		if err != nil {
			check.Details = err.Error()
			report.Healthy = false
		}
		report.Checks = append(report.Checks, check)
	}

	state, hasGraph, err := store.LoadGraphState()
	if err != nil {
		return api.VerifyReport{}, err
	}
	requireGraph := manifest.ActiveSegment != ""
	checkGraphPresent := api.VerifyCheck{Name: "graph_state_present", OK: hasGraph || !requireGraph}
	if requireGraph && !hasGraph {
		checkGraphPresent.Details = "graph state not found in metadata store"
		report.Healthy = false
	}
	report.Checks = append(report.Checks, checkGraphPresent)
	if hasGraph && deep {
		dangling := 0
		for src, nbs := range state.Adj {
			_, ok, err := store.GetVector([]byte(src))
			if err != nil {
				return api.VerifyReport{}, err
			}
			if !ok {
				dangling++
			}
			for _, dst := range nbs {
				_, ok, err := store.GetVector([]byte(dst))
				if err != nil {
					return api.VerifyReport{}, err
				}
				if !ok {
					dangling++
				}
			}
		}
		checkGraphRefs := api.VerifyCheck{Name: "graph_edges_resolve", OK: dangling == 0}
		if dangling > 0 {
			checkGraphRefs.Details = fmt.Sprintf("%d dangling graph references", dangling)
			report.Healthy = false
		}
		report.Checks = append(report.Checks, checkGraphRefs)
	}

	if deep {
		count, err := store.CountVectors()
		if err != nil {
			return api.VerifyReport{}, err
		}
		report.Checks = append(report.Checks, api.VerifyCheck{Name: "deep_vector_count", OK: count >= 0})
	}

	return report, nil
}
