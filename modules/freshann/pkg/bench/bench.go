package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Profile string

const (
	ProfileCISmoke Profile = "ci_smoke"
	ProfileNightly Profile = "nightly"
)

type System string

const (
	SystemFreshANN System = "freshann"
	SystemMilvus   System = "milvus"
	SystemQdrant   System = "qdrant"
	SystemPgVector System = "pgvector"
)

type RunConfig struct {
	Profile Profile
	Dataset string
	TopK    int
}

type Result struct {
	System        System    `json:"system"`
	Dataset       string    `json:"dataset"`
	RecallAt10    float64   `json:"recall_at_10"`
	P95MS         float64   `json:"p95_ms"`
	P99MS         float64   `json:"p99_ms"`
	QPS           float64   `json:"qps"`
	QPSAtRecall90 float64   `json:"qps_at_recall_90"`
	CollectedAt   time.Time `json:"collected_at"`
}

type ComparisonReport struct {
	Profile Profile           `json:"profile"`
	Dataset string            `json:"dataset"`
	Results map[System]Result `json:"results"`
}

type Runner interface {
	Run(ctx context.Context, system System, cfg RunConfig) (Result, error)
}

type TierThreshold struct {
	MinRecallAt10    float64
	MaxP95MS         float64
	MaxP99MS         float64
	MinQPS           float64
	MinQPSAtRecall90 float64
}

func DefaultThresholds(p Profile) TierThreshold {
	switch p {
	case ProfileNightly:
		return TierThreshold{MinRecallAt10: 0.90, MaxP95MS: 80, MaxP99MS: 150, MinQPS: 250, MinQPSAtRecall90: 200}
	default:
		return TierThreshold{MinRecallAt10: 0.75, MaxP95MS: 150, MaxP99MS: 300, MinQPS: 50, MinQPSAtRecall90: 40}
	}
}

var Core6Datasets = []string{
	"glove-100-angular",
	"sift-128-euclidean",
	"fashion-mnist-784-euclidean",
	"nytimes-256-angular",
	"gist-960-euclidean",
	"glove-25-angular",
}

func RunComparison(ctx context.Context, r Runner, cfg RunConfig, systems []System) (ComparisonReport, error) {
	if cfg.Profile == "" {
		return ComparisonReport{}, fmt.Errorf("profile is required")
	}
	if cfg.Dataset == "" {
		return ComparisonReport{}, fmt.Errorf("dataset is required")
	}
	if len(systems) == 0 {
		systems = []System{SystemFreshANN}
	}
	res := ComparisonReport{Profile: cfg.Profile, Dataset: cfg.Dataset, Results: make(map[System]Result)}
	var mu sync.Mutex
	group, gctx := errgroup.WithContext(ctx)
	for _, system := range systems {
		sys := system
		group.Go(func() error {
			run, err := r.Run(gctx, sys, cfg)
			if err != nil {
				return err
			}
			run.System = sys
			run.Dataset = cfg.Dataset
			run.CollectedAt = time.Now().UTC()
			mu.Lock()
			res.Results[sys] = run
			mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return ComparisonReport{}, err
	}
	return res, nil
}

func CheckThreshold(report ComparisonReport, targetSystem System, th TierThreshold) error {
	r, ok := report.Results[targetSystem]
	if !ok {
		return fmt.Errorf("missing result for system %s", targetSystem)
	}
	if r.RecallAt10 < th.MinRecallAt10 {
		return fmt.Errorf("recall %.4f below threshold %.4f", r.RecallAt10, th.MinRecallAt10)
	}
	if r.P95MS > th.MaxP95MS {
		return fmt.Errorf("p95 %.2f exceeds threshold %.2f", r.P95MS, th.MaxP95MS)
	}
	if r.P99MS > th.MaxP99MS {
		return fmt.Errorf("p99 %.2f exceeds threshold %.2f", r.P99MS, th.MaxP99MS)
	}
	if r.QPS < th.MinQPS {
		return fmt.Errorf("qps %.2f below threshold %.2f", r.QPS, th.MinQPS)
	}
	if r.QPSAtRecall90 < th.MinQPSAtRecall90 {
		return fmt.Errorf("qps@recall90 %.2f below threshold %.2f", r.QPSAtRecall90, th.MinQPSAtRecall90)
	}
	return nil
}

func SaveReport(path string, report ComparisonReport) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func SystemsSorted(report ComparisonReport) []System {
	out := make([]System, 0, len(report.Results))
	for k := range report.Results {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
