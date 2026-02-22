package api

import "context"

// Engine owns index lifecycle under a root path.
type Engine interface {
	CreateIndex(ctx context.Context, spec IndexSpec) (IndexHandle, error)
	OpenIndex(ctx context.Context, id IndexID) (Index, error)
	DropIndex(ctx context.Context, id IndexID) error
	ListIndexes(ctx context.Context) ([]IndexMeta, error)
	Close() error
}

// Index provides mutation and query operations for a single physical index.
type Index interface {
	Upsert(ctx context.Context, mut Mutation) (ApplyToken, error)
	Delete(ctx context.Context, mut DeleteMutation) (ApplyToken, error)
	Search(ctx context.Context, req SearchRequest) (SearchResult, error)
	WaitApplied(ctx context.Context, token ApplyToken) error
	Flush(ctx context.Context) error
	Snapshot(ctx context.Context, dst string) error
	Verify(ctx context.Context, opts VerifyOptions) (VerifyReport, error)
	Stats(ctx context.Context) (IndexStats, error)
	Close() error
}

type IndexID string

type IndexHandle struct {
	ID IndexID
}

type IndexMeta struct {
	ID   IndexID
	Spec IndexSpec
}

type Metric string

const (
	MetricCosine    Metric = "cosine"
	MetricDot       Metric = "dot"
	MetricEuclidean Metric = "euclidean"
)

type ApplyMode string

const (
	ApplyModeSync  ApplyMode = "sync"
	ApplyModeAsync ApplyMode = "async"
)

type DurabilityMode string

const (
	DurabilitySyncEveryCommit DurabilityMode = "sync_every_commit"
	DurabilityPeriodic        DurabilityMode = "periodic"
	DurabilityAsync           DurabilityMode = "async"
)

type QuantizerSpec struct {
	EnableFP16        bool
	PQSubQuantizers   int
	PQBitsPerCodebook int
	TrainingSeed      int64
}

type GraphSpec struct {
	R       int
	LBuild  int
	LSearch int
	Beam    int
}

type StorageSpec struct {
	PebbleCacheBytes int64
	VectorCacheBytes int64
	BloomBitsPerKey  int
	VectorBlockSize  int
	GraphPageSize    int
	PostingChunkSize int
	EnableSSTIngest  bool
}

type BudgetPolicyMode string

const (
	BudgetPolicyAdaptive BudgetPolicyMode = "adaptive"
	BudgetPolicyFixed    BudgetPolicyMode = "fixed"
)

type BudgetPolicySpec struct {
	Mode               BudgetPolicyMode
	TargetRecall       float64
	MinEfSearch        int
	MaxEfSearch        int
	MinBeam            int
	MaxBeam            int
	MinCandidateBudget int
	MaxCandidateBudget int
	MinRerankK         int
	MaxRerankK         int
}

type SearchTuning struct {
	EfSearch           int
	Beam               int
	CandidateBudget    int
	RerankK            int
	ShardWorkers       int
	TargetRecall       float64
	BudgetScale        float64
	AllowExactFallback bool
}

type FilterSpec struct {
	EnablePartitionKey bool
	EnableTags         bool
}

type IndexSpec struct {
	ID             IndexID
	Dim            int
	Metric         Metric
	ApplyMode      ApplyMode
	DurabilityMode DurabilityMode
	Quantizer      QuantizerSpec
	Graph          GraphSpec
	Storage        StorageSpec
	BudgetPolicy   BudgetPolicySpec
	SearchDefaults SearchTuning
	FilterSpec     FilterSpec
}

type Mutation struct {
	SeqID        uint64
	TxnID        uint64
	ExternalID   []byte
	VectorFP32   []float32
	PartitionKey string
	Tags         map[string]string
}

type DeleteMutation struct {
	SeqID        uint64
	TxnID        uint64
	ExternalID   []byte
	PartitionKey string
}

type ApplyToken struct {
	TxnID uint64
	SeqID uint64
}

type SearchRequest struct {
	VectorFP32   []float32
	TopK         int
	PartitionKey string
	Tags         map[string]string
	Tuning       SearchTuning
}

type SearchHit struct {
	ExternalID []byte
	Score      float32
	Distance   float32
}

type SearchResult struct {
	Hits []SearchHit
}

type VerifyOptions struct {
	Deep bool
}

type VerifyReport struct {
	Healthy bool
	Checks  []VerifyCheck
}

type VerifyCheck struct {
	Name    string
	OK      bool
	Details string
}

type IndexStats struct {
	VectorCount      uint64
	AppliedMutations uint64
	CurrentWatermark ApplyToken
	FallbackScans    uint64
	GraphPageReads   uint64
	VectorBlockReads uint64
	QPS90Last        float64
	RecallAt10Last   float64
}
