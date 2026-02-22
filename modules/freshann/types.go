package freshann

import "github.com/maxpert/marmot/modules/freshann/pkg/api"

type (
	Engine           = api.Engine
	Index            = api.Index
	IndexID          = api.IndexID
	IndexHandle      = api.IndexHandle
	IndexMeta        = api.IndexMeta
	IndexSpec        = api.IndexSpec
	Metric           = api.Metric
	ApplyMode        = api.ApplyMode
	DurabilityMode   = api.DurabilityMode
	QuantizerSpec    = api.QuantizerSpec
	GraphSpec        = api.GraphSpec
	StorageSpec      = api.StorageSpec
	BudgetPolicyMode = api.BudgetPolicyMode
	BudgetPolicySpec = api.BudgetPolicySpec
	SearchTuning     = api.SearchTuning
	FilterSpec       = api.FilterSpec
	Mutation         = api.Mutation
	DeleteMutation   = api.DeleteMutation
	ApplyToken       = api.ApplyToken
	SearchRequest    = api.SearchRequest
	SearchResult     = api.SearchResult
	SearchHit        = api.SearchHit
	VerifyOptions    = api.VerifyOptions
	VerifyReport     = api.VerifyReport
	VerifyCheck      = api.VerifyCheck
	IndexStats       = api.IndexStats
)

const (
	MetricCosine    = api.MetricCosine
	MetricDot       = api.MetricDot
	MetricEuclidean = api.MetricEuclidean

	ApplyModeSync  = api.ApplyModeSync
	ApplyModeAsync = api.ApplyModeAsync

	DurabilitySyncEveryCommit = api.DurabilitySyncEveryCommit
	DurabilityPeriodic        = api.DurabilityPeriodic
	DurabilityAsync           = api.DurabilityAsync

	BudgetPolicyAdaptive = api.BudgetPolicyAdaptive
	BudgetPolicyFixed    = api.BudgetPolicyFixed
)

var (
	ErrInvalidSpec       = api.ErrInvalidSpec
	ErrInvalidMutation   = api.ErrInvalidMutation
	ErrIndexExists       = api.ErrIndexExists
	ErrIndexNotFound     = api.ErrIndexNotFound
	ErrClosed            = api.ErrClosed
	ErrNotAppliedYet     = api.ErrNotAppliedYet
	ErrUnsupportedMetric = api.ErrUnsupportedMetric
)
