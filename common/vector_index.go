package common

// VectorIndexMeta holds metadata for a vector index.
// This type is shared between the db and coordinator packages.
type VectorIndexMeta struct {
	IndexName  string
	TableName  string
	ColumnName string
	Database   string
	Metric     string
	Dim        int
	// Nlist is the number of IVF centroids. 0 means auto-tune at CREATE time.
	Nlist int
	// Nprobe is the number of centroids searched per query. 0 means auto-tune.
	Nprobe int
	// AutoTuneNlist records whether Nlist was user-supplied or auto-derived.
	// REINDEX recomputes auto-tuned values against the current corpus size.
	AutoTuneNlist bool
	// AutoTuneNprobe records whether Nprobe was user-supplied or auto-derived.
	AutoTuneNprobe bool
	// TargetPartitionSize is the desired average vectors/partition when
	// auto-tuning. Reserved for future tuning strategies; defaults to 100.
	TargetPartitionSize int
	// MaxNorm is the fixed L2 norm cap for dot-product MIPS→L2 augmentation.
	MaxNorm   float32
	Status    string // "building", "ready", "reindexing"
	CreatedAt int64
}

const (
	DefaultVectorTargetPartitionSize = 512
	VectorControlTrainerVersion      = uint32(1)
	VectorControlCodecVersion        = uint32(1)
)

type VectorIndexAction uint8

const (
	VectorIndexActionCreate     VectorIndexAction = 1
	VectorIndexActionDrop       VectorIndexAction = 2
	VectorIndexActionReindex    VectorIndexAction = 3
	VectorIndexActionCheckpoint VectorIndexAction = 4
)

func (a VectorIndexAction) String() string {
	switch a {
	case VectorIndexActionCreate:
		return "create"
	case VectorIndexActionDrop:
		return "drop"
	case VectorIndexActionReindex:
		return "reindex"
	case VectorIndexActionCheckpoint:
		return "checkpoint"
	default:
		return "unknown"
	}
}

// VectorIndexChange is the replicated control-plane payload for local vector
// index state. It intentionally carries metadata only; segment files, rowmaps,
// centroids, PQ codebooks, and overlay journals remain node-local derived data.
type VectorIndexChange struct {
	Action              VectorIndexAction `msgpack:"a"`
	Database            string            `msgpack:"db"`
	IndexName           string            `msgpack:"idx"`
	TableName           string            `msgpack:"tbl"`
	ColumnName          string            `msgpack:"col"`
	Metric              string            `msgpack:"m"`
	Dim                 int               `msgpack:"dim"`
	Nlist               int               `msgpack:"nl"`
	Nprobe              int               `msgpack:"np"`
	AutoTuneNlist       bool              `msgpack:"anl"`
	AutoTuneNprobe      bool              `msgpack:"anp"`
	TargetPartitionSize int               `msgpack:"tps"`
	MaxNorm             float32           `msgpack:"mn"`
	SourceProbeEpoch    uint64            `msgpack:"spe"`
	TargetProbeEpoch    uint64            `msgpack:"tpe"`
	CutoffTxnID         uint64            `msgpack:"ctxn"`
	CutoffSeqNum        uint64            `msgpack:"cseq"`
	TrainerVersion      uint32            `msgpack:"trv"`
	CodecVersion        uint32            `msgpack:"cv"`
	Seed                uint64            `msgpack:"seed"`
	CreatedAt           int64             `msgpack:"ca"`
}

func (c VectorIndexChange) Meta() VectorIndexMeta {
	return VectorIndexMeta{
		IndexName:           c.IndexName,
		TableName:           c.TableName,
		ColumnName:          c.ColumnName,
		Database:            c.Database,
		Metric:              c.Metric,
		Dim:                 c.Dim,
		Nlist:               c.Nlist,
		Nprobe:              c.Nprobe,
		AutoTuneNlist:       c.AutoTuneNlist,
		AutoTuneNprobe:      c.AutoTuneNprobe,
		TargetPartitionSize: c.TargetPartitionSize,
		MaxNorm:             c.MaxNorm,
		Status:              "building",
		CreatedAt:           c.CreatedAt,
	}
}
