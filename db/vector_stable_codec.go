package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

const stableCodecTrainingSampleLimit = 32768
const stableCodecPQTrainingRowFloor = 4096

type stableCodecReservoir struct {
	rng        *rand.Rand
	seen       int64
	slots      int
	dim        int
	recordSize int64
	path       string
	file       *os.File
	err        error
}

func newStableCodecReservoir(seed uint64, dim int) (*stableCodecReservoir, error) {
	if dim <= 0 {
		return nil, fmt.Errorf("stable codec reservoir: invalid dim %d", dim)
	}
	if err := os.MkdirAll("/tmp/marmot", 0o755); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp("/tmp/marmot", "stable-codec-sample-*.pqtrain")
	if err != nil {
		return nil, err
	}
	return &stableCodecReservoir{
		rng:        rand.New(rand.NewSource(int64(seed))),
		dim:        dim,
		recordSize: int64(8 + dim*4),
		path:       file.Name(),
		file:       file,
	}, nil
}

func (r *stableCodecReservoir) Close() {
	if r == nil {
		return
	}
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	if r.path != "" {
		_ = os.Remove(r.path)
		r.path = ""
	}
}

func (r *stableCodecReservoir) Count() int {
	if r == nil {
		return 0
	}
	return r.slots
}

func (r *stableCodecReservoir) Add(clusterID int64, prepared []byte) {
	if r == nil || r.file == nil || clusterID <= 0 || len(prepared) != r.dim*4 {
		return
	}
	r.seen++
	if r.slots < stableCodecTrainingSampleLimit {
		if err := r.writeSlot(r.slots, clusterID, metric.BytesToFloat32(prepared)); err != nil {
			r.err = err
			return
		}
		r.slots++
		return
	}
	slot := r.rng.Int63n(r.seen)
	if slot >= int64(r.slots) {
		return
	}
	if err := r.writeSlot(int(slot), clusterID, metric.BytesToFloat32(prepared)); err != nil {
		r.err = err
	}
}

func (r *stableCodecReservoir) Samples() ([]vecindex.StableCodecTrainingVector, error) {
	if r == nil || r.file == nil || r.slots == 0 {
		return nil, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	if _, err := r.file.Seek(0, 0); err != nil {
		return nil, err
	}
	buf := make([]byte, r.recordSize)
	byCluster := make(map[int64][]vecindex.StableCodecTrainingVector)
	for slot := 0; slot < r.slots; slot++ {
		if _, err := r.file.ReadAt(buf, int64(slot)*r.recordSize); err != nil {
			return nil, err
		}
		clusterID := int64(binary.LittleEndian.Uint64(buf[:8]))
		vec := make([]float32, r.dim)
		cursor := 8
		for i := range vec {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[cursor : cursor+4]))
			cursor += 4
		}
		byCluster[clusterID] = append(byCluster[clusterID], vecindex.StableCodecTrainingVector{ClusterID: clusterID, Vec: vec})
	}
	out := make([]vecindex.StableCodecTrainingVector, 0, r.slots)
	clusterIDs := make([]int64, 0, len(byCluster))
	for clusterID := range byCluster {
		clusterIDs = append(clusterIDs, clusterID)
	}
	slices.Sort(clusterIDs)
	perClusterCap := stableCodecTrainingSampleLimit
	if len(clusterIDs) > 0 {
		perClusterCap = max(1, stableCodecTrainingSampleLimit/len(clusterIDs))
	}
	for _, clusterID := range clusterIDs {
		samples := byCluster[clusterID]
		if len(samples) > perClusterCap {
			samples = samples[:perClusterCap]
		}
		out = append(out, samples...)
	}
	return out, nil
}

func (r *stableCodecReservoir) writeSlot(slot int, clusterID int64, vec []float32) error {
	if len(vec) != r.dim {
		return fmt.Errorf("stable codec reservoir: vector dim=%d want=%d", len(vec), r.dim)
	}
	buf := make([]byte, r.recordSize)
	binary.LittleEndian.PutUint64(buf[:8], uint64(clusterID))
	cursor := 8
	for _, value := range vec {
		binary.LittleEndian.PutUint32(buf[cursor:cursor+4], math.Float32bits(value))
		cursor += 4
	}
	_, err := r.file.WriteAt(buf, int64(slot)*r.recordSize)
	return err
}

func buildStableMemberCodec(spec vecindex.IVFSpec, cs *kmeans.CentroidSet, reservoir *stableCodecReservoir) (*vecindex.StableMemberCodec, []byte, error) {
	var samples []vecindex.StableCodecTrainingVector
	if reservoir != nil && spec.InternalDim() >= 512 && reservoir.Count() >= stableCodecPQTrainingRowFloor {
		var err error
		samples, err = reservoir.Samples()
		if err != nil {
			return nil, nil, err
		}
	}
	codec, err := vecindex.BuildStableMemberCodec(spec, cs, samples, spec.Seed^cs.Epoch())
	if err != nil {
		return nil, nil, err
	}
	blob, err := vecindex.EncodeStableMemberCodecBlob(codec)
	if err != nil {
		return nil, nil, err
	}
	return codec, blob, nil
}
