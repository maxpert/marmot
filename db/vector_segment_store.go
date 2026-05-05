package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

type OpenedSegmentGeneration struct {
	Manifest        *vecindex.SegmentManifest
	Data            *vecindex.SegmentDataStore
	RowMap          *vecindex.SegmentRowMap
	Blocks          *vecindex.SegmentBlockMetaStore
	ProbeCentroids  *kmeans.CentroidSet
	StableCentroids *kmeans.CentroidSet
	StableCodec     *vecindex.StableMemberCodec
}

func (g *OpenedSegmentGeneration) TakeGeneration() *vecindex.SegmentGeneration {
	if g == nil || g.Manifest == nil {
		return nil
	}
	generation := &vecindex.SegmentGeneration{
		Data:                     g.Data,
		RowMap:                   g.RowMap,
		Blocks:                   g.Blocks,
		ProbeCentroids:           g.ProbeCentroids,
		StableCentroids:          g.StableCentroids,
		StableCodec:              g.StableCodec,
		AppliedOverlaySeq:        g.Manifest.AppliedOverlaySeq,
		ClusterRowCounts:         append([]uint64(nil), g.Manifest.ClusterRowCounts...),
		ClusterVectorSums:        cloneClusterVectorSums(g.Manifest.ClusterVectorSums),
		RowsModifiedSinceRebuild: g.Manifest.RowsModifiedSinceRebuild,
		LastRebuildRowCount:      g.Manifest.LastRebuildRowCount,
		ConsecutiveSkewCycles:    g.Manifest.ConsecutiveSkewCycles,
		LayoutHotClusters:        int64Slice(g.Manifest.LayoutHotClusters),
	}
	g.Data = nil
	g.RowMap = nil
	g.Blocks = nil
	return generation
}

func segmentGenerationFromOpened(opened *OpenedSegmentGeneration) *vecindex.SegmentGeneration {
	return opened.TakeGeneration()
}

const segmentLayoutSeed uint64 = 0x9e3779b97f4a7c15
const segmentLayoutHotClusterLimit = 64

func (g *OpenedSegmentGeneration) Close() error {
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
	if g.Blocks != nil {
		if err := g.Blocks.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type segmentGenerationArtifacts struct {
	dataPath   string
	rowMapPath string
	blockPath  string
}

type segmentGenerationStaging struct {
	dir       string
	artifacts segmentGenerationArtifacts
}

func (s *segmentGenerationStaging) cleanup() {
	if s == nil || s.dir == "" {
		return
	}
	_ = os.RemoveAll(s.dir)
}

func publishSegmentGeneration(dir string, manifest vecindex.SegmentManifest, artifacts segmentGenerationArtifacts) error {
	dataTmpPath := artifacts.dataPath
	rowMapTmpPath := artifacts.rowMapPath
	blockTmpPath := artifacts.blockPath
	if dataTmpPath == "" || rowMapTmpPath == "" {
		return fmt.Errorf("segment publish: temp paths must not be empty")
	}
	if err := validatePublishableSegmentManifest(&manifest); err != nil {
		return fmt.Errorf("segment publish: invalid manifest: %w", err)
	}

	dataInfo, err := os.Stat(dataTmpPath)
	if err != nil {
		return fmt.Errorf("segment publish: stat temp data: %w", err)
	}
	if dataInfo.IsDir() {
		return fmt.Errorf("segment publish: temp data path is a directory")
	}
	rowMapInfo, err := os.Stat(rowMapTmpPath)
	if err != nil {
		return fmt.Errorf("segment publish: stat temp rowmap: %w", err)
	}
	if rowMapInfo.IsDir() {
		return fmt.Errorf("segment publish: temp rowmap path is a directory")
	}

	dataDir := filepath.Join(dir, "segments")
	rowMapDir := filepath.Join(dir, "rowmap")
	blockDir := filepath.Join(dir, "blocks")
	manifestDir := filepath.Join(dir, "manifest")
	for _, path := range []string{dataDir, rowMapDir, blockDir, manifestDir} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
	}

	finalDataPath := vecindex.SegmentDataPath(dir, manifest.Generation)
	finalRowMapPath := vecindex.SegmentRowMapPath(dir, manifest.Generation)
	manifest.DataFile = filepath.Base(finalDataPath)
	manifest.RowMapFile = filepath.Base(finalRowMapPath)
	if blockTmpPath != "" {
		finalBlockPath := vecindex.SegmentBlockPath(dir, manifest.Generation)
		manifest.BlockMetaFile = filepath.Base(finalBlockPath)
		if manifest.BlockRows == 0 {
			return fmt.Errorf("segment publish: block rows missing")
		}
	}

	if manifest.DataFileSize != 0 && manifest.DataFileSize != uint64(dataInfo.Size()) {
		return fmt.Errorf("segment publish: data size mismatch: temp=%d manifest=%d", dataInfo.Size(), manifest.DataFileSize)
	}
	if manifest.DataFileSize == 0 {
		manifest.DataFileSize = uint64(dataInfo.Size())
	}
	if manifest.RowMapFileSize != 0 && manifest.RowMapFileSize != uint64(rowMapInfo.Size()) {
		return fmt.Errorf("segment publish: rowmap size mismatch: temp=%d manifest=%d", rowMapInfo.Size(), manifest.RowMapFileSize)
	}
	if manifest.RowMapFileSize == 0 {
		manifest.RowMapFileSize = uint64(rowMapInfo.Size())
	}
	var blockInfo os.FileInfo
	if blockTmpPath != "" {
		var err error
		blockInfo, err = os.Stat(blockTmpPath)
		if err != nil {
			return fmt.Errorf("segment publish: stat temp blocks: %w", err)
		}
		if blockInfo.IsDir() {
			return fmt.Errorf("segment publish: temp blocks path is a directory")
		}
		if manifest.BlockMetaFileSize != 0 && manifest.BlockMetaFileSize != uint64(blockInfo.Size()) {
			return fmt.Errorf("segment publish: block size mismatch: temp=%d manifest=%d", blockInfo.Size(), manifest.BlockMetaFileSize)
		}
		if manifest.BlockMetaFileSize == 0 {
			manifest.BlockMetaFileSize = uint64(blockInfo.Size())
		}
	}

	dataHash, err := sha256File(dataTmpPath)
	if err != nil {
		return fmt.Errorf("segment publish: hash temp data: %w", err)
	}
	if manifest.DataFileSHA256 != "" && !strings.EqualFold(manifest.DataFileSHA256, dataHash) {
		return fmt.Errorf("segment publish: data checksum mismatch")
	}
	manifest.DataFileSHA256 = dataHash

	rowMapHash, err := sha256File(rowMapTmpPath)
	if err != nil {
		return fmt.Errorf("segment publish: hash temp rowmap: %w", err)
	}
	if manifest.RowMapFileSHA256 != "" && !strings.EqualFold(manifest.RowMapFileSHA256, rowMapHash) {
		return fmt.Errorf("segment publish: rowmap checksum mismatch")
	}
	manifest.RowMapFileSHA256 = rowMapHash
	if blockTmpPath != "" {
		blockHash, err := sha256File(blockTmpPath)
		if err != nil {
			return fmt.Errorf("segment publish: hash temp blocks: %w", err)
		}
		if manifest.BlockMetaFileSHA256 != "" && !strings.EqualFold(manifest.BlockMetaFileSHA256, blockHash) {
			return fmt.Errorf("segment publish: block checksum mismatch")
		}
		manifest.BlockMetaFileSHA256 = blockHash
	}

	if err := syncFile(dataTmpPath); err != nil {
		return fmt.Errorf("segment publish: sync temp data: %w", err)
	}
	if err := os.Rename(dataTmpPath, finalDataPath); err != nil {
		return fmt.Errorf("segment publish: rename data: %w", err)
	}
	if err := syncDir(dataDir); err != nil {
		return fmt.Errorf("segment publish: sync data dir: %w", err)
	}

	if err := syncFile(rowMapTmpPath); err != nil {
		return fmt.Errorf("segment publish: sync temp rowmap: %w", err)
	}
	if err := os.Rename(rowMapTmpPath, finalRowMapPath); err != nil {
		return fmt.Errorf("segment publish: rename rowmap: %w", err)
	}
	if err := syncDir(rowMapDir); err != nil {
		return fmt.Errorf("segment publish: sync rowmap dir: %w", err)
	}
	if blockTmpPath != "" {
		finalBlockPath := vecindex.SegmentBlockPath(dir, manifest.Generation)
		if err := syncFile(blockTmpPath); err != nil {
			return fmt.Errorf("segment publish: sync temp blocks: %w", err)
		}
		if err := os.Rename(blockTmpPath, finalBlockPath); err != nil {
			return fmt.Errorf("segment publish: rename blocks: %w", err)
		}
		if err := syncDir(blockDir); err != nil {
			return fmt.Errorf("segment publish: sync blocks dir: %w", err)
		}
	}

	manifestPath := vecindex.SegmentManifestPath(dir, manifest.Generation)
	manifestTmpPath := manifestPath + ".tmp"
	manifestBlob, err := vecindex.EncodeSegmentManifest(&manifest)
	if err != nil {
		return fmt.Errorf("segment publish: encode manifest: %w", err)
	}
	if err := writeDurableFile(manifestTmpPath, manifestBlob); err != nil {
		return fmt.Errorf("segment publish: write manifest: %w", err)
	}
	if err := os.Rename(manifestTmpPath, manifestPath); err != nil {
		return fmt.Errorf("segment publish: rename manifest: %w", err)
	}
	if err := syncDir(manifestDir); err != nil {
		return fmt.Errorf("segment publish: sync manifest dir: %w", err)
	}

	current := vecindex.SegmentCurrent{
		Version:      vecindex.SegmentStoreVersion,
		Generation:   manifest.Generation,
		ManifestFile: filepath.Base(manifestPath),
	}
	currentBlob, err := vecindex.EncodeSegmentCurrent(&current)
	if err != nil {
		return fmt.Errorf("segment publish: encode current: %w", err)
	}
	currentPath := vecindex.SegmentCurrentPath(dir)
	currentTmpPath := currentPath + ".tmp"
	if err := writeDurableFile(currentTmpPath, currentBlob); err != nil {
		return fmt.Errorf("segment publish: write current: %w", err)
	}
	if err := os.Rename(currentTmpPath, currentPath); err != nil {
		return fmt.Errorf("segment publish: rename current: %w", err)
	}
	if err := syncDir(manifestDir); err != nil {
		return fmt.Errorf("segment publish: final sync manifest dir: %w", err)
	}
	return nil
}

func openSegmentGeneration(dir string, meta common.VectorIndexMeta, spec vecindex.IVFSpec, expectedEpoch uint64) (*OpenedSegmentGeneration, error) {
	_, manifest, err := loadCurrentManifest(dir)
	if err != nil || manifest == nil {
		return nil, err
	}
	if err := validateSegmentManifest(manifest, meta, spec, expectedEpoch); err != nil {
		return nil, err
	}

	dataPath := filepath.Join(dir, "segments", manifest.DataFile)
	if err := validateSegmentFile(dataPath, manifest.DataFileSize, manifest.DataFileSHA256); err != nil {
		return nil, err
	}
	rowMapPath := filepath.Join(dir, "rowmap", manifest.RowMapFile)
	if err := validateSegmentFile(rowMapPath, manifest.RowMapFileSize, manifest.RowMapFileSHA256); err != nil {
		return nil, err
	}
	var blockStore *vecindex.SegmentBlockMetaStore
	if manifest.BlockMetaFile != "" {
		blockPath := filepath.Join(dir, "blocks", manifest.BlockMetaFile)
		if err := validateSegmentFile(blockPath, manifest.BlockMetaFileSize, manifest.BlockMetaFileSHA256); err != nil {
			return nil, err
		}
		blockStore, err = vecindex.OpenSegmentBlockMetaStore(blockPath)
		if err != nil {
			return nil, err
		}
	}
	manifest.NormalizeCentroidFields()

	probeCentroids, err := vecindex.DecodeCentroidBlob(manifest.ProbeBlobValue())
	if err != nil {
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, fmt.Errorf("segment store decode probe centroids: %w", err)
	}
	stableCentroids, err := vecindex.DecodeCentroidBlob(manifest.StableBlobValue())
	if err != nil {
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, fmt.Errorf("segment store decode stable centroids: %w", err)
	}

	dataStore, err := vecindex.OpenSegmentDataStore(dataPath)
	if err != nil {
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, err
	}
	stableCodec, err := vecindex.DecodeStableMemberCodecBlob(spec, stableCentroids, dataStore.Encoding(), manifest.StableMemberCodecBlob)
	if err != nil {
		_ = dataStore.Close()
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, fmt.Errorf("segment store decode stable codec: %w", err)
	}
	if stableCodec.Encoding() != dataStore.Encoding() || stableCodec.EncodedSize() != dataStore.VecBytes() {
		_ = dataStore.Close()
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, fmt.Errorf("segment store stable codec/header mismatch")
	}
	rowMap, err := vecindex.OpenSegmentRowMap(rowMapPath)
	if err != nil {
		_ = dataStore.Close()
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, err
	}
	if err := validateOpenedSegmentGeneration(manifest, dataStore, rowMap, blockStore, probeCentroids, stableCentroids, meta, spec, expectedEpoch); err != nil {
		_ = dataStore.Close()
		_ = rowMap.Close()
		if blockStore != nil {
			_ = blockStore.Close()
		}
		return nil, err
	}
	return &OpenedSegmentGeneration{
		Manifest:        manifest,
		Data:            dataStore,
		RowMap:          rowMap,
		Blocks:          blockStore,
		ProbeCentroids:  probeCentroids,
		StableCentroids: stableCentroids,
		StableCodec:     stableCodec,
	}, nil
}

func loadCurrentManifest(dir string) (*vecindex.SegmentCurrent, *vecindex.SegmentManifest, error) {
	currentPath := vecindex.SegmentCurrentPath(dir)
	currentBlob, err := os.ReadFile(currentPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	current, err := vecindex.DecodeSegmentCurrent(currentBlob)
	if err != nil {
		return nil, nil, err
	}
	if current.Version != vecindex.SegmentStoreVersion &&
		current.Version != vecindex.SegmentStoreV3Compat() &&
		current.Version != vecindex.SegmentStoreV2Compat() &&
		current.Version != vecindex.SegmentStoreV1Compat() {
		return nil, nil, fmt.Errorf("segment current version %d unsupported", current.Version)
	}
	if !isSafeSegmentFile(current.ManifestFile) {
		return nil, nil, fmt.Errorf("segment current manifest filename is unsafe")
	}
	manifestBlob, err := os.ReadFile(filepath.Join(dir, "manifest", current.ManifestFile))
	if err != nil {
		return nil, nil, err
	}
	manifest, err := vecindex.DecodeSegmentManifest(manifestBlob)
	if err != nil {
		return nil, nil, err
	}
	manifest.NormalizeCentroidFields()
	if current.Generation != manifest.Generation {
		return nil, nil, fmt.Errorf("segment current generation mismatch")
	}
	return current, manifest, nil
}

func validateSegmentManifest(manifest *vecindex.SegmentManifest, meta common.VectorIndexMeta, spec vecindex.IVFSpec, expectedEpoch uint64) error {
	if manifest == nil {
		return fmt.Errorf("segment manifest missing")
	}
	if manifest.Version != vecindex.SegmentStoreVersion &&
		manifest.Version != vecindex.SegmentStoreV3Compat() &&
		manifest.Version != vecindex.SegmentStoreV2Compat() &&
		manifest.Version != vecindex.SegmentStoreV1Compat() {
		return fmt.Errorf("segment manifest version %d unsupported", manifest.Version)
	}
	manifest.NormalizeCentroidFields()
	if manifest.Database != meta.Database {
		return fmt.Errorf("segment manifest database mismatch")
	}
	if manifest.IndexName != meta.IndexName {
		return fmt.Errorf("segment manifest index name mismatch")
	}
	if manifest.IndexCreatedAt != meta.CreatedAt {
		return fmt.Errorf("segment manifest index identity mismatch")
	}
	if manifest.Metric != meta.Metric {
		return fmt.Errorf("segment manifest metric mismatch")
	}
	if manifest.Dim != uint32(meta.Dim) || manifest.InternalDim != uint32(spec.InternalDim()) {
		return fmt.Errorf("segment manifest dimension mismatch")
	}
	if manifest.ProbeEpochValue() == 0 {
		return fmt.Errorf("segment manifest probe centroid epoch missing")
	}
	if len(manifest.ProbeBlobValue()) == 0 {
		return fmt.Errorf("segment manifest probe centroid blob missing")
	}
	if manifest.StableEpochValue() == 0 {
		return fmt.Errorf("segment manifest stable centroid epoch missing")
	}
	if len(manifest.StableBlobValue()) == 0 {
		return fmt.Errorf("segment manifest stable centroid blob missing")
	}
	if expectedEpoch != 0 && manifest.ProbeEpochValue() != expectedEpoch {
		return fmt.Errorf("segment manifest probe centroid epoch mismatch")
	}
	if manifest.Generation == 0 {
		return fmt.Errorf("segment manifest generation missing")
	}
	if !isSafeSegmentFile(manifest.DataFile) {
		return fmt.Errorf("segment manifest data filename is unsafe")
	}
	if !isSafeSegmentFile(manifest.RowMapFile) {
		return fmt.Errorf("segment manifest rowmap filename is unsafe")
	}
	if manifest.BlockMetaFile != "" && !isSafeSegmentFile(manifest.BlockMetaFile) {
		return fmt.Errorf("segment manifest block filename is unsafe")
	}
	if manifest.DataFileSize == 0 || manifest.RowMapFileSize == 0 {
		return fmt.Errorf("segment manifest file sizes missing")
	}
	if manifest.DataFileSHA256 == "" || manifest.RowMapFileSHA256 == "" {
		return fmt.Errorf("segment manifest file checksums missing")
	}
	if manifest.BlockMetaFile != "" && (manifest.BlockMetaFileSize == 0 || manifest.BlockMetaFileSHA256 == "") {
		return fmt.Errorf("segment manifest block metadata incomplete")
	}
	return nil
}

func validatePublishableSegmentManifest(manifest *vecindex.SegmentManifest) error {
	if manifest == nil {
		return fmt.Errorf("manifest missing")
	}
	if manifest.Version != vecindex.SegmentStoreVersion {
		return fmt.Errorf("unsupported version %d", manifest.Version)
	}
	if manifest.Database == "" || manifest.IndexName == "" || manifest.Metric == "" {
		return fmt.Errorf("missing identity fields")
	}
	if manifest.IndexCreatedAt == 0 {
		return fmt.Errorf("missing index identity")
	}
	if manifest.Dim == 0 || manifest.InternalDim == 0 {
		return fmt.Errorf("missing dimensions")
	}
	if manifest.ProbeEpochValue() == 0 {
		return fmt.Errorf("missing probe centroid epoch")
	}
	if len(manifest.ProbeBlobValue()) == 0 {
		return fmt.Errorf("missing probe centroid blob")
	}
	if manifest.StableEpochValue() == 0 {
		return fmt.Errorf("missing stable centroid epoch")
	}
	if len(manifest.StableBlobValue()) == 0 {
		return fmt.Errorf("missing stable centroid blob")
	}
	if manifest.Generation == 0 {
		return fmt.Errorf("missing generation")
	}
	if manifest.MaxCluster == 0 {
		return fmt.Errorf("missing max cluster")
	}
	if manifest.RowCount == 0 {
		return fmt.Errorf("missing row count")
	}
	if len(manifest.ClusterRowCounts) > 0 && len(manifest.ClusterRowCounts) != int(manifest.MaxCluster)+1 {
		return fmt.Errorf("cluster row count metadata length mismatch")
	}
	if len(manifest.ClusterVectorSums) > 0 && len(manifest.ClusterVectorSums) != int(manifest.MaxCluster)+1 {
		return fmt.Errorf("cluster vector sums metadata length mismatch")
	}
	if manifest.BlockMetaFile != "" {
		if manifest.BlockMetaFileSize == 0 || manifest.BlockMetaFileSHA256 == "" || manifest.BlockRows == 0 {
			return fmt.Errorf("block metadata fields incomplete")
		}
	}
	return nil
}

func validateOpenedSegmentGeneration(
	manifest *vecindex.SegmentManifest,
	dataStore *vecindex.SegmentDataStore,
	rowMap *vecindex.SegmentRowMap,
	blockStore *vecindex.SegmentBlockMetaStore,
	probeCentroids *kmeans.CentroidSet,
	stableCentroids *kmeans.CentroidSet,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	expectedEpoch uint64,
) error {
	if dataStore == nil || rowMap == nil || probeCentroids == nil || stableCentroids == nil {
		return fmt.Errorf("segment store missing opened files")
	}
	manifest.NormalizeCentroidFields()
	if dataStore.Generation() != manifest.Generation || rowMap.Generation() != manifest.Generation {
		return fmt.Errorf("segment store header generation mismatch")
	}
	if blockStore != nil && blockStore.Generation() != manifest.Generation {
		return fmt.Errorf("segment block header generation mismatch")
	}
	if dataStore.Epoch() != manifest.StableEpochValue() || rowMap.Epoch() != manifest.StableEpochValue() {
		return fmt.Errorf("segment store header epoch mismatch")
	}
	if blockStore != nil && blockStore.Epoch() != manifest.StableEpochValue() {
		return fmt.Errorf("segment block header epoch mismatch")
	}
	if expectedEpoch != 0 && manifest.ProbeEpochValue() != expectedEpoch {
		return fmt.Errorf("segment probe expected epoch mismatch")
	}
	if probeCentroids.Epoch() != manifest.ProbeEpochValue() {
		return fmt.Errorf("segment probe centroid epoch mismatch")
	}
	if stableCentroids.Epoch() != manifest.StableEpochValue() {
		return fmt.Errorf("segment stable centroid epoch mismatch")
	}
	if dataStore.Dim() != meta.Dim || dataStore.InternalDim() != spec.InternalDim() {
		return fmt.Errorf("segment store header dimension mismatch")
	}
	if probeCentroids.Len() != int(manifest.MaxCluster) {
		return fmt.Errorf("segment probe centroid count mismatch")
	}
	if stableCentroids.Len() != int(manifest.MaxCluster) {
		return fmt.Errorf("segment stable centroid count mismatch")
	}
	if int(manifest.MaxCluster) != dataStore.MaxCluster() {
		return fmt.Errorf("segment store cluster count mismatch")
	}
	if blockStore != nil {
		if blockStore.Metric() != dataStore.Metric() {
			return fmt.Errorf("segment block metric mismatch")
		}
		if blockStore.Encoding() != dataStore.Encoding() {
			return fmt.Errorf("segment block encoding mismatch")
		}
		if blockStore.Dim() != dataStore.Dim() || blockStore.InternalDim() != dataStore.InternalDim() {
			return fmt.Errorf("segment block dimension mismatch")
		}
		if blockStore.MaxCluster() != dataStore.MaxCluster() {
			return fmt.Errorf("segment block cluster count mismatch")
		}
		if manifest.BlockRows != 0 && uint32(blockStore.BlockRows()) != manifest.BlockRows {
			return fmt.Errorf("segment block rows mismatch")
		}
		if err := blockStore.ValidateCoverage(dataStore); err != nil {
			return fmt.Errorf("segment block coverage mismatch: %w", err)
		}
	}
	if manifest.RowCount != dataStore.RowCount() || manifest.RowCount != rowMap.EntryCount() {
		return fmt.Errorf("segment store row count mismatch")
	}
	if len(manifest.ClusterRowCounts) > 0 {
		for clusterID := 1; clusterID <= dataStore.MaxCluster(); clusterID++ {
			if got, want := dataStore.ClusterCount(int64(clusterID)), manifest.ClusterRowCounts[clusterID]; got != want {
				return fmt.Errorf("segment cluster %d row count mismatch", clusterID)
			}
		}
	}
	if len(manifest.ClusterVectorSums) > 0 {
		for clusterID := 1; clusterID <= dataStore.MaxCluster(); clusterID++ {
			sumLen := len(manifest.ClusterVectorSums[clusterID])
			rowCount := uint64(0)
			if len(manifest.ClusterRowCounts) > clusterID {
				rowCount = manifest.ClusterRowCounts[clusterID]
			}
			if rowCount == 0 {
				if sumLen != 0 && sumLen != spec.InternalDim() {
					return fmt.Errorf("segment cluster %d vector-sum dim mismatch", clusterID)
				}
				continue
			}
			if sumLen != spec.InternalDim() {
				return fmt.Errorf("segment cluster %d vector-sum dim mismatch", clusterID)
			}
		}
	}
	if dataStore.Metric() != spec.InternalMetric() {
		return fmt.Errorf("segment store header metric mismatch")
	}
	return nil
}

func validateSegmentFile(path string, wantSize uint64, wantSHA256 string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("segment store path is a directory")
	}
	if uint64(info.Size()) != wantSize {
		return fmt.Errorf("segment store size mismatch: got=%d want=%d", info.Size(), wantSize)
	}
	hash, err := sha256File(path)
	if err != nil {
		return err
	}
	if !strings.EqualFold(hash, wantSHA256) {
		return fmt.Errorf("segment store checksum mismatch")
	}
	return nil
}

func isSafeSegmentFile(name string) bool {
	return name != "" && filepath.Base(name) == name && !strings.Contains(name, string(filepath.Separator))
}

func writeDurableFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func metricStringFromEnum(m vecindex.Metric) string {
	switch m {
	case vecindex.MetricL2:
		return "l2"
	case vecindex.MetricDot:
		return "dot"
	case vecindex.MetricCosine:
		return "cosine"
	default:
		return ""
	}
}

func mustCentroidBlob(cs *kmeans.CentroidSet) []byte {
	blob, err := vecindex.EncodeCentroidBlob(cs)
	if err != nil {
		panic(fmt.Sprintf("encode centroid blob: %v", err))
	}
	return blob
}

func nextSegmentGeneration(dir string) (uint64, error) {
	current, manifest, err := loadCurrentManifest(dir)
	if err != nil {
		return 0, err
	}
	if current == nil || manifest == nil {
		return 1, nil
	}
	return manifest.Generation + 1, nil
}

func createSegmentGenerationStaging(dir string, generation uint64) (*segmentGenerationStaging, error) {
	stagingDir := filepath.Join(dir, "staging", fmt.Sprintf("gen-%020d-%d", generation, time.Now().UnixNano()))
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return nil, err
	}
	return &segmentGenerationStaging{
		dir: stagingDir,
		artifacts: segmentGenerationArtifacts{
			dataPath:   filepath.Join(stagingDir, filepath.Base(vecindex.SegmentDataPath(dir, generation))),
			rowMapPath: filepath.Join(stagingDir, filepath.Base(vecindex.SegmentRowMapPath(dir, generation))),
			blockPath:  filepath.Join(stagingDir, filepath.Base(vecindex.SegmentBlockPath(dir, generation))),
		},
	}, nil
}

func RebuildSegmentGeneration(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
	cs *kmeans.CentroidSet,
	appliedOverlaySeq uint64,
	hotClusterScores map[int64]uint64,
) (*vecindex.SegmentGeneration, error) {
	if db == nil {
		return nil, fmt.Errorf("segment generation rebuild: db is nil")
	}
	if cs == nil || cs.Epoch() == 0 {
		return nil, fmt.Errorf("segment generation rebuild: centroid set is nil")
	}
	expectedEpoch := cs.Epoch()
	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	generation, err := nextSegmentGeneration(dir)
	if err != nil {
		return nil, fmt.Errorf("segment generation rebuild: load current generation: %w", err)
	}

	maxCluster := cs.Len()
	if maxCluster == 0 {
		return nil, nil
	}

	staging, err := createSegmentGenerationStaging(dir, generation)
	if err != nil {
		return nil, fmt.Errorf("segment generation rebuild: create staging: %w", err)
	}
	defer staging.cleanup()
	rowMapWriter, err := vecindex.CreateSegmentRowMapWriter(staging.artifacts.rowMapPath, expectedEpoch, generation)
	if err != nil {
		return nil, err
	}
	defer rowMapWriter.Abort()

	sqlText := fmt.Sprintf(`
SELECT rowid, %s
  FROM %s
 WHERE %s IS NOT NULL
 ORDER BY rowid`,
		quoteIdent(meta.ColumnName),
		quoteIdent(meta.TableName),
		quoteIdent(meta.ColumnName),
	)
	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("segment generation rebuild: query rows: %w", err)
	}
	defer rows.Close()

	type rowLoc struct {
		rowID     int64
		clusterID int64
		offset    uint64
	}
	rowLocs := make([]rowLoc, 0, 1024)
	clusterRowCounts := make([]uint64, maxCluster+1)
	clusterVectorSums := make([][]float32, maxCluster+1)
	var codecReservoir *stableCodecReservoir
	if spec.InternalDim() >= vecindex.StablePQMinInternalDim {
		codecReservoir, err = newStableCodecReservoir(spec.Seed^expectedEpoch, spec.InternalDim())
		if err != nil {
			return nil, fmt.Errorf("segment generation rebuild: stable codec reservoir: %w", err)
		}
		defer codecReservoir.Close()
	}
	preparedEntrySize := 8 + spec.InternalDim()*4
	type clusterSpool struct {
		path string
		file *os.File
	}
	clusterSpools := make(map[int64]*clusterSpool, maxCluster)
	defer func() {
		for _, spool := range clusterSpools {
			if spool == nil {
				continue
			}
			if spool.file != nil {
				_ = spool.file.Close()
			}
			if spool.path != "" {
				_ = os.Remove(spool.path)
			}
		}
	}()
	var rowCount uint64
	for rows.Next() {
		var rowID int64
		var raw []byte
		if err := rows.Scan(&rowID, &raw); err != nil {
			return nil, fmt.Errorf("segment generation rebuild: scan row: %w", err)
		}
		prepared, err := materializeVectorBlob(raw, spec.Metric, spec.Dim, spec.MaxNorm)
		if err != nil {
			return nil, fmt.Errorf("segment generation rebuild: materialize rowid %d: %w", rowID, err)
		}
		if prepared == nil {
			continue
		}
		clusterID, err := assignPreparedAgainstSet(prepared, spec, cs)
		if err != nil {
			return nil, fmt.Errorf("segment generation rebuild: assign rowid %d: %w", rowID, err)
		}
		if clusterVectorSums[clusterID] == nil {
			clusterVectorSums[clusterID] = make([]float32, spec.InternalDim())
		}
		preparedVec := metric.BytesToFloat32(prepared)
		for i, value := range preparedVec {
			clusterVectorSums[clusterID][i] += value
		}
		if codecReservoir != nil {
			codecReservoir.Add(clusterID, prepared)
		}
		spool := clusterSpools[clusterID]
		if spool == nil {
			tmp, err := os.CreateTemp(dir, fmt.Sprintf("cluster-%06d-*.segrows", clusterID))
			if err != nil {
				return nil, fmt.Errorf("segment generation rebuild: create cluster spool: %w", err)
			}
			spool = &clusterSpool{path: tmp.Name(), file: tmp}
			clusterSpools[clusterID] = spool
		}
		var rowidBuf [8]byte
		binary.LittleEndian.PutUint64(rowidBuf[:], uint64(rowID))
		if _, err := spool.file.Write(rowidBuf[:]); err != nil {
			return nil, fmt.Errorf("segment generation rebuild: write spool rowid: %w", err)
		}
		if _, err := spool.file.Write(prepared); err != nil {
			return nil, fmt.Errorf("segment generation rebuild: write spool vec: %w", err)
		}
		clusterRowCounts[clusterID]++
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("segment generation rebuild: iterate rows: %w", err)
	}
	if rowCount == 0 {
		return nil, nil
	}
	stableCodec, stableCodecBlob, err := buildStableMemberCodec(spec, cs, codecReservoir)
	if err != nil {
		return nil, fmt.Errorf("segment generation rebuild: build stable codec: %w", err)
	}
	dataWriter, err := vecindex.CreateSegmentDataWriter(
		staging.artifacts.dataPath,
		spec.InternalMetric(),
		stableCodec.Encoding(),
		spec.Dim,
		spec.InternalDim(),
		stableCodec.EncodedSize(),
		maxCluster,
		expectedEpoch,
		generation,
	)
	if err != nil {
		return nil, err
	}
	defer dataWriter.Abort()
	blockWriter, err := vecindex.CreateSegmentBlockMetaWriter(
		staging.artifacts.blockPath,
		spec,
		stableCodec,
		vecindex.DefaultSegmentBlockRows(stableCodec.Encoding()),
		maxCluster,
		expectedEpoch,
		generation,
	)
	if err != nil {
		return nil, err
	}
	defer blockWriter.Abort()
	layoutHotClusters := orderedHotClusterIDs(hotClusterScores, segmentLayoutHotClusterLimit)
	buf := make([]byte, preparedEntrySize*256)
	for _, clusterID := range segmentClusterWriteOrder(spec, cs, hotClusterScores) {
		spool := clusterSpools[clusterID]
		if spool == nil {
			continue
		}
		if _, err := spool.file.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("segment generation rebuild: rewind spool: %w", err)
		}
		for {
			n, err := io.ReadFull(spool.file, buf)
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				if n == 0 {
					break
				}
				if n%preparedEntrySize != 0 {
					return nil, fmt.Errorf("segment generation rebuild: truncated cluster spool")
				}
			} else if err != nil {
				return nil, fmt.Errorf("segment generation rebuild: read cluster spool: %w", err)
			}
			for cursor := 0; cursor < n; cursor += preparedEntrySize {
				rowID := int64(binary.LittleEndian.Uint64(buf[cursor : cursor+8]))
				prepared := buf[cursor+8 : cursor+preparedEntrySize]
				enc, encoded, err := stableCodec.Encode(clusterID, prepared)
				if err != nil {
					return nil, fmt.Errorf("segment generation rebuild: encode rowid %d: %w", rowID, err)
				}
				if enc != stableCodec.Encoding() {
					return nil, fmt.Errorf("segment generation rebuild: unexpected stable encoding %d for rowid %d", enc, rowID)
				}
				offset := dataWriter.NextOffset()
				if err := dataWriter.Append(clusterID, rowID, encoded); err != nil {
					return nil, fmt.Errorf("segment generation rebuild: append data: %w", err)
				}
				if err := blockWriter.Append(clusterID, rowID, offset, dataWriter.EntrySize(), encoded); err != nil {
					return nil, fmt.Errorf("segment generation rebuild: append block rowid %d: %w", rowID, err)
				}
				rowLocs = append(rowLocs, rowLoc{rowID: rowID, clusterID: clusterID, offset: offset})
			}
			if err == io.ErrUnexpectedEOF {
				break
			}
		}
		if err := spool.file.Close(); err != nil {
			return nil, fmt.Errorf("segment generation rebuild: close cluster spool: %w", err)
		}
		spool.file = nil
		if err := os.Remove(spool.path); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("segment generation rebuild: remove cluster spool: %w", err)
		}
		spool.path = ""
	}
	dataStore, err := dataWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("segment generation rebuild: close data writer: %w", err)
	}
	dataWriter = nil
	blockStore, err := blockWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		return nil, fmt.Errorf("segment generation rebuild: close block writer: %w", err)
	}
	blockWriter = nil

	slices.SortFunc(rowLocs, func(a, b rowLoc) int {
		switch {
		case a.rowID < b.rowID:
			return -1
		case a.rowID > b.rowID:
			return 1
		default:
			return 0
		}
	})
	for _, loc := range rowLocs {
		if err := rowMapWriter.Append(loc.rowID, loc.clusterID, loc.offset); err != nil {
			_ = dataStore.Close()
			_ = blockStore.Close()
			return nil, fmt.Errorf("segment generation rebuild: append rowmap: %w", err)
		}
	}
	rowMapStore, err := rowMapWriter.Close()
	if err != nil {
		_ = dataStore.Close()
		_ = blockStore.Close()
		return nil, fmt.Errorf("segment generation rebuild: close rowmap writer: %w", err)
	}
	rowMapWriter = nil

	manifest := vecindex.SegmentManifest{
		Version:                  vecindex.SegmentStoreVersion,
		Database:                 meta.Database,
		IndexName:                meta.IndexName,
		IndexCreatedAt:           meta.CreatedAt,
		Metric:                   meta.Metric,
		Dim:                      uint32(meta.Dim),
		InternalDim:              uint32(spec.InternalDim()),
		ProbeCentroidEpoch:       expectedEpoch,
		ProbeCentroidBlob:        mustCentroidBlob(cs),
		StableCentroidEpoch:      expectedEpoch,
		StableCentroidBlob:       mustCentroidBlob(cs),
		StableMemberCodecBlob:    stableCodecBlob,
		AppliedOverlaySeq:        appliedOverlaySeq,
		Generation:               generation,
		MaxCluster:               uint32(maxCluster),
		RowCount:                 rowCount,
		ClusterRowCounts:         clusterRowCounts,
		ClusterVectorSums:        cloneClusterVectorSums(clusterVectorSums),
		RowsModifiedSinceRebuild: 0,
		LastRebuildRowCount:      rowCount,
		ConsecutiveSkewCycles:    nextSkewCycleCount(clusterRowCounts, meta.TargetPartitionSize, 0),
		LayoutHotClusters:        uint32Slice(layoutHotClusters),
		BlockRows:                uint32(blockStore.BlockRows()),
		CreatedAtUnixNano:        time.Now().UnixNano(),
	}
	if err := publishSegmentGeneration(dir, manifest, staging.artifacts); err != nil {
		_ = dataStore.Close()
		_ = rowMapStore.Close()
		_ = blockStore.Close()
		return nil, err
	}
	_ = dataStore.Close()
	_ = rowMapStore.Close()
	_ = blockStore.Close()

	opened, err := openSegmentGeneration(dir, meta, spec, expectedEpoch)
	if err != nil {
		return nil, err
	}
	return segmentGenerationFromOpened(opened), nil
}

func buildAndStoreSegmentGeneration(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	state *vecindex.IndexState,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) error {
	if state == nil || dbPath == "" {
		return nil
	}
	cs := state.ProbeState()
	if cs == nil {
		state.ClearSegmentStore()
		return nil
	}
	var appliedOverlaySeq uint64
	if overlay := state.LoadOverlay(); overlay != nil && overlay.Snapshot() != nil {
		appliedOverlaySeq = overlay.Snapshot().LastSequence()
	}
	generation, err := RebuildSegmentGeneration(
		ctx,
		db,
		dbPath,
		meta,
		spec,
		cs,
		appliedOverlaySeq,
		state.HotClusterScores(segmentLayoutHotClusterLimit),
	)
	if err != nil {
		return err
	}
	if generation != nil {
		var previous uint32
		if maintenance := state.LoadMaintenanceState(); maintenance != nil {
			_, _, previous = maintenance.Stats()
		}
		generation.ConsecutiveSkewCycles = nextSkewCycleCount(generation.ClusterRowCounts, meta.TargetPartitionSize, previous)
	}
	state.StoreSegmentStore(generation)
	return nil
}

func segmentClusterWriteOrder(spec vecindex.IVFSpec, cs *kmeans.CentroidSet, hotClusterScores map[int64]uint64) []int64 {
	if cs == nil || cs.Len() == 0 {
		return nil
	}
	centroids := cs.Snapshot()
	projection := deterministicSegmentProjection(spec.Seed^segmentLayoutSeed, spec.InternalDim())
	return orderClustersByProjection(centroids, projection, hotClusterScores)
}

func orderClustersByProjection(centroids [][]float32, projection []float32, hotClusterScores map[int64]uint64) []int64 {
	if len(centroids) == 0 {
		return nil
	}
	type clusterScore struct {
		clusterID int64
		hotScore  uint64
		score     float32
	}
	scores := make([]clusterScore, 0, len(centroids))
	for i, centroid := range centroids {
		if len(centroid) == 0 {
			continue
		}
		clusterID := int64(i + 1)
		scores = append(scores, clusterScore{
			clusterID: clusterID,
			hotScore:  hotClusterScores[clusterID],
			score:     metric.DotProduct(centroid, projection),
		})
	}
	slices.SortFunc(scores, func(a, b clusterScore) int {
		switch {
		case a.hotScore > b.hotScore:
			return -1
		case a.hotScore < b.hotScore:
			return 1
		case a.score < b.score:
			return -1
		case a.score > b.score:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		case a.clusterID > b.clusterID:
			return 1
		default:
			return 0
		}
	})
	order := make([]int64, len(scores))
	for i, score := range scores {
		order[i] = score.clusterID
	}
	return order
}

func deterministicSegmentProjection(seed uint64, dim int) []float32 {
	if dim <= 0 {
		return nil
	}
	projection := make([]float32, dim)
	state := seed
	for i := range projection {
		state = splitMix64(state)
		v := float32((state>>40)&0xFFFFFF) / (1 << 23)
		projection[i] = v - 1
	}
	return projection
}

func splitMix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func orderedHotClusterIDs(scores map[int64]uint64, limit int) []int64 {
	if len(scores) == 0 {
		return nil
	}
	type hotCluster struct {
		clusterID int64
		score     uint64
	}
	ordered := make([]hotCluster, 0, len(scores))
	for clusterID, score := range scores {
		if clusterID <= 0 || score == 0 {
			continue
		}
		ordered = append(ordered, hotCluster{clusterID: clusterID, score: score})
	}
	if len(ordered) == 0 {
		return nil
	}
	slices.SortFunc(ordered, func(a, b hotCluster) int {
		switch {
		case a.score > b.score:
			return -1
		case a.score < b.score:
			return 1
		case a.clusterID < b.clusterID:
			return -1
		case a.clusterID > b.clusterID:
			return 1
		default:
			return 0
		}
	})
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}
	out := make([]int64, len(ordered))
	for i, item := range ordered {
		out[i] = item.clusterID
	}
	return out
}

func uint32Slice(clusterIDs []int64) []uint32 {
	if len(clusterIDs) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(clusterIDs))
	for _, clusterID := range clusterIDs {
		if clusterID <= 0 || clusterID > math.MaxUint32 {
			continue
		}
		out = append(out, uint32(clusterID))
	}
	return out
}

func int64Slice(clusterIDs []uint32) []int64 {
	if len(clusterIDs) == 0 {
		return nil
	}
	out := make([]int64, len(clusterIDs))
	for i, clusterID := range clusterIDs {
		out[i] = int64(clusterID)
	}
	return out
}

func cloneClusterVectorSums(src [][]float32) [][]float32 {
	if len(src) == 0 {
		return nil
	}
	out := make([][]float32, len(src))
	for i := range src {
		if len(src[i]) == 0 {
			continue
		}
		out[i] = append([]float32(nil), src[i]...)
	}
	return out
}

func openAndStoreOverlay(dbPath, indexName string, state *vecindex.IndexState, epoch uint64) error {
	if state == nil || dbPath == "" {
		return nil
	}
	dir := vecindex.SegmentStoreDir(dbPath, indexName)
	overlay, err := vecindex.OpenJournaledOverlay(vecindex.OverlayJournalPath(dir))
	if err != nil {
		return err
	}
	if err := reconcileOverlayForState(state, overlay, epoch); err != nil {
		_ = overlay.Close()
		return err
	}
	state.StoreOverlay(overlay)
	if err := syncMaintenanceStateFromOverlay(state); err != nil {
		state.ClearOverlay()
		return err
	}
	return nil
}

func reconcileOverlayForState(state *vecindex.IndexState, overlay *vecindex.JournaledOverlay, epoch uint64) error {
	if overlay == nil {
		return nil
	}
	snapshot := overlay.Snapshot()
	if snapshot == nil {
		if err := overlay.Reset(epoch); err != nil {
			return err
		}
		return nil
	}
	segments := state.LoadSegmentStore()
	if epoch == 0 || snapshot.Epoch() == epoch {
		if segments != nil && snapshot.LastSequence() < segments.AppliedOverlaySeq {
			return overlay.Rewrite(epoch, segments.AppliedOverlaySeq, nil)
		}
		return nil
	}
	if segments == nil || state.ProbeState() == nil {
		return overlay.Reset(epoch)
	}
	tailMutations, err := reassignOverlayMutationsForProbe(context.Background(), nil, snapshot, segments.AppliedOverlaySeq, state.Spec(), state.ProbeState(), segments)
	if err != nil {
		return err
	}
	return overlay.Rewrite(epoch, segments.AppliedOverlaySeq, tailMutations)
}

func syncMaintenanceStateFromOverlay(state *vecindex.IndexState) error {
	if state == nil {
		return nil
	}
	maintenance := state.LoadMaintenanceState()
	if maintenance == nil {
		return nil
	}
	maintenance.ResetPending()
	segments := state.LoadSegmentStore()
	overlay := state.LoadOverlay()
	if segments == nil || segments.Data == nil || segments.RowMap == nil || overlay == nil {
		return nil
	}
	snapshot := overlay.Snapshot()
	if snapshot == nil {
		return nil
	}
	var syncErr error
	snapshot.VisitMutationHeadersAfter(segments.AppliedOverlaySeq, func(mutation vecindex.OverlayMutation) bool {
		oldCluster := int64(0)
		if loc, ok, err := segments.RowMap.Lookup(mutation.RowID); err != nil {
			syncErr = err
			return false
		} else if ok {
			oldCluster = loc.ClusterID
		}
		var newCluster int64
		if mutation.Kind != vecindex.OverlayMutationDelete {
			newCluster = mutation.ClusterID
		}
		if mutation.Kind == vecindex.OverlayMutationUpsert {
			oldCluster = 0
		}
		maintenance.RecordClusterMutation(oldCluster, nil, newCluster, nil)
		return true
	})
	if syncErr != nil {
		return syncErr
	}
	return nil
}

func loadStablePreparedForMaintenance(segments *vecindex.SegmentGeneration, spec vecindex.IVFSpec, rowID int64) (int64, []float32, error) {
	if segments == nil || segments.Data == nil || segments.RowMap == nil || rowID == 0 {
		return 0, nil, nil
	}
	loc, ok, err := segments.RowMap.Lookup(rowID)
	if err != nil || !ok {
		return 0, nil, err
	}
	readRowID, vecBytes, err := segments.Data.ReadEntryAt(loc.Offset)
	if err != nil {
		return 0, nil, err
	}
	if readRowID != rowID {
		return 0, nil, fmt.Errorf("stable maintenance row mismatch: got %d want %d", readRowID, rowID)
	}
	prepared, err := decodeStableMemberPrepared(spec, segments.StableCodec, segments.StableCentroids, loc.ClusterID, vecBytes)
	if err != nil {
		return 0, nil, err
	}
	return loc.ClusterID, prepared, nil
}

func BuildSegmentGenerationOnReopen(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	state *vecindex.IndexState,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) error {
	if state == nil || dbPath == "" {
		return nil
	}
	dir := vecindex.SegmentStoreDir(dbPath, meta.IndexName)
	if generation, err := openSegmentGeneration(dir, meta, spec, state.ProbeVersion()); err == nil && generation != nil {
		pruneSegmentStoreOnStartup(dir, generation.Manifest.Generation)
		state.SwapProbeState(generation.ProbeCentroids)
		state.StoreSegmentStore(segmentGenerationFromOpened(generation))
		if err := openAndStoreOverlay(dbPath, meta.IndexName, state, generation.Manifest.ProbeEpochValue()); err != nil {
			return err
		}
		return nil
	}
	if err := openAndStoreOverlay(dbPath, meta.IndexName, state, state.ProbeVersion()); err != nil {
		return err
	}
	return buildAndStoreSegmentGeneration(ctx, db, dbPath, state, meta, spec)
}

// keep the compiler honest that this file still belongs in db lifecycle code.
var (
	_ = context.Background
	_ *sql.DB
)
