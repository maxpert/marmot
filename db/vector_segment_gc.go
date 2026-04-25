package db

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/rs/zerolog/log"
)

func installRetiredGenerationGC(state *vecindex.IndexState) {
	if state == nil {
		return
	}
	generation := state.LoadSegmentStore()
	files := retiredGenerationFiles(generation)
	if len(files) == 0 {
		return
	}
	state.AddRetireCallback(func() {
		for _, path := range files {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				log.Debug().Err(err).Str("path", path).Msg("vector segment gc: remove retired file failed")
			}
		}
	})
}

func retiredGenerationFiles(generation *vecindex.SegmentGeneration) []string {
	if generation == nil || generation.Data == nil {
		return nil
	}
	gen := generation.Data.Generation()
	dataPath := generation.Data.Path()
	if dataPath == "" || gen == 0 {
		return nil
	}
	dir := filepath.Dir(filepath.Dir(dataPath))
	files := []string{dataPath}
	if generation.RowMap != nil && generation.RowMap.Path() != "" {
		files = append(files, generation.RowMap.Path())
	} else {
		files = append(files, vecindex.SegmentRowMapPath(dir, gen))
	}
	files = append(files, vecindex.SegmentManifestPath(dir, gen))
	return files
}

func pruneSegmentStoreOnStartup(dir string, keepGeneration uint64) {
	if dir == "" || keepGeneration == 0 {
		return
	}
	removeTmpFiles(filepath.Join(dir, "segments"))
	removeTmpFiles(filepath.Join(dir, "rowmap"))
	removeTmpFiles(filepath.Join(dir, "manifest"))
	removeOldGenerationFiles(filepath.Join(dir, "segments"), ".dat", keepGeneration)
	removeOldGenerationFiles(filepath.Join(dir, "rowmap"), ".rmap", keepGeneration)
	removeOldGenerationFiles(filepath.Join(dir, "manifest"), ".mf", keepGeneration)
}

func removeTmpFiles(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Debug().Err(err).Str("path", path).Msg("vector segment gc: remove tmp failed")
		}
	}
}

func removeOldGenerationFiles(dir string, suffix string, keepGeneration uint64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}
		gen, ok := parseSegmentGenerationName(entry.Name(), suffix)
		if !ok || gen == keepGeneration {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Debug().Err(err).Str("path", path).Msg("vector segment gc: remove orphan failed")
		}
	}
}

func parseSegmentGenerationName(name string, suffix string) (uint64, bool) {
	if !strings.HasPrefix(name, "gen-") || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, "gen-"), suffix)
	gen, err := strconv.ParseUint(raw, 10, 64)
	return gen, err == nil
}
