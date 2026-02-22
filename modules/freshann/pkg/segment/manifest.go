package segment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type Manifest struct {
	Version       uint64    `json:"version"`
	ActiveSegment string    `json:"active_segment"`
	VectorCount   uint64    `json:"vector_count"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func ManifestPath(dir string) string {
	return filepath.Join(dir, "manifest.json")
}

func LoadManifest(dir string) (Manifest, error) {
	var m Manifest
	f, err := os.Open(ManifestPath(dir))
	if err != nil {
		return m, err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(&m)
	return m, err
}

func SaveManifestAtomic(dir string, m Manifest) error {
	path := ManifestPath(dir)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
