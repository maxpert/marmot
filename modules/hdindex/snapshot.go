package hdindex

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// SnapshotIndex creates a compressed tar archive of an index's Pebble data.
// Writes a tar.gz stream to the provided writer. The index remains open and
// readable during the snapshot (Pebble checkpoint is crash-consistent).
func (e *Engine) SnapshotIndex(ctx context.Context, id string, w io.Writer) error {
	e.mu.RLock()
	idx, exists := e.indexes[id]
	e.mu.RUnlock()
	if !exists {
		return fmt.Errorf("hdindex: index %q not open", id)
	}

	tmpDir, err := os.MkdirTemp(e.rootDir, ".snapshot-"+id+"-*")
	if err != nil {
		return fmt.Errorf("hdindex: create snapshot tmpdir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	checkpointDir := filepath.Join(tmpDir, id)
	if err := idx.Checkpoint(checkpointDir); err != nil {
		return fmt.Errorf("hdindex: checkpoint index %q: %w", id, err)
	}

	return tarGzDir(checkpointDir, w)
}

// RestoreIndex restores an index from a compressed tar archive.
// Reads a tar.gz stream from the provided reader and extracts it to the
// engine root directory. The archive must contain a top-level directory
// named after the index ID (as produced by SnapshotIndex). The index can
// then be opened with OpenIndex.
func (e *Engine) RestoreIndex(ctx context.Context, id string, r io.Reader) error {
	idxDir := filepath.Join(e.rootDir, id)
	if err := os.RemoveAll(idxDir); err != nil {
		return fmt.Errorf("hdindex: remove existing index dir: %w", err)
	}
	// The archive contains a top-level "<id>/" directory, so we extract into
	// e.rootDir so the result lands at e.rootDir/<id>/...
	return untarGzDir(r, e.rootDir)
}

// tarGzDir writes a tar.gz archive of the given directory to w.
func tarGzDir(srcDir string, w io.Writer) error {
	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	return filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(filepath.Dir(srcDir), path)
		if err != nil {
			return err
		}
		header.Name = rel

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
}

// untarGzDir extracts a tar.gz archive to destDir.
func untarGzDir(r io.Reader, destDir string) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gr.Close()
	tr := tar.NewReader(gr)

	cleanDest := filepath.Clean(destDir)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		target := filepath.Join(destDir, header.Name)
		if !strings.HasPrefix(filepath.Clean(target), cleanDest) {
			return fmt.Errorf("hdindex: invalid tar path: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			f.Close()
		}
	}
	return nil
}
