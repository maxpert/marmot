package vecindex

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/maxpert/marmot/modules/vecindex/pkg/store"
)

// SnapshotIndex writes a binary snapshot of the index identified by id to w.
// The snapshot is a gzip-compressed tar archive of the Pebble checkpoint.
func (e *Engine) SnapshotIndex(ctx context.Context, id string, w io.Writer) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	v, ok := e.indexes.Load(id)
	if !ok {
		return fmt.Errorf("vecindex: index %q not open", id)
	}
	idx := v.(*Index)

	// Take a read lock to get a consistent checkpoint.
	idx.mu.RLock()
	checkpointDir, err := os.MkdirTemp("", "vecindex-snap-*")
	if err != nil {
		idx.mu.RUnlock()
		return fmt.Errorf("vecindex: create checkpoint temp dir: %w", err)
	}
	// Pebble Checkpoint requires the destination to not exist.
	checkpointDir = filepath.Join(checkpointDir, "chk")
	snapErr := idx.st.Checkpoint(checkpointDir)
	idx.mu.RUnlock()

	defer os.RemoveAll(filepath.Dir(checkpointDir))

	if snapErr != nil {
		return fmt.Errorf("vecindex: checkpoint: %w", snapErr)
	}

	return tarGzDir(ctx, checkpointDir, w)
}

// RestoreIndex reads a snapshot from r and restores it into this engine as the
// index identified by id. Returns an error if an index with that id already exists.
func (e *Engine) RestoreIndex(ctx context.Context, id string, r io.Reader) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if _, exists := e.indexes.Load(id); exists {
		return fmt.Errorf("vecindex: index %q already exists", id)
	}

	dir := e.indexDir(id)
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("vecindex: index directory %s already exists", dir)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("vecindex: create restore dir: %w", err)
	}

	if err := untarGz(ctx, r, dir); err != nil {
		_ = os.RemoveAll(dir)
		return fmt.Errorf("vecindex: untar snapshot: %w", err)
	}

	// Verify the restored data is a valid store.
	st, err := store.New(dir, &pebble.Options{})
	if err != nil {
		_ = os.RemoveAll(dir)
		return fmt.Errorf("vecindex: open restored store: %w", err)
	}
	_ = st.Close()

	return nil
}

// tarGzDir writes all files under srcDir into a gzip-compressed tar written to w.
func tarGzDir(ctx context.Context, srcDir string, w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)

	err := filepath.Walk(srcDir, func(path string, fi os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		hdr, err := tar.FileInfoHeader(fi, "")
		if err != nil {
			return err
		}
		hdr.Name = rel

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if fi.IsDir() {
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

	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// untarGz extracts a gzip-compressed tar stream into destDir.
func untarGz(ctx context.Context, r io.Reader, destDir string) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if strings.Contains(hdr.Name, "..") || strings.HasPrefix(hdr.Name, "/") {
			return fmt.Errorf("untarGz: unsafe path in archive: %s", hdr.Name)
		}
		target := filepath.Join(destDir, filepath.Clean(hdr.Name))
		cleanDest := filepath.Clean(destDir) + string(os.PathSeparator)
		if !strings.HasPrefix(target, cleanDest) && target != filepath.Clean(destDir) {
			return fmt.Errorf("untarGz: path escapes dest: %s", hdr.Name)
		}

		switch hdr.Typeflag {
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
