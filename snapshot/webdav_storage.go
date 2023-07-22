package snapshot

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"

	"github.com/maxpert/marmot/cfg"
	"github.com/rs/zerolog/log"
	"github.com/studio-b12/gowebdav"
)

type webDAVStorage struct {
	client *gowebdav.Client
	path   string
}

func (w *webDAVStorage) Upload(name, filePath string) error {
	rfl, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer rfl.Close()

	randomPath := path.Join(w.path, fmt.Sprintf("%d-pending.db", rand.Uint32()))
	err = w.client.WriteStream(randomPath, rfl, 0644)
	if err != nil {
		return err
	}

	completedPath := path.Join(w.path, name)
	err = w.client.Rename(randomPath, completedPath, true)
	if err != nil {
		return err
	}

	log.Info().
		Str("file_name", name).
		Str("file_path", filePath).
		Str("webdav_path", completedPath).
		Msg("Snapshot saved to WebDAV")
	return nil
}

func (w *webDAVStorage) Download(filePath, name string) error {
	completedPath := path.Join(w.path, name)
	rst, err := w.client.ReadStream(completedPath)
	if err != nil {
		return err
	}
	defer rst.Close()

	wst, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer wst.Close()

	if _, err = io.Copy(wst, rst); err != nil {
		return err
	}

	log.Info().
		Str("file_name", name).
		Str("file_path", filePath).
		Str("webdav_path", completedPath).
		Msg("Snapshot downloaded from WebDAV")
	return nil
}

func newWebDAVStorage() (*webDAVStorage, error) {
	webDAVCfg := cfg.Config.Snapshot.WebDAV
	cl := gowebdav.NewAuthClient(webDAVCfg.Url, gowebdav.NewAutoAuth(webDAVCfg.Login, webDAVCfg.Secret))
	if webDAVCfg.Path != "" {
		err := cl.MkdirAll(webDAVCfg.Path, 0644)
		if err != nil {
			return nil, err
		}
	}

	return &webDAVStorage{client: cl, path: webDAVCfg.Path}, nil
}
