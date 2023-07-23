package snapshot

import (
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/rs/zerolog/log"
	"github.com/studio-b12/gowebdav"
)

const queryParamTargetDir = "dir"
const queryParamLogin = "login"
const queryParamSecret = "secret"

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

	err = w.makeStoragePath()
	if err != nil {
		return err
	}

	nodePath := fmt.Sprintf("%s-%d-temp-%s", cfg.Config.NodeName(), time.Now().UnixMilli(), name)
	err = w.client.WriteStream(nodePath, rfl, 0644)
	if err != nil {
		return err
	}

	completedPath := path.Join("/", w.path, name)
	err = w.client.Rename(nodePath, completedPath, true)
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
		if fsErr, ok := err.(*fs.PathError); ok {
			if wdErr, ok := fsErr.Err.(gowebdav.StatusError); ok && wdErr.Status == 404 {
				return ErrNoSnapshotFound
			}
		}
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

func (w *webDAVStorage) makeStoragePath() error {
	err := w.client.MkdirAll(w.path, 0740)
	if err == nil {
		return nil
	}

	if fsError, ok := err.(*os.PathError); ok {
		if wdErr, ok := fsError.Err.(gowebdav.StatusError); ok {
			if wdErr.Status == 409 { // Conflict means directory already exists!
				return nil
			}
		}
	}

	return err
}

func newWebDAVStorage() (*webDAVStorage, error) {
	webDAVCfg := cfg.Config.Snapshot.WebDAV
	u, err := url.Parse(webDAVCfg.Url)
	if err != nil {
		return nil, err
	}

	qp := u.Query()
	targetDir := qp.Get(queryParamTargetDir)
	if targetDir == "" {
		targetDir = "/"
	}

	login := qp.Get(queryParamLogin)
	secret := qp.Get(queryParamSecret)
	if login == "" || secret == "" {
		return nil, ErrRequiredParameterMissing
	}

	// Remove webdav parameters from query params
	qp.Del(queryParamTargetDir)
	qp.Del(queryParamLogin)
	qp.Del(queryParamSecret)

	// Set query params without parameters
	u.RawQuery = qp.Encode()
	cl := gowebdav.NewAuthClient(u.String(), gowebdav.NewAutoAuth(login, secret))
	ret := &webDAVStorage{client: cl, path: targetDir}

	err = cl.Connect()
	if err != nil {
		return nil, err
	}

	return ret, nil
}
