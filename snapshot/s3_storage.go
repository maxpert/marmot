package snapshot

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog/log"
)

type s3Storage struct {
	mc *minio.Client
}

func (s s3Storage) Upload(name, filePath string) error {
	ctx := context.Background()
	cS3 := cfg.Config.Snapshot.S3
	bucketPath := fmt.Sprintf("%s/%s", cS3.DirPath, name)
	info, err := s.mc.FPutObject(ctx, cS3.Bucket, bucketPath, filePath, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	log.Info().
		Str("file_name", name).
		Int64("size", info.Size).
		Str("file_path", filePath).
		Str("bucket", info.Bucket).
		Msg("Snapshot saved to S3")

	return nil
}

func (s s3Storage) Download(filePath, name string) error {
	ctx := context.Background()
	cS3 := cfg.Config.Snapshot.S3
	bucketPath := fmt.Sprintf("%s/%s", cS3.DirPath, name)
	err := s.mc.FGetObject(ctx, cS3.Bucket, bucketPath, filePath, minio.GetObjectOptions{})
	if mErr, ok := err.(minio.ErrorResponse); ok {
		if mErr.StatusCode == http.StatusNotFound {
			return ErrNoSnapshotFound
		}
	}

	return err
}

func newS3Storage() (*s3Storage, error) {
	c := cfg.Config
	cS3 := c.Snapshot.S3
	v := credentials.Value{
		AccessKeyID:     cS3.AccessKey,
		SecretAccessKey: cS3.SecretKey,
		SessionToken:    cS3.SessionToken,
	}

	if cS3.AccessKey == "" && cS3.SecretKey == "" {
		v.SignerType = credentials.SignatureAnonymous
	} else {
		v.SignerType = credentials.SignatureV4
	}

	chain := []credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.EnvMinio{},
		&credentials.Static{
			Value: v,
		},
	}

	creds := credentials.NewChainCredentials(chain)
	mc, err := minio.New(cS3.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cS3.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := mc.BucketExists(ctx, cS3.Bucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		err = mc.MakeBucket(ctx, cS3.Bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}

	return &s3Storage{
		mc: mc,
	}, nil
}
