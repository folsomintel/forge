package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3 is a Store on any S3-compatible endpoint (AWS, R2, GCS interop,
// MinIO). BYO-bucket is the point: point FORGE_S3_* at the customer's bucket.
type S3 struct {
	client *minio.Client
	bucket string
}

func NewS3(endpoint, bucket, accessKey, secretKey string, useSSL bool) (*S3, error) {
	c, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	return &S3{client: c, bucket: bucket}, nil
}

func (s *S3) key(repoID, name string) (string, error) {
	if err := checkKey(repoID, name); err != nil {
		return "", err
	}
	return repoID + "/" + name, nil
}

func (s *S3) Put(ctx context.Context, repoID, name string, r io.Reader) error {
	// With unknown size, minio-go allocates a full multipart part buffer up
	// front - fatal on small machines (a 16-writer push storm of in-memory
	// packs once OOM-killed a 256MB box at 16MiB per Put). Learn the real
	// size wherever possible: stat files, Len() for in-memory readers
	// (bytes.Reader, strings.Reader). Only true unbounded streams (e.g.
	// LFS uploads) pay the capped part buffer.
	size := int64(-1)
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	switch v := r.(type) {
	case *os.File:
		if info, err := v.Stat(); err == nil {
			size = info.Size()
		}
	case interface{ Len() int }:
		size = int64(v.Len())
	}
	if size < 0 {
		opts.PartSize = 16 << 20
	}
	// Large uploads: parallel multipart moves push-ack latency for big
	// packs from bandwidth-serial to bandwidth-parallel.
	opts.NumThreads = 4
	key, err := s.key(repoID, name)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, s.bucket, key, r, size, opts)
	return err
}

func (s *S3) Get(ctx context.Context, repoID, name string) (io.ReadCloser, error) {
	key, err := s.key(repoID, name)
	if err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy; surface missing-key errors now, not at first read.
	if _, err := obj.Stat(); err != nil {
		obj.Close()
		var mErr minio.ErrorResponse
		if errors.As(err, &mErr) && mErr.Code == "NoSuchKey" {
			return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, repoID, name)
		}
		return nil, err
	}
	return obj, nil
}

func (s *S3) GetRange(ctx context.Context, repoID, name string, off, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return nil, fmt.Errorf("%w: non-positive range length", ErrBadKey)
	}
	key, err := s.key(repoID, name)
	if err != nil {
		return nil, err
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+length-1); err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	return obj, nil // lazy; read errors (incl. NoSuchKey) surface on first Read
}

func (s *S3) Copy(ctx context.Context, repoID, srcName, dstName string) error {
	// Server-side copy: no bytes transit the machine. Works on any
	// S3-compatible endpoint and stays inside the (possibly BYO) bucket.
	srcKey, err := s.key(repoID, srcName)
	if err != nil {
		return err
	}
	dstKey, err := s.key(repoID, dstName)
	if err != nil {
		return err
	}
	_, err = s.client.CopyObject(ctx,
		minio.CopyDestOptions{Bucket: s.bucket, Object: dstKey},
		minio.CopySrcOptions{Bucket: s.bucket, Object: srcKey})
	return err
}

func (s *S3) PutIfAbsent(ctx context.Context, repoID, name string, data []byte) error {
	key, err := s.key(repoID, name)
	if err != nil {
		return err
	}
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	opts.SetMatchETagExcept("*") // If-None-Match: * - conditional create
	_, err = s.client.PutObject(ctx, s.bucket, key,
		bytes.NewReader(data), int64(len(data)), opts)
	var mErr minio.ErrorResponse
	if errors.As(err, &mErr) && (mErr.StatusCode == http.StatusPreconditionFailed || mErr.StatusCode == http.StatusConflict) {
		return ErrExists
	}
	return err
}

func (s *S3) Prefixes(ctx context.Context) ([]string, error) {
	var out []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Recursive: false}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			out = append(out, strings.TrimSuffix(obj.Key, "/"))
		}
	}
	return out, nil
}

func (s *S3) Delete(ctx context.Context, repoID, name string) error {
	key, err := s.key(repoID, name)
	if err != nil {
		return err
	}
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// Presign works against any S3-compatible endpoint (AWS/R2/Tigris/MinIO):
// V4 signing is client-side, no API round trip. AWS caps expiry at 7 days.
func (s *S3) Presign(ctx context.Context, repoID, name string, expiry time.Duration) (string, error) {
	key, err := s.key(repoID, name)
	if err != nil {
		return "", err
	}
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, expiry, nil)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *S3) List(ctx context.Context, repoID, prefix string) ([]BlobInfo, error) {
	if err := checkPrefix(repoID, prefix); err != nil {
		return nil, err
	}
	base := repoID + "/"
	// Names stay relative to the repo root; prefix only narrows the listing
	// (e.g. "refs/" to skip a repo's thousands of pack blobs).
	var out []BlobInfo
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: base + prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, BlobInfo{
			Name:    strings.TrimPrefix(obj.Key, base),
			Size:    obj.Size,
			ModTime: obj.LastModified,
		})
	}
	return out, nil
}
