// Package store is the pack store: immutable blobs (pack + idx files) keyed
// by repo and filename. Blobs are written once and never modified - deletes
// only happen during compaction, after the metadata pack list has moved on.
package blobstore

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrNotFound  = errors.New("blob not found")
	ErrExists    = errors.New("blob already exists")
	ErrNoPresign = errors.New("store cannot presign URLs")
)

type BlobInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

type Store interface {
	// Put writes a blob. Must be atomic: readers never see partial content.
	Put(ctx context.Context, repoID, name string, r io.Reader) error
	Get(ctx context.Context, repoID, name string) (io.ReadCloser, error)
	// GetRange reads length bytes starting at off - the primitive behind the
	// block-backed pack reader (packstore), which pages pack data from the
	// bucket without materializing the whole pack. length must be > 0; the
	// reader yields at most length bytes (fewer at EOF).
	GetRange(ctx context.Context, repoID, name string, off, length int64) (io.ReadCloser, error)
	Delete(ctx context.Context, repoID, name string) error
	// Copy duplicates a blob within the same repo prefix without the bytes
	// leaving the store (S3 CopyObject; hardlink/copy locally). Same-bucket
	// by construction, so BYOB stays self-contained.
	Copy(ctx context.Context, repoID, srcName, dstName string) error
	// PutIfAbsent writes a small blob only if the key does not exist yet -
	// the compare-and-swap primitive of the ref WAL (S3 conditional PUT
	// If-None-Match: *; O_EXCL locally). Returns ErrExists on losing the
	// race.
	PutIfAbsent(ctx context.Context, repoID, name string, data []byte) error
	// Prefixes lists top-level repo prefixes in the store (disaster
	// recovery: rediscover repos from the bucket alone).
	Prefixes(ctx context.Context) ([]string, error)
	// List enumerates the repo's blobs whose name starts with prefix (""
	// for all). Names are returned relative to the repo root regardless of
	// prefix; a narrower prefix (e.g. "refs/") just avoids listing packs.
	List(ctx context.Context, repoID, prefix string) ([]BlobInfo, error)
	// Presign returns a time-limited direct-download URL for a blob, or
	// ErrNoPresign when the backend can't (local store, private buckets are
	// caught by the caller's reachability probe instead).
	Presign(ctx context.Context, repoID, name string, expiry time.Duration) (string, error)
}
