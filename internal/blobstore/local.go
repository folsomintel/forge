package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Local is a filesystem Store - the dev/self-host default, and the shape
// S3 mirrors (write-temp-then-rename gives the same atomicity Put promises).
type Local struct {
	root string
}

func NewLocal(root string) (*Local, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Local{root: root}, nil
}

func (l *Local) path(repoID, name string) (string, error) {
	if err := checkKey(repoID, name); err != nil {
		return "", err
	}
	return filepath.Join(l.root, repoID, name), nil
}

func (l *Local) Put(ctx context.Context, repoID, name string, r io.Reader) error {
	// name may contain subpaths (e.g. "lfs/<oid>").
	dest, err := l.path(repoID, name)
	if err != nil {
		return err
	}
	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(name)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dest)
}

func (l *Local) Get(ctx context.Context, repoID, name string) (io.ReadCloser, error) {
	p, err := l.path(repoID, name)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, repoID, name)
	}
	return f, err
}

// fileSection is a length-bounded reader over a file that closes the file.
type fileSection struct {
	*io.SectionReader
	f *os.File
}

func (s *fileSection) Close() error { return s.f.Close() }

func (l *Local) GetRange(ctx context.Context, repoID, name string, off, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return nil, fmt.Errorf("%w: non-positive range length", ErrBadKey)
	}
	p, err := l.path(repoID, name)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, repoID, name)
	}
	if err != nil {
		return nil, err
	}
	return &fileSection{SectionReader: io.NewSectionReader(f, off, length), f: f}, nil
}

func (l *Local) Copy(ctx context.Context, repoID, srcName, dstName string) error {
	src, err := l.path(repoID, srcName)
	if err != nil {
		return err
	}
	dst, err := l.path(repoID, dstName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	os.Remove(dst)
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s/%s", ErrNotFound, repoID, srcName)
		}
		return err
	}
	defer in.Close()
	return l.Put(ctx, repoID, dstName, in)
}

func (l *Local) PutIfAbsent(ctx context.Context, repoID, name string, data []byte) error {
	dest, err := l.path(repoID, name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, fs.ErrExist) {
		return ErrExists
	}
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(dest)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (l *Local) Prefixes(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(l.root)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func (l *Local) Delete(ctx context.Context, repoID, name string) error {
	p, err := l.path(repoID, name)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (l *Local) Presign(ctx context.Context, repoID, name string, expiry time.Duration) (string, error) {
	return "", ErrNoPresign
}

func (l *Local) List(ctx context.Context, repoID, prefix string) ([]BlobInfo, error) {
	if err := checkPrefix(repoID, prefix); err != nil {
		return nil, err
	}
	root := filepath.Join(l.root, repoID)
	// Names stay relative to the repo root (e.g. "refs/wal/5.json") whatever
	// the prefix; the walk just starts deeper so a caller after one subtree
	// doesn't pay to enumerate every pack.
	walkRoot := filepath.Join(root, filepath.FromSlash(prefix))
	var out []BlobInfo
	err := filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return filepath.SkipAll
		}
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out = append(out, BlobInfo{Name: filepath.ToSlash(rel), Size: info.Size(), ModTime: info.ModTime()})
		return nil
	})
	return out, err
}
