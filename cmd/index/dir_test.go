package index

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadDir_Save(t *testing.T) {
	datadir, err := os.MkdirTemp("", "edgar")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(datadir) })

	d := newDownloadDir(datadir)
	buf := bytes.NewReader([]byte("foobar"))
	require.NoError(t, d.Save("a/b/c", "foobar.txt", buf))

	data, err := os.ReadFile(filepath.Join(datadir, "a/b/c/foobar.txt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("foobar"), data)

	require.Error(t, d.Save("a/b/c/foobar.txt", "foobar.txt", buf))
	require.Error(t, d.Save("a/b/", "c", buf))

	wantErr := errors.New("test error")
	readErr := errReader{err: wantErr}
	require.ErrorIs(t, d.Save("a/b/c", "foobar.txt", &readErr), wantErr)
}

type errReader struct {
	err error
}

func (self *errReader) Read(p []byte) (n int, err error) {
	return 0, self.err
}

func TestDownloadDir_makePath(t *testing.T) {
	datadir, err := os.MkdirTemp("", "edgar")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(datadir) })

	d := newDownloadDir(datadir)
	require.NoError(t, d.makePath("a/b/c"))

	fi, err := os.Stat(filepath.Join(datadir, "a/b/c"))
	require.NoError(t, err)
	assert.True(t, fi.IsDir())
	require.NoError(t, d.makePath("a/b/c"))

	require.NoError(t, os.WriteFile(
		filepath.Join(datadir, "foobar"), []byte("foobar"), 0o600))
	require.Error(t, d.makePath("foobar/b/c"))

	d.datadir = filepath.Join(datadir, "foobar")
	require.Error(t, d.makePath("a/b/c"))

	require.NoError(t, os.RemoveAll(datadir))
	require.Error(t, d.makePath("a/b/c"))
}
