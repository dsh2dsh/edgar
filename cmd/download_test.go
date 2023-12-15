package cmd

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadDir_NeedFile(t *testing.T) {
	tests := []struct {
		name      string
		needFiles []string
		fname     string
		expect    bool
	}{
		{
			name:      "nil needFiles",
			needFiles: nil,
			fname:     "foo",
			expect:    true,
		},
		{
			name:      "empty needFiles",
			needFiles: []string{},
			fname:     "foo",
			expect:    true,
		},
		{
			name:      "found",
			needFiles: []string{"foo", "bar"},
			fname:     "foo",
			expect:    true,
		},
		{
			name:      "found",
			needFiles: []string{"foo", "bar"},
			fname:     "baz",
			expect:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDownloadDir("", tt.needFiles)
			if tt.expect {
				assert.True(t, d.NeedFile(tt.fname))
			} else {
				assert.False(t, d.NeedFile(tt.fname))
			}
		})
	}
}

func TestDownloadDir_Save(t *testing.T) {
	datadir, err := os.MkdirTemp("", "edgar")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(datadir) })

	d := newDownloadDir(datadir, nil)
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

	d := newDownloadDir(datadir, nil)
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
