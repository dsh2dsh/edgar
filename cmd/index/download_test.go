package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/edgar/client"
	mocksClient "github.com/dsh2dsh/edgar/internal/mocks/client"
	mocksDownload "github.com/dsh2dsh/edgar/internal/mocks/download"
)

func TestDownload_WithProcsLimit(t *testing.T) {
	d := Download{}
	assert.Same(t, &d, d.WithProcsLimit(10))
	assert.Equal(t, 10, d.procs)
}

func TestDownload_readIndex(t *testing.T) {
	const testPath = "edgar/full-index"
	testErr := errors.New("test error")

	tests := []struct {
		name    string
		mockDo  func(t *testing.T, m *mocksClient.MockHttpRequestDoer)
		errorIs error
		skip    bool
	}{
		{
			name: "ok",
		},
		{
			name: "error",
			mockDo: func(t *testing.T, m *mocksClient.MockHttpRequestDoer) {
				m.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						return nil, testErr
					})
			},
			errorIs: testErr,
		},
		{
			name: "unexpected status",
			mockDo: func(t *testing.T, m *mocksClient.MockHttpRequestDoer) {
				m.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						recorder := httptest.NewRecorder()
						recorder.WriteHeader(http.StatusNotFound)
						return recorder.Result(), nil
					})
			},
			errorIs: client.ErrUnexpectedStatus,
		},
		{
			name: "skip 403",
			mockDo: func(t *testing.T, m *mocksClient.MockHttpRequestDoer) {
				m.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						recorder := httptest.NewRecorder()
						recorder.WriteHeader(http.StatusForbidden)
						return recorder.Result(), nil
					})
			},
			skip: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := mocksClient.NewMockHttpRequestDoer(t)
			if tt.mockDo != nil {
				tt.mockDo(t, httpClient)
			} else {
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						index := readTestArchiveIndex(t, testPath)
						b, err := json.Marshal(&index)
						require.NoError(t, err)
						recorder := httptest.NewRecorder()
						_, err = recorder.Write(b)
						require.NoError(t, err)
						return recorder.Result(), nil
					})
			}

			storage := mocksDownload.NewMockStorage(t)
			d := newTestDownload(t, httpClient, storage)

			index, skip, err := d.readIndex(context.Background(), testPath)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
				if tt.skip {
					assert.True(t, skip)
				} else {
					assert.False(t, skip)
					assert.Equal(t, readTestArchiveIndex(t, testPath), index)
				}
			}
		})
	}
}

func readTestArchiveIndex(t *testing.T, path string) (index client.ArchiveIndex) {
	index.Directory.Name = filepath.Base(path)
	files, err := os.ReadDir(filepath.Join("testdata", path))
	require.NoError(t, err)

	var items []client.ArchiveItem
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), ".") {
			item := client.ArchiveItem{Name: file.Name()}
			if file.IsDir() {
				item.Type = "dir"
			} else {
				item.Type = "file"
			}
			items = append(items, item)
		}
	}
	index.Directory.Item = items
	return
}

func newTestDownload(t *testing.T, httpClient client.HttpRequestDoer,
	st Storage,
) *Download {
	c := client.New(client.WithHttpClient(httpClient))
	require.NotNil(t, c)
	d := NewDownload(c, st)
	require.NotNil(t, d)
	return d
}

func TestDownload_itemHandler(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		item    client.ArchiveItem
		wantNil bool
		wantErr bool
	}{
		{
			name: "dir",
			path: "edgar/full-index/1994",
			item: client.ArchiveItem{Name: "QTR1", Type: "dir"},
		},
		{
			name: "file",
			path: "edgar/full-index/1994/QTR1",
			item: client.ArchiveItem{Name: "master.gz", Type: "file"},
		},
		{
			name:    "unknown type",
			path:    "edgar/full-index/1994/QTR1",
			item:    client.ArchiveItem{Name: "master.gz", Type: "unknown"},
			wantNil: true,
		},
		{
			name:    "unknown type",
			path:    "edgar/full-index/1994/QTR1",
			item:    client.ArchiveItem{Name: "master.gz", Type: "unknown"},
			wantNil: true,
		},
		{
			name:    "JoinPath error",
			path:    ":localhost",
			wantErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Download{}
			f, err := d.itemHandler(ctx, tt.path, tt.item)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.wantNil {
					assert.Nil(t, f)
				} else {
					assert.NotNil(t, f)
				}
			}
		})
	}
}

func TestDownload_NeedFile(t *testing.T) {
	tests := []struct {
		name      string
		needFiles []string
		fname     string
		found     bool
	}{
		{
			name:  "without needFiles",
			fname: "foo",
			found: true,
		},
		{
			name:      "empty needFiles",
			needFiles: []string{},
			fname:     "foo",
			found:     true,
		},
		{
			name:      "found",
			needFiles: []string{"foo", "bar"},
			fname:     "foo",
			found:     true,
		},
		{
			name:      "not found",
			needFiles: []string{"foo", "bar"},
			fname:     "baz",
			found:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Download{}
			if tt.needFiles != nil {
				d.WithNeedFiles(tt.needFiles)
			}
			if tt.found {
				assert.True(t, d.NeedFile(tt.fname))
			} else {
				assert.False(t, d.NeedFile(tt.fname))
			}
		})
	}
}

func TestDownload_downloadFile(t *testing.T) {
	const testFile = "edgar/full-index/master.gz"
	testErr := errors.New("test error")

	tests := []struct {
		name        string
		httpDo      func(t *testing.T, req *http.Request) (*http.Response, error)
		mockStorage func(t *testing.T, m *mocksDownload.MockStorage)
		errorIs     error
	}{
		{
			name: "ok",
		},
		{
			name: "save error",
			mockStorage: func(t *testing.T, m *mocksDownload.MockStorage) {
				m.EXPECT().Save("edgar/full-index", "master.gz", mock.Anything).
					RunAndReturn(func(path, fname string, r io.Reader) error {
						return testErr
					})
			},
			errorIs: testErr,
		},
		{
			name: "http error",
			httpDo: func(t *testing.T, req *http.Request) (*http.Response, error) {
				return nil, testErr
			},
			mockStorage: func(t *testing.T, m *mocksDownload.MockStorage) {},
			errorIs:     testErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := mocksClient.NewMockHttpRequestDoer(t)
			if tt.httpDo != nil {
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						return tt.httpDo(t, req)
					})
			} else {
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						recorder := httptest.NewRecorder()
						_, err := recorder.Write(readTestArchiveFile(t, testFile))
						require.NoError(t, err)
						return recorder.Result(), nil
					})
			}

			parentPath := filepath.Dir(testFile)
			fname := filepath.Base(testFile)
			var savedBytes []byte

			storage := mocksDownload.NewMockStorage(t)
			if tt.mockStorage != nil {
				tt.mockStorage(t, storage)
			} else {
				storage.EXPECT().Save(parentPath, fname, mock.Anything).
					RunAndReturn(func(path, fname string, r io.Reader) error {
						var b bytes.Buffer
						_, err := io.Copy(&b, r)
						require.NoError(t, err)
						savedBytes = b.Bytes()
						return nil
					})
			}

			d := newTestDownload(t, httpClient, storage)
			err := d.downloadFile(context.Background(), parentPath, fname, testFile)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
				fileContent := readTestArchiveFile(t, testFile)
				assert.Equal(t, savedBytes, fileContent)
			}
		})
	}
}

func readTestArchiveFile(t *testing.T, path string) []byte {
	b, err := os.ReadFile(filepath.Join("testdata", path))
	require.NoError(t, err)
	return b
}
