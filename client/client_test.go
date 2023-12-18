package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/dsh2dsh/edgar/client/internal/mocks"
)

func TestNew(t *testing.T) {
	c := testNew(t)
	require.IsType(t, new(Client), c)
	assert.NotNil(t, c.client)
	assert.NotNil(t, c.limiter)
}

func testNew(t *testing.T, opts ...ClientOption) *Client {
	c := New(opts...)
	require.NotNil(t, c)
	return c
}

func TestNew_WithHttpClient(t *testing.T) {
	client := &http.Client{}
	c := testNew(t, WithHttpClient(client))
	assert.Same(t, client, c.client)
}

func TestNew_WithRateLimiter(t *testing.T) {
	l := rate.NewLimiter(limitRate, limitRate)
	c := testNew(t, WithRateLimiter(l))
	assert.Same(t, l, c.limiter)
}

func TestClient_WithUserAgent(t *testing.T) {
	c := testNew(t)
	assert.Same(t, c, c.WithUserAgent("foobar"))
	assert.Equal(t, "foobar", c.ua)
}

func TestClient_Get(t *testing.T) {
	const ua = "Acme admin@acme.com"
	const url = "https://localhost"
	ctx := context.Background()
	testErr := errors.New("expected error")

	tests := []struct {
		name    string
		opts    func() (opts []ClientOption)
		mockDo  func(req *http.Request) (*http.Response, error)
		get     func(c *Client) (*http.Response, error)
		wantErr bool
		errorIs error
	}{
		{
			name: "default",
		},
		{
			name: "WithRateLimit",
			opts: func() (opts []ClientOption) {
				limiter := mocks.NewMockLimiter(t)
				limiter.EXPECT().Wait(mock.Anything).Return(nil)
				opts = append(opts, WithRateLimiter(limiter))
				return
			},
		},
		{
			name: "WithRateLimit nil",
			opts: func() (opts []ClientOption) {
				opts = append(opts, WithRateLimiter(nil))
				return
			},
		},
		{
			name: "WithRateLimit error",
			opts: func() (opts []ClientOption) {
				limiter := mocks.NewMockLimiter(t)
				limiter.EXPECT().Wait(mock.Anything).Return(testErr)
				opts = append(opts, WithRateLimiter(limiter))
				return
			},
			errorIs: testErr,
		},
		{
			name: "Do error",
			mockDo: func(req *http.Request) (*http.Response, error) {
				return nil, testErr
			},
			errorIs: testErr,
		},
		{
			name: "NewRequestWithContext error",
			get: func(c *Client) (*http.Response, error) {
				return c.Get(nil, url) //nolint:staticcheck
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := mocks.NewMockHttpRequestDoer(t)
			opts := []ClientOption{WithHttpClient(httpClient)}
			if tt.opts != nil {
				opts = append(opts, tt.opts()...)
			}
			c := testNew(t, opts...).WithUserAgent(ua)

			if tt.mockDo != nil {
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(tt.mockDo)
			} else if tt.errorIs == nil && !tt.wantErr {
				recorder := httptest.NewRecorder()
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						assert.Equal(t, ua, req.Header.Get("User-Agent"))
						return recorder.Result(), nil
					})
			}

			callGet := func(ctx context.Context, url string) (*http.Response, error) {
				if tt.get != nil {
					return tt.get(c)
				}
				return c.Get(ctx, url)
			}
			resp, err := callGet(ctx, url)

			switch {
			case tt.wantErr:
				require.Error(t, err)
			case tt.errorIs != nil:
				require.ErrorIs(t, err, tt.errorIs)
			default:
				require.NoError(t, err)
				defer resp.Body.Close()
				assert.Equal(t, http.StatusOK, resp.StatusCode)
			}
		})
	}
}

func TestClient_GetJSON(t *testing.T) {
	const testJson = `{
  "directory": {
    "name": "foobar"
  }
}`
	testErr := errors.New("expected error")

	tests := []struct {
		name        string
		json        string
		mockDo      func(req *http.Request) (*http.Response, error)
		wantErr     bool
		errorIs     error
		assertError func(t *testing.T, err error)
	}{
		{
			name: "default",
			json: testJson,
		},
		{
			name: "Get error",
			mockDo: func(req *http.Request) (*http.Response, error) {
				return nil, testErr
			},
			errorIs: testErr,
		},
		{
			name: "unexpected StatusCode",
			mockDo: func(req *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				recorder.WriteHeader(http.StatusNotFound)
				return recorder.Result(), nil
			},
			assertError: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrUnexpectedStatus)
				var statusErr *UnexpectedStatusError
				require.ErrorAs(t, err, &statusErr)
				assert.Equal(t, http.StatusNotFound, statusErr.StatusCode())
			},
		},
		{
			name:    "Unmarshal error",
			json:    "{ foo: bar }",
			wantErr: true,
		},
		{
			name: "Read error",
			mockDo: func(req *http.Request) (*http.Response, error) {
				resp := httptest.NewRecorder().Result()
				reader := mocks.NewMockReader(t)
				reader.EXPECT().Read(mock.Anything).Return(0, testErr)
				resp.Body = io.NopCloser(reader)
				return resp, nil
			},
			errorIs: testErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := mocks.NewMockHttpRequestDoer(t)
			if tt.mockDo != nil {
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(tt.mockDo)
			} else {
				httpClient.EXPECT().Do(mock.Anything).RunAndReturn(
					func(req *http.Request) (*http.Response, error) {
						recorder := httptest.NewRecorder()
						_, err := recorder.WriteString(tt.json)
						require.NoError(t, err)
						return recorder.Result(), nil
					})
			}

			c := testNew(t, WithHttpClient(httpClient))
			var wantIndex ArchiveIndex
			wantIndex.Directory.Name = "foobar"
			var gotIndex ArchiveIndex
			err := c.GetJSON(context.Background(), "https://localhost", &gotIndex)

			switch {
			case tt.assertError != nil:
				tt.assertError(t, err)
			case tt.errorIs != nil:
				require.ErrorIs(t, err, tt.errorIs)
			case tt.wantErr:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				assert.Equal(t, wantIndex, gotIndex)
			}
		})
	}
}

func TestClient_indexJsonURL(t *testing.T) {
	c := testNew(t)
	url, err := c.indexJsonURL("full-index")
	require.NoError(t, err)
	assert.Equal(t, archivesBaseURL+"/full-index/index.json", url)

	url, err = c.WithArchivesBaseURL(":localhost").indexJsonURL("full-index")
	require.Error(t, err)
	assert.Empty(t, url)
}

func TestClient_IndexArchive(t *testing.T) {
	fakeIndex := fakeArchiveIndex()
	jsonIndex, err := json.Marshal(&fakeIndex)
	require.NoError(t, err)
	assert.NotEmpty(t, jsonIndex)

	httpClient := mocks.NewMockHttpRequestDoer(t)
	httpClient.EXPECT().Do(mock.Anything).RunAndReturn(
		func(req *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			_, err := recorder.Write(jsonIndex)
			require.NoError(t, err)
			return recorder.Result(), nil
		})

	c := testNew(t, WithHttpClient(httpClient))
	gotIndex, err := c.IndexArchive(context.Background(), "full-index")
	require.NoError(t, err)
	assert.Equal(t, &fakeIndex, &gotIndex)

	gotIndex, err = c.WithArchivesBaseURL(":localhost").
		IndexArchive(context.Background(), "full-index")
	require.Error(t, err)
	var emptyIndex ArchiveIndex
	assert.Equal(t, &emptyIndex, &gotIndex)
}
