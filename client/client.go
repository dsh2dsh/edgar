// Package client implements API access to EDGAR data. It's rate limited, as
// required by [1], and uses User-Agent. Don't forget set correct User-Agent
// using WithUserAgent().
//
// Also see [2] for details.
//
// [1]: https://www.sec.gov/os/accessing-edgar-data
// [2]: https://www.sec.gov/edgar/sec-api-documentation
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/time/rate"
)

const (
	apiBaseURL            = "https://data.sec.gov"
	archivesBaseURL       = "https://www.sec.gov/Archives"
	companyFactsURI       = "/api/xbrl/companyfacts/CIK%010d.json"
	companyTickersJsonURL = "https://www.sec.gov/files/company_tickers.json"
	indexJsonName         = "index.json"

	// Default access rate for EDGAR, see
	// https://www.sec.gov/os/webmaster-faq#code-support
	//
	// Note that our current maximum access rate is 10 requests per second.
	limitRate = 10
)

// Doer performs HTTP requests.
//
// The standard http.Client implements this interface.
type HttpRequestDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type Limiter interface{ Wait(context.Context) error }

func New(opts ...ClientOption) *Client {
	c := &Client{apiBaseURL: apiBaseURL}
	return c.applyOptions(opts...)
}

type ClientOption func(c *Client)

func WithHttpClient(client HttpRequestDoer) ClientOption {
	return func(c *Client) { c.client = client }
}

func WithRateLimiter(l Limiter) ClientOption {
	return func(c *Client) { c.limiter = l }
}

type Client struct {
	client  HttpRequestDoer
	limiter Limiter
	ua      string

	apiBaseURL       string
	archrivesBaseUrl string
}

func (self *Client) applyOptions(opts ...ClientOption) *Client {
	for _, fn := range opts {
		fn(self)
	}

	if self.client == nil {
		self.client = &http.Client{Timeout: 5 * time.Second}
	}

	if self.limiter == nil {
		self.limiter = rate.NewLimiter(limitRate, limitRate)
	}

	return self
}

func (self *Client) WithApiBaseURL(u string) *Client {
	self.apiBaseURL = u
	return self
}

func (self *Client) WithArchivesBaseURL(url string) *Client {
	self.archrivesBaseUrl = url
	return self
}

func (self *Client) ArchivesBaseURL() string {
	if self.archrivesBaseUrl == "" {
		return archivesBaseURL
	}
	return self.archrivesBaseUrl
}

func (self *Client) WithUserAgent(ua string) *Client {
	self.ua = ua
	return self
}

func (self *Client) Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create new GET request for %q: %w", url, err)
	}
	req.Header.Add("User-Agent", self.ua)

	if err := self.limitRate(ctx); err != nil {
		return nil, fmt.Errorf("rate limit GET %s: %w", url, err)
	}

	resp, err := self.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}

	return resp, nil
}

func (self *Client) limitRate(ctx context.Context) error {
	if self.limiter != nil {
		if err := self.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("wait: %w", err)
		}
	}
	return nil
}

func (self *Client) GetJSON(ctx context.Context, url string, value any) error {
	resp, err := self.Get(ctx, url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode > maxExpectedStatusCode {
		return fmt.Errorf("GET %s: %w", url, newUnexpectedStatusError(resp))
	}
	if err != nil {
		return fmt.Errorf("read body from GET %s: %w", url, err)
	}

	if err := json.Unmarshal(body, value); err != nil {
		return fmt.Errorf("unmarshal GET %s: %w", url, err)
	}

	return nil
}

func (self *Client) IndexArchive(ctx context.Context, path string,
) (index ArchiveIndex, err error) {
	url, err := self.indexJsonURL(path)
	if err != nil {
		return
	}
	err = self.GetJSON(ctx, url, &index)
	return
}

func (self *Client) indexJsonURL(path string) (string, error) {
	url, err := url.JoinPath(self.ArchivesBaseURL(), path, indexJsonName)
	if err != nil {
		return "", fmt.Errorf("join path %q: %w", path, err)
	}
	return url, nil
}

func (self *Client) GetArchiveFile(ctx context.Context, path string,
) (*http.Response, error) {
	url, err := url.JoinPath(self.ArchivesBaseURL(), path)
	if err != nil {
		return nil, fmt.Errorf("join path %q: %w", path, err)
	}
	return self.Get(ctx, url)
}

func (self *Client) CompanyTickers(ctx context.Context) ([]CompanyTicker, error) {
	var tickersMap companyTickers
	if err := self.GetJSON(ctx, companyTickersJsonURL, &tickersMap); err != nil {
		return nil, err
	}

	allTickers := make([]CompanyTicker, 0, len(tickersMap))
	for _, v := range tickersMap {
		allTickers = append(allTickers, v)
	}
	return allTickers, nil
}

func (self *Client) CompanyFacts(ctx context.Context, cik uint32,
) (facts CompanyFacts, err error) {
	jsonName := fmt.Sprintf(companyFactsURI, cik)
	url, err := url.JoinPath(self.apiBaseURL, jsonName)
	if err != nil {
		err = fmt.Errorf("join %q, %q: %w", self.apiBaseURL, jsonName, err)
		return
	}
	err = self.GetJSON(ctx, url, &facts)
	return
}
