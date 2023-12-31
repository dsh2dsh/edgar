//go:build e2e

package client

import (
	"bufio"
	"compress/gzip"
	"context"
	"testing"

	"github.com/caarlos0/env/v10"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/stretchr/testify/suite"
)

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

type ClientTestSuite struct {
	suite.Suite
	client *Client
}

func (self *ClientTestSuite) SetupSuite() {
	cfg := struct {
		UA string `env:"EDGAR_UA,notEmpty"`
	}{}
	self.Require().NoError(dotenv.Load(func() error { return env.Parse(&cfg) }))
	self.client = New().WithUserAgent(cfg.UA)
}

func (self *ClientTestSuite) TestFullIndex() {
	fullIndex, err := self.client.IndexArchive(
		context.Background(), "edgar/full-index")
	self.Require().NoError(err)
	self.Equal("full-index/", fullIndex.Name())
	self.Equal("../", fullIndex.Parent())

	items := fullIndex.Items()
	self.NotEmpty(items)
	self.Equal("1993", items[0].Name)
	self.Equal("dir", items[0].Type)
}

func (self *ClientTestSuite) TestFullIndexFile() {
	resp, err := self.client.GetArchiveFile(context.Background(),
		"edgar/full-index/xbrl.gz")
	self.Require().NoError(err)
	defer resp.Body.Close()

	zr, err := gzip.NewReader(resp.Body)
	self.Require().NoError(err)

	scanner := bufio.NewScanner(zr)
	self.Require().True(scanner.Scan())
	self.Require().NoError(scanner.Err())
	self.Equal("Description:           XBRL Index of EDGAR Dissemination Feed",
		scanner.Text())
}

func (self *ClientTestSuite) TestCompanyTickers() {
	tickers, err := self.client.CompanyTickers(context.Background())
	self.Require().NoError(err)
	self.NotEmpty(tickers)
	self.NotEmpty(tickers[0].CIK)
}

func (self *ClientTestSuite) TestCompanyFacts() {
	const appleCIK = 320193
	facts, err := self.client.CompanyFacts(context.Background(), appleCIK)
	self.Require().NoError(err)
	self.Equal(uint32(appleCIK), facts.CIK)
	self.Equal("Apple Inc.", facts.EntityName)

	self.Contains(facts.Facts, "dei")
	self.NotEmpty(facts.Facts["dei"])

	self.Require().Contains(facts.Facts, "us-gaap")
	usGaap := facts.Facts["us-gaap"]
	self.NotEmpty(usGaap)

	self.Require().Contains(usGaap, "AccountsPayable")
	fact := usGaap["AccountsPayable"]
	self.NotEmpty(fact)
	self.Equal("Accounts Payable (Deprecated 2009-01-31)", fact.Label)

	self.Require().Contains(fact.Units, "USD")
	usd := fact.Units["USD"]
	self.NotEmpty(usd)
}
