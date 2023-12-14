//go:build e2e

package client

import (
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
