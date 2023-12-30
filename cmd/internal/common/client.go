package common

import (
	"fmt"

	"github.com/caarlos0/env/v10"

	"github.com/dsh2dsh/edgar/client"
)

func NewClient() (*client.Client, error) {
	cfg := struct {
		UA string `env:"EDGAR_UA,notEmpty"`
	}{}
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse edgar envs: %w", err)
	}
	return client.New().WithUserAgent(cfg.UA), nil
}
