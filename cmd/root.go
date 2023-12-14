package cmd

import (
	"fmt"

	"github.com/caarlos0/env/v10"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/spf13/cobra"

	"github.com/dsh2dsh/edgar/client"
)

var (
	edgarDataDir string

	rootCmd = cobra.Command{
		Use:   "edgar",
		Short: "Download data files from SEC EDGAR",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return loadEnvs()
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&edgarDataDir, "datadir", "d", "./",
		"store EDGAR files into this directory")
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func loadEnvs() error {
	if err := dotenv.New().WithDepth(1).Load(); err != nil {
		return fmt.Errorf("load edgar envs: %w", err)
	}
	return nil
}

func newClient() (*client.Client, error) {
	cfg := struct {
		UA string `env:"EDGAR_UA,notEmpty"`
	}{}
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse edgar envs: %w", err)
	}
	return client.New().WithUserAgent(cfg.UA), nil
}
