package cmd

import (
	"fmt"

	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/spf13/cobra"

	"github.com/dsh2dsh/edgar/cmd/db"
	"github.com/dsh2dsh/edgar/cmd/index"
)

var rootCmd = cobra.Command{
	Use:   "edgar",
	Short: "Download data files from SEC EDGAR",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return loadEnvs()
	},
}

func init() {
	rootCmd.AddCommand(&db.Cmd)
	rootCmd.AddCommand(&index.Cmd)
}

func Execute(version string) {
	rootCmd.Version = version
	cobra.CheckErr(rootCmd.Execute())
}

func loadEnvs() error {
	if err := dotenv.New().WithDepth(1).Load(); err != nil {
		return fmt.Errorf("load edgar envs: %w", err)
	}
	return nil
}
