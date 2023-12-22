package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/caarlos0/env/v10"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

var (
	SchemaSQL string

	databaseCmd = cobra.Command{
		Use:   "db",
		Short: "Database staff",
		Long: `All sub-commands require EDGAR_DB_URL environment variable set:

  EDGAR_DB_URL="postgres://username:password@localhost:5432/database_name"

Before using any of sub-commands, please create database:

  $ createuser -U postgres -e -P edgar
  $ createdb -U postgres -O edgar -E UTF8 --locale en_US.UTF-8 -T template0 edgar

and initialize it:

  $ edgar db init
`,
	}

	initCmd = cobra.Command{
		Use:   "init",
		Short: "Initialize database before first usage",
		Run: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(createDBTables(SchemaSQL))
			log.Println("all done.")
		},
	}

	uploadCmd = cobra.Command{
		Use:   "upload",
		Short: "Fetch all companies and their facts from EDGAR API",
		Run: func(cmd *cobra.Command, args []string) {
		},
	}
)

func init() {
	databaseCmd.AddCommand(&initCmd)
	databaseCmd.AddCommand(&uploadCmd)
	rootCmd.AddCommand(&databaseCmd)
}

func createDBTables(scheme string) error {
	connURL, err := connString()
	if err != nil {
		return err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	_, err = conn.Exec(ctx, scheme)
	if err != nil {
		return fmt.Errorf("create DB scheme': %w", err)
	}

	return nil
}

func connString() (string, error) {
	cfg := struct {
		ConnURL string `env:"EDGAR_DB_URL,notEmpty"`
	}{}
	if err := env.Parse(&cfg); err != nil {
		return "", fmt.Errorf("parse edgar envs: %w", err)
	}
	return cfg.ConnURL, nil
}
