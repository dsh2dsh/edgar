package db

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/caarlos0/env/v10"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/dsh2dsh/edgar/cmd/internal/common"
	"github.com/dsh2dsh/edgar/internal/repo"
)

const uploadProcs = 4 // number of parallel uploads

var (
	// SchemaSQL contains db/schema.sql via main.go
	SchemaSQL string

	Cmd = cobra.Command{
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
			cobra.CheckErr(createTables(SchemaSQL))
			log.Println("all done.")
		},
	}

	uploadCmd = cobra.Command{
		Use:   "upload",
		Short: "Fetch all companies and their facts from EDGAR API",
		Run: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(withUpload(func(u *Upload) error { return u.Upload() }))
		},
	}

	updateCmd = cobra.Command{
		Use:   "update",
		Short: "Fetch new facts for all known companies from EDGAR API",
		Run: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(withUpload(func(u *Upload) error { return u.Update() }))
		},
	}
)

//nolint:wrapcheck // we'll pass error as is to cobra.CheckErr()
func withUpload(fn func(u *Upload) error) error {
	connURL, err := connString()
	if err != nil {
		return err
	}

	ctx := context.Background()
	db, err := pgxpool.New(ctx, connURL)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		return err
	}

	edgar, err := common.NewClient()
	if err != nil {
		return err
	}

	uploader := NewUpload(edgar, repo.New(db)).
		WithLogger(slog.Default()).WithProcsLimit(uploadProcs)
	return fn(uploader)
}

func init() {
	Cmd.AddCommand(&initCmd)
	Cmd.AddCommand(&uploadCmd)
	Cmd.AddCommand(&updateCmd)
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
