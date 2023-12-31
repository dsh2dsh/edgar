package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func createTables(scheme string) error {
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

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("close DB': %w", err)
	}

	return nil
}
