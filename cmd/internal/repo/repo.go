package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func New(db Postgreser) *Repo {
	return &Repo{db: db}
}

type Repo struct {
	db Postgreser
}

type Postgreser interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (self *Repo) AddCompany(ctx context.Context, cik uint32, name string,
) (bool, error) {
	cmdTag, err := self.db.Exec(ctx, `
INSERT INTO companies (cik, entity_name)
  VALUES              ($1,  $2)
  ON CONFLICT DO NOTHING`, cik, name)
	if err != nil {
		return false, fmt.Errorf("add company CIK=%v %q: %w", cik, name, err)
	}
	return cmdTag.RowsAffected() > 0, nil
}

func (self *Repo) AddFact(ctx context.Context, tax, name string) (uint32, error) {
	rows, err := self.db.Query(ctx, `
INSERT INTO facts (fact_tax, fact_name)
  VALUES          ($1,       $2)
  ON CONFLICT DO NOTHING
  RETURNING id`, tax, name)
	if err != nil {
		return 0, fmt.Errorf("add fact \"%v:%v\": %w", tax, name, err)
	}

	id, err := pgx.CollectOneRow(rows, pgx.RowTo[uint32])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("collect one row: %w", err)
	}

	return id, nil
}
