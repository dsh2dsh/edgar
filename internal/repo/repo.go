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
	makeErr := func(err error) error {
		return fmt.Errorf("add fact \"%v:%v\": %w", tax, name, err)
	}

	rows, err := self.db.Query(ctx, `
INSERT INTO facts (fact_tax, fact_name)
  VALUES          ($1,       $2)
  ON CONFLICT DO NOTHING
  RETURNING id`, tax, name)
	if err != nil {
		return 0, makeErr(err)
	}

	if id, err := pgx.CollectOneRow(rows, pgx.RowTo[uint32]); err == nil {
		return id, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, makeErr(err)
	}

	rows, err = self.db.Query(ctx,
		`SELECT id FROM facts WHERE fact_tax = $1 AND fact_name = $2`, tax, name)
	if err != nil {
		return 0, makeErr(err)
	}

	id, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[uint32])
	if err != nil {
		return 0, makeErr(err)
	}

	return id, nil
}

func (self *Repo) AddLabel(ctx context.Context, factId uint32,
	label, descr string, labelHash, descrHash uint64,
) error {
	_, err := self.db.Exec(ctx, `
INSERT INTO fact_labels (fact_id, fact_label, descr, xxhash1, xxhash2)
  VALUES                ($1,      $2,         $3,    $4,      $5)
  ON CONFLICT DO NOTHING`,
		factId, label, descr, labelHash, descrHash)
	if err != nil {
		return fmt.Errorf("add fact label for %v: %w", factId, err)
	}
	return nil
}
