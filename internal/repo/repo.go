package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var factUnitCols = [...]string{
	"company_cik", "fact_id", "unit_id", "fact_start", "fact_end", "val", "accn",
	"fy", "fp", "form", "filed", "frame",
}

func New(db Postgreser) *Repo {
	return &Repo{db: db}
}

type Repo struct {
	db Postgreser
}

type Postgreser interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string,
		rowSrc pgx.CopyFromSource) (int64, error)
	Begin(ctx context.Context) (pgx.Tx, error)
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

func (self *Repo) AddUnit(ctx context.Context, name string) (uint32, error) {
	makeErr := func(err error) error {
		return fmt.Errorf("add unit %q: %w", name, err)
	}

	rows, err := self.db.Query(ctx, `
INSERT INTO units (unit_name)
  VALUES          ($1)
  ON CONFLICT DO NOTHING
  RETURNING id`, name)
	if err != nil {
		return 0, makeErr(err)
	}

	if id, err := pgx.CollectOneRow(rows, pgx.RowTo[uint32]); err == nil {
		return id, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return 0, makeErr(err)
	}

	rows, err = self.db.Query(ctx,
		`SELECT id FROM units WHERE unit_name = $1`, name)
	if err != nil {
		return 0, makeErr(err)
	}

	id, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[uint32])
	if err != nil {
		return 0, makeErr(err)
	}

	return id, nil
}

func (self *Repo) AddFactUnit(ctx context.Context, fact FactUnit) error {
	_, err := self.db.Exec(ctx, `
INSERT INTO fact_units (company_cik,  fact_id,   unit_id,
                        fact_start,   fact_end,  val,      accn,  fy,  fp,
                        form,         filed,     frame)
  VALUES               (@company_cik, @fact_id,  @unit_id,
                        @fact_start,  @fact_end, @val,     @accn, @fy, @fp,
                        @form,        @filed,    @frame)`, fact.NamedArgs())
	if err != nil {
		return fmt.Errorf("failed add fact unit: %w", err)
	}
	return nil
}

func (self *Repo) CopyFactUnits(ctx context.Context, length int,
	next func(i int) (FactUnit, error),
) error {
	return self.copyFactUnits(ctx, self.db, length, next)
}

func (self *Repo) copyFactUnits(ctx context.Context, conn Postgreser,
	length int, next func(i int) (FactUnit, error),
) error {
	n, err := conn.CopyFrom(ctx, pgx.Identifier{"fact_units"}, factUnitCols[:],
		pgx.CopyFromSlice(length, func(i int) ([]any, error) {
			fact, err := next(i)
			if err != nil {
				return nil, err
			}
			values := []any{
				fact.CIK, fact.FactId, fact.UnitId, fact.Start, fact.End, fact.Val,
				fact.Accn, fact.FY, fact.FP, fact.Form, fact.Filed, fact.Frame,
			}
			return values, nil
		}))
	if err != nil {
		return fmt.Errorf("failed copy %v fact units: %w", length, err)
	} else if n != int64(length) {
		return fmt.Errorf("copied %v fact units instead of %v", n, length)
	}
	return nil
}

func (self *Repo) LastFiled(ctx context.Context) (map[uint32]time.Time, error) {
	rows, err := self.db.Query(ctx, `
SELECT company_cik, MAX(filed) AS last_filed
  FROM fact_units GROUP BY company_cik`)
	if err != nil {
		return nil, fmt.Errorf("repo.LastFiled: %w", err)
	}

	type lastFiled struct {
		CIK   uint32    `db:"company_cik"`
		Filed time.Time `db:"last_filed"`
	}

	cikFiled, err := pgx.CollectRows(rows, pgx.RowToStructByName[lastFiled])
	if err != nil {
		return nil, fmt.Errorf("repo.LastFiled: %w", err)
	}

	filedByCIK := make(map[uint32]time.Time, len(cikFiled))
	for i := range cikFiled {
		item := &cikFiled[i]
		filedByCIK[item.CIK] = item.Filed
	}

	return filedByCIK, nil
}

func (self *Repo) FactLabels(ctx context.Context) ([]FactLabels, error) {
	rows, err := self.db.Query(ctx, `
SELECT facts.id AS fact_id, fact_tax, fact_name,
       fact_labels.id AS label_id, xxhash1, xxhash2
  FROM facts, fact_labels WHERE facts.id = fact_labels.fact_id`)
	if err != nil {
		return nil, fmt.Errorf("repo.FactLabels: %w", err)
	}

	facts, err := pgx.CollectRows(rows, pgx.RowToStructByName[FactLabels])
	if err != nil {
		return nil, fmt.Errorf("repo.FactLabels: %w", err)
	}

	return facts, nil
}

func (self *Repo) Units(ctx context.Context) (map[uint32]string, error) {
	rows, err := self.db.Query(ctx, `SELECT id, unit_name FROM units`)
	if err != nil {
		return nil, fmt.Errorf("repo.Units: %w", err)
	}

	type unitItem struct {
		Id       uint32 `db:"id"`
		UnitName string `db:"unit_name"`
	}

	unitItems, err := pgx.CollectRows(rows, pgx.RowToStructByName[unitItem])
	if err != nil {
		return nil, fmt.Errorf("repo.FactLabels: %w", err)
	}

	units := make(map[uint32]string, len(unitItems))
	for _, item := range unitItems {
		units[item.Id] = item.UnitName
	}
	return units, nil
}

func (self *Repo) FiledCounts(ctx context.Context, cik uint32,
) (map[time.Time]uint32, error) {
	rows, err := self.db.Query(ctx, `
SELECT filed, COUNT(*) AS facts FROM fact_units
  WHERE company_cik = $1
  GROUP BY company_cik, filed`, cik)
	if err != nil {
		return nil, fmt.Errorf("repo.FiledForms: %w", err)
	}

	type filedCount struct {
		Filed time.Time `db:"filed"`
		Facts uint32    `db:"facts"`
	}

	filedCounts, err := pgx.CollectRows(rows, pgx.RowToStructByName[filedCount])
	if err != nil {
		return nil, fmt.Errorf("repo.FiledCounts: %w", err)
	}

	counts := make(map[time.Time]uint32, len(filedCounts))
	for _, item := range filedCounts {
		counts[item.Filed] = item.Facts
	}
	return counts, nil
}

func (self *Repo) ReplaceFactUnits(ctx context.Context, cik uint32,
	lastFiled time.Time, length int, next func(i int) (FactUnit, error),
) error {
	err := pgx.BeginFunc(ctx, self.db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
DELETE FROM fact_units WHERE company_cik = $1 AND filed >= $2`, cik, lastFiled)
		if err != nil {
			return err //nolint:wrapcheck // wrap it below
		}
		return self.copyFactUnits(ctx, tx, length, next)
	})
	if err != nil {
		return fmt.Errorf("repo.ReplaceFactUnits: %w", err)
	}
	return nil
}
