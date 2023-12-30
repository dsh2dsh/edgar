package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string,
		rowSrc pgx.CopyFromSource) (int64, error)
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

func (self *Repo) CopyFactUnits(ctx context.Context, len int,
	next func(i int) (FactUnit, error),
) error {
	colNames := []string{
		"company_cik", "fact_id", "unit_id", "fact_start", "fact_end", "val",
		"accn", "fy", "fp", "form", "filed", "frame",
	}
	n, err := self.db.CopyFrom(ctx, pgx.Identifier{"fact_units"}, colNames,
		pgx.CopyFromSlice(len, func(i int) ([]any, error) {
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
		return fmt.Errorf("failed copy %v fact units: %w", len, err)
	} else if n != int64(len) {
		return fmt.Errorf("copied %v fact units instead of %v", n, len)
	}
	return nil
}

type FactUnit struct {
	CIK    uint32 `db:"company_cik"`
	FactId uint32 `db:"fact_id"`
	UnitId uint32 `db:"unit_id"`

	Start pgtype.Date `db:"fact_start"`
	End   time.Time   `db:"fact_end"`
	Val   float64     `db:"val"`
	Accn  string      `db:"accn"`
	FY    uint        `db:"fy"`
	FP    string      `db:"fp"`
	Form  string      `db:"form"`
	Filed time.Time   `db:"filed"`
	Frame pgtype.Text `db:"frame"`
}

func (self *FactUnit) WithStart(d time.Time) *FactUnit {
	self.Start = pgtype.Date{Time: d, Valid: true}
	return self
}

func (self *FactUnit) WithFrame(frame string) *FactUnit {
	self.Frame = pgtype.Text{String: frame, Valid: true}
	return self
}

func (self *FactUnit) NamedArgs() pgx.NamedArgs {
	return pgx.NamedArgs{
		"company_cik": self.CIK,
		"fact_id":     self.FactId,
		"unit_id":     self.UnitId,

		"fact_start": self.Start,
		"fact_end":   self.End,
		"val":        self.Val,
		"accn":       self.Accn,
		"fy":         self.FY,
		"fp":         self.FP,
		"filed":      self.Filed,
		"form":       self.Form,
		"frame":      self.Frame,
	}
}
