package repo

import (
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

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
