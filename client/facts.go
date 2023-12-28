package client

import (
	"fmt"
	"time"
)

type CompanyFacts struct {
	CIK        uint32                            `json:"cik"`
	EntityName string                            `json:"entityName"`
	Facts      map[string]map[string]CompanyFact `json:"facts"`
}

type CompanyFact struct {
	Label       string                `json:"label"`
	Description string                `json:"description"`
	Units       map[string][]FactUnit `json:"units"`
}

type FactUnit struct {
	Start string  `json:"start"`
	End   string  `json:"end"`
	Val   float64 `json:"val"`
	Accn  string  `json:"accn"`
	FY    uint    `json:"fy"`
	FP    string  `json:"fp"`
	Form  string  `json:"form"`
	Filed string  `json:"filed"`
	Frame string  `json:"frame"`
}

func (self *FactUnit) StartTime() (time.Time, error) {
	return self.parseDate(self.Start, "start")
}

func (self *FactUnit) parseDate(d, field string) (t time.Time, err error) {
	if d == "" {
		return
	}
	t, err = time.Parse("2006-01-02", d)
	if err != nil {
		err = fmt.Errorf("parse %q = %q: %w", field, d, err)
	}
	return
}

func (self *FactUnit) EndTime() (time.Time, error) {
	return self.parseDate(self.End, "end")
}

func (self *FactUnit) FiledTime() (time.Time, error) {
	return self.parseDate(self.Filed, "filed")
}

func (self *FactUnit) ParseTimes(fn func(startTime, endTime, filedTime time.Time),
) error {
	startTime, err := self.StartTime()
	if err != nil {
		return err
	}

	endTime, err := self.EndTime()
	if err != nil {
		return err
	}

	filedTime, err := self.FiledTime()
	if err != nil {
		return err
	}

	fn(startTime, endTime, filedTime)
	return nil
}
