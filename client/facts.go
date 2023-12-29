package client

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type CompanyFacts struct {
	CIK        Uint32String                      `json:"cik"`
	EntityName string                            `json:"entityName"`
	Facts      map[string]map[string]CompanyFact `json:"facts"`
}

func (self *CompanyFacts) Id() uint32 {
	return uint32(self.CIK)
}

type Uint32String uint32

func (self *Uint32String) UnmarshalJSON(b []byte) error {
	var value any
	if err := json.Unmarshal(b, &value); err != nil {
		return fmt.Errorf("client.Uint32String: %w", err)
	}

	if s, ok := value.(string); ok {
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return fmt.Errorf("client.Uint32String: %w", err)
		}
		*self = Uint32String(v)
		return nil
	}

	var v uint32
	if err := json.Unmarshal(b, &v); err != nil {
		return fmt.Errorf("client.Uint32String: %w", err)
	}
	*self = Uint32String(v)

	return nil
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
