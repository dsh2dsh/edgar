package index

import (
	"fmt"
	"strconv"
	"time"
)

const dateFiledLayout = "2006-01-02"

type Item struct {
	CIK         uint32
	Filed       time.Time
	CompanyName string
	FormType    string
	Filename    string
}

func (self *Item) parseCIK(s string) error {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return fmt.Errorf("failed parse %q as CIK: %w", s, err)
	}
	self.CIK = uint32(v)
	return nil
}

func (self *Item) parseFiled(s string) error {
	filed, err := time.Parse(dateFiledLayout, s)
	if err != nil {
		return fmt.Errorf("failed parse %q as Date Filed: %w", s, err)
	}
	self.Filed = filed
	return nil
}
