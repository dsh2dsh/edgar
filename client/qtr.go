package client

import (
	"path/filepath"
	"strconv"
	"time"
)

func NewQtr(date time.Time) Qtr {
	y, m, _ := date.Date()
	return Qtr{year: y, qtr: monthQtr(int(m))}
}

type Qtr struct {
	year, qtr int
}

func monthQtr(month int) int {
	if month%3 > 0 {
		return month/3 + 1
	}
	return month / 3
}

func (self *Qtr) Path() string {
	return filepath.Join(strconv.Itoa(self.year), self.QTR())
}

func (self *Qtr) QTR() string {
	return "QTR" + strconv.Itoa(self.qtr)
}

func (self *Qtr) Next() string {
	if self.qtr == 4 {
		self.year++
		self.qtr = 1
	} else {
		self.qtr++
	}
	return self.Path()
}
