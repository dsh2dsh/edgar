package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewQtr(t *testing.T) {
	var qtrs [12]int
	for month := 1; month <= 12; month++ {
		date := time.Date(2023, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		qtr := NewQtr(date)
		qtrs[month-1] = qtr.qtr
	}
	assert.Equal(t, [...]int{1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4}, qtrs)
}

func TestQtr_Path(t *testing.T) {
	qtr := NewQtr(time.Date(2023, time.October, 25, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, "2023/QTR4", qtr.Path())
}

func TestQtr_Next(t *testing.T) {
	qtr := NewQtr(time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC))
	var paths [4]string
	for i := 0; i < len(paths); i++ {
		paths[i] = qtr.Next()
	}
	wantPaths := [...]string{"2023/QTR2", "2023/QTR3", "2023/QTR4", "2024/QTR1"}
	assert.Equal(t, wantPaths, paths)
}
