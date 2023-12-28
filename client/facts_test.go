package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFactUnit_ParseTimes(t *testing.T) {
	const unparseableTime = "unparseable time"
	testFact := FactUnit{
		Start: "2008-09-27",
		End:   "2008-09-27",
		Filed: "2009-07-22",
	}

	tests := []struct {
		name        string
		fact        FactUnit
		prepare     func(t *testing.T, fact *FactUnit)
		wantErr     bool
		expectTimes [3]time.Time
	}{
		{
			name: "ok",
			fact: testFact,
			expectTimes: [...]time.Time{
				time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC),
				time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC),
				time.Date(2009, 7, 22, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "empty start",
			fact: testFact,
			prepare: func(t *testing.T, fact *FactUnit) {
				var s string
				fact.Start = s
			},
			expectTimes: [...]time.Time{
				{},
				time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC),
				time.Date(2009, 7, 22, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "error start",
			fact: testFact,
			prepare: func(t *testing.T, fact *FactUnit) {
				fact.Start = unparseableTime
			},
			wantErr: true,
		},
		{
			name: "error end",
			fact: testFact,
			prepare: func(t *testing.T, fact *FactUnit) {
				fact.End = unparseableTime
			},
			wantErr: true,
		},
		{
			name: "error filed",
			fact: testFact,
			prepare: func(t *testing.T, fact *FactUnit) {
				fact.Filed = unparseableTime
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fact := tt.fact
			if tt.prepare != nil {
				tt.prepare(t, &fact)
			}
			err := fact.ParseTimes(func(startTime, endTime, filedTime time.Time) {
				gotTimes := [...]time.Time{startTime, endTime, filedTime}
				assert.Equal(t, tt.expectTimes, gotTimes)
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
