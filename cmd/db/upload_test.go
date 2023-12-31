package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dsh2dsh/edgar/client"
	"github.com/dsh2dsh/edgar/internal/repo"
)

func TestUpload_repoFactUnit(t *testing.T) {
	const appleCIK = 320193
	const factId = 1
	const unitId = 2

	clientFact := client.FactUnit{
		Start: "2008-09-27",
		End:   "2008-09-27",
		Val:   5520000000,
		Accn:  "0001193125-09-153165",
		FY:    2009,
		FP:    "Q3",
		Form:  "10-Q",
		Filed: "2009-07-22",
		Frame: "CY2008Q3I",
	}

	repoFact := repo.FactUnit{
		CIK:    appleCIK,
		FactId: factId,
		UnitId: unitId,
		End:    time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC),
		Val:    5520000000,
		Accn:   "0001193125-09-153165",
		FY:     2009,
		FP:     "Q3",
		Form:   "10-Q",
		Filed:  time.Date(2009, 7, 22, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name     string
		prepare  func(t *testing.T, fact *client.FactUnit)
		repoFact func() repo.FactUnit
		wantErr  bool
	}{
		{
			name: "ok",
			repoFact: func() repo.FactUnit {
				fact := repoFact
				fact.WithStart(time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC)).
					WithFrame("CY2008Q3I")
				return fact
			},
		},
		{
			name: "without Start",
			prepare: func(t *testing.T, fact *client.FactUnit) {
				fact.Start = ""
			},
			repoFact: func() repo.FactUnit {
				fact := repoFact
				fact.WithFrame("CY2008Q3I")
				return fact
			},
		},
		{
			name: "without Frame",
			prepare: func(t *testing.T, fact *client.FactUnit) {
				fact.Frame = ""
			},
			repoFact: func() repo.FactUnit {
				fact := repoFact
				fact.WithStart(time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC))
				return fact
			},
		},
		{
			name: "invalid date",
			prepare: func(t *testing.T, fact *client.FactUnit) {
				fact.Filed = "not a date"
			},
			repoFact: func() repo.FactUnit {
				fact := repoFact
				fact.WithFrame("CY2008Q3I")
				return fact
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fact := clientFact
			if tt.prepare != nil {
				tt.prepare(t, &fact)
			}
			u := Upload{}
			gotFact, err := u.repoFactUnit(appleCIK, factId, unitId, &fact)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.repoFact(), gotFact)
			}
		})
	}
}
