package client

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompanyFacts_Id(t *testing.T) {
	facts := CompanyFacts{CIK: CIK(1895262)}
	assert.Equal(t, uint32(1895262), facts.Id())
}

func TestCompanyFacts_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
		want    CompanyFacts
	}{
		{
			name: "CIK as number",
			json: `{ "cik": 1895262 }`,
			want: CompanyFacts{CIK: CIK(1895262)},
		},
		{
			name: "CIK as string",
			json: `{ "cik": "0001895262" }`,
			want: CompanyFacts{CIK: CIK(1895262)},
		},
		{
			name:    "CIK string error",
			json:    `{ "cik": "1895262.123" }`,
			wantErr: true,
		},
		{
			name:    "CIK number error",
			json:    `{ "cik": 1895262.123 }`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got CompanyFacts
			err := json.Unmarshal([]byte(tt.json), &got)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestUint32String_UnmarshalJSON_error(t *testing.T) {
	var cik CIK
	require.Error(t, cik.UnmarshalJSON([]byte{}))
}

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
