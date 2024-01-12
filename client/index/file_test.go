package index

import (
	"compress/gzip"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFile_Headers(t *testing.T) {
	wantHeaders := map[string]string{
		"Description":        "Master Index of EDGAR Dissemination Feed",
		"Last Data Received": "January 11, 2024",
		"Comments":           "webmaster@sec.gov",
		"Anonymous FTP":      "ftp://ftp.sec.gov/edgar/",
		"Cloud HTTP":         "https://www.sec.gov/Archives/",
	}
	indexFile := newTestFile(t)
	assert.Equal(t, wantHeaders, indexFile.Headers())

	headers := indexFile.Headers()
	headers["foo"] = "bar"
	assert.Equal(t, wantHeaders, indexFile.Headers())
}

func newTestFile(t *testing.T) File {
	file, err := os.Open("testdata/master.gz")
	require.NoError(t, err)

	zr, err := gzip.NewReader(file)
	require.NoError(t, err)

	indexFile := NewFile(zr)
	require.NoError(t, indexFile.ReadHeaders())

	return indexFile
}

func TestFile_LastFiled(t *testing.T) {
	indexFile := newTestFile(t)
	assert.Equal(t, time.Date(2024, time.January, 11, 0, 0, 0, 0, time.UTC),
		indexFile.LastFiled())
}

func TestFile_FieldNames(t *testing.T) {
	wantNames := []string{"CIK", "Company Name", "Form Type", "Date Filed", "Filename"}
	indexFile := newTestFile(t)
	names := indexFile.FieldNames()
	assert.Equal(t, wantNames, names)

	names[0] = ""
	assert.Equal(t, wantNames, indexFile.FieldNames())
}

func TestFile_Iterate(t *testing.T) {
	indexFile := newTestFile(t)
	var minFiled, maxFiled time.Time
	var cnt int
	err := indexFile.Iterate(func(item *Item) error {
		cnt++
		if minFiled.IsZero() || item.Filed.Before(minFiled) {
			minFiled = item.Filed
		}
		if maxFiled.IsZero() || item.Filed.After(maxFiled) {
			maxFiled = item.Filed
		}
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, 38824, cnt)
	wantMin := time.Date(2024, time.January, 2, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, wantMin, minFiled)
	wantMax := time.Date(2024, time.January, 11, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, wantMax, maxFiled)
}

func TestFile_CompaniesLastFiled(t *testing.T) {
	indexFile := newTestFile(t)
	lastFiled, err := indexFile.CompaniesLastFiled()
	require.NoError(t, err)
	assert.Len(t, lastFiled, 17318)
	assert.Equal(t, time.Date(2024, time.January, 10, 0, 0, 0, 0, time.UTC),
		lastFiled[1000045])
	assert.Equal(t, time.Date(2024, time.January, 11, 0, 0, 0, 0, time.UTC),
		lastFiled[9984])
}
