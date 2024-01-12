package index

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestItem_parseCIK(t *testing.T) {
	var item Item
	require.NoError(t, item.parseCIK("123"))
	assert.Equal(t, uint32(123), item.CIK)
	require.ErrorIs(t, item.parseCIK("12.3"), strconv.ErrSyntax)
}

func TestItem_parseFiled(t *testing.T) {
	var item Item
	require.NoError(t, item.parseFiled("2023-01-02"))
	assert.Equal(t, time.Date(2023, time.January, 2, 0, 0, 0, 0, time.UTC), item.Filed)
	require.Error(t, item.parseFiled("2023"))
}
