package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArchiveIndex(t *testing.T) {
	index := fakeArchiveIndex()
	assert.Equal(t, index.Directory.Item, index.Items())
	assert.Equal(t, "full-index/", index.Name())
	assert.Equal(t, "../", index.Parent())
}

func fakeArchiveIndex() (index ArchiveIndex) {
	index.Directory.Item = []ArchiveItem{
		{
			LastModified: "12/09/2023 01:01:44 AM",
			Name:         "1993",
			Type:         "dir",
			Href:         "1993/",
			Size:         "4 KB",
		},
		{
			LastModified: "12/12/2023 10:07:17 PM",
			Name:         "2023",
			Type:         "dir",
			Href:         "2023/",
			Size:         "4 KB",
		},
		{
			LastModified: "12/12/2023 10:06:25 PM",
			Name:         "company.gz",
			Type:         "file",
			Href:         "company.gz",
			Size:         "2933 KB",
		},
	}
	index.Directory.Name = "full-index/"
	index.Directory.ParentDir = "../"
	return
}
