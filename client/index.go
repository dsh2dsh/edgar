package client

const indexJsonName = "index.json"

type ArchiveIndex struct {
	Directory struct {
		Item      []ArchiveItem `json:"item"`
		Name      string        `json:"name"`
		ParentDir string        `json:"parent-dir"`
	} `json:"directory"`
}

type ArchiveItem struct {
	LastModified string `json:"last-modified"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	Href         string `json:"href"`
	Size         string `json:"size"`
}

func (self *ArchiveIndex) Items() []ArchiveItem {
	return self.Directory.Item
}

func (self *ArchiveIndex) Name() string {
	return self.Directory.Name
}

func (self *ArchiveIndex) Parent() string {
	return self.Directory.ParentDir
}
