package index

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func newDownloadDir(datadir string) *downloadDir {
	return &downloadDir{datadir: datadir}
}

type downloadDir struct {
	datadir string
}

func (self *downloadDir) Save(path, fname string, r io.Reader) error {
	if err := self.makePath(path); err != nil {
		return err
	}

	path = filepath.Join(self.datadir, path, fname)
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("failed create %q: %w", path, err)
	}
	defer w.Close()

	_, err = io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("failed write into %q: %w", path, err)
	}

	return nil
}

func (self *downloadDir) makePath(path string) error {
	dir, err := os.Stat(self.datadir)
	if err != nil {
		return fmt.Errorf("makePath %q: %w", self.datadir, err)
	} else if !dir.IsDir() {
		return fmt.Errorf("makePath: %q not a directory", self.datadir)
	}

	path = filepath.Join(self.datadir, path)
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", path, err)
	}

	return nil
}
