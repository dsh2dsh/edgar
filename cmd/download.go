package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/edgar/client"
)

var downloadCmd = cobra.Command{
	Use:   "download indexPath files...",
	Short: "Recursively download files from EDGAR's /Archives/indexPath",
	Example: `
  - Download all master.gz files from full-index:

    $ edgar download edgar/full-index master.gz`,
	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := newClient()
		if err != nil {
			return err
		}
		return NewDownload(client, edgarDataDir).Download(args[0], args[1:])
	},
}

func init() {
	rootCmd.AddCommand(&downloadCmd)
}

func NewDownload(client *client.Client, dataDir string) *Download {
	return &Download{
		client:  client,
		datadir: dataDir,
	}
}

type Download struct {
	client  *client.Client
	datadir string

	storage *downloadDir
}

func (self *Download) Download(path string, needFiles []string) error {
	self.storage = newDownloadDir(self.datadir, needFiles)
	return nil
}

func newDownloadDir(datadir string, needFiles []string) *downloadDir {
	d := &downloadDir{
		datadir:   datadir,
		needFiles: make(map[string]struct{}, len(needFiles)),
	}

	for _, fname := range needFiles {
		d.needFiles[fname] = struct{}{}
	}

	return d
}

type downloadDir struct {
	datadir   string
	needFiles map[string]struct{}
}

func (self *downloadDir) NeedFile(fname string) bool {
	if len(self.needFiles) == 0 {
		return true
	}
	_, ok := self.needFiles[fname]
	return ok
}

func (self *downloadDir) Save(path, fname string, r io.Reader) error {
	if err := self.makePath(path); err != nil {
		return err
	}

	path = filepath.Join(self.datadir, path, fname)
	w, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o755)
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
