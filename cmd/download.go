package cmd

import (
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
		return NewDownload(args[0], args[1:]).WithClient(client).
			WithDataDir(edgarDataDir).Run()
	},
}

func init() {
	rootCmd.AddCommand(&downloadCmd)
}

func NewDownload(indexPath string, files []string) *Download {
	return &Download{
		files:     files,
		indexPath: indexPath,
	}
}

type Download struct {
	client  *client.Client
	datadir string

	files     []string
	indexPath string
}

func (self *Download) WithClient(c *client.Client) *Download {
	self.client = c
	return self
}

func (self *Download) WithDataDir(dir string) *Download {
	self.datadir = dir
	return self
}

func (self *Download) Run() error {
	return nil
}
