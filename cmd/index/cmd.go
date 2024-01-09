package index

import (
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/dsh2dsh/edgar/cmd/internal/common"
)

var (
	edgarDataDir string

	Cmd = cobra.Command{
		Use:   "archive",
		Short: "EDGAR index files",
	}

	downloadCmd = cobra.Command{
		Use:   "download index [files...]",
		Short: "Recursively download files from EDGAR's /Archives/edgar/index",
		Example: `
  - Download all master.gz files from full-index:

    $ edgar index download full-index master.gz

  - Download all files from daily-index:

    $ edgar index download daily-index`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			client, err := common.NewClient()
			cobra.CheckErr(err)
			d := NewDownload(client, newDownloadDir(edgarDataDir)).
				WithProcsLimit(downloadProcs)
			if len(args) > 1 {
				d.WithNeedFiles(args[1:])
			}
			cobra.CheckErr(d.Download(filepath.Join(edgarPath, args[0])))
		},
	}
)

func init() {
	Cmd.AddCommand(&downloadCmd)
	downloadCmd.Flags().StringVarP(&edgarDataDir, "datadir", "d", "./",
		"store EDGAR files into this directory")
}
