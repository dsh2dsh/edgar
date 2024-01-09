package index

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/edgar/client"
)

const (
	downloadProcs = 10 // Number of parallel downloads
	edgarPath     = "edgar"
)

func NewDownload(client *client.Client, st Storage) *Download {
	return &Download{
		client:  client,
		storage: st,
		procs:   1,
	}
}

type Download struct {
	client  *client.Client
	storage Storage

	needFiles map[string]struct{}
	procs     int
}

type Storage interface {
	Save(path, fname string, r io.Reader) error
}

func (self *Download) WithNeedFiles(needFiles []string) *Download {
	self.needFiles = make(map[string]struct{}, len(needFiles))
	for _, fname := range needFiles {
		self.needFiles[fname] = struct{}{}
	}
	return self
}

func (self *Download) WithProcsLimit(lim int) *Download {
	self.procs = lim
	return self
}

func (self *Download) Download(path string) error {
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(self.procs)

	if err := self.processIndex(ctx, path, g); err != nil {
		return err
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("download of %v: %w", path, err)
	}

	return nil
}

func (self *Download) processIndex(ctx context.Context, path string,
	g *errgroup.Group,
) error {
	index, skipPath, err := self.readIndex(ctx, path)
	if err != nil {
		return err
	} else if skipPath {
		return nil
	}
	log.Printf("got index of %v: %v items...", path, len(index.Items()))

	for _, item := range index.Items() {
		if ctx.Err() != nil {
			return nil
		}
		handler, err := self.itemHandler(ctx, path, item)
		if err != nil {
			return err
		} else if g != nil {
			g.Go(handler)
		} else if err := handler(); err != nil {
			return err
		}
	}

	return nil
}

func (self *Download) readIndex(ctx context.Context, path string,
) (index client.ArchiveIndex, skip bool, err error) {
	log.Printf("go into %v", path)
	index, err = self.client.IndexArchive(ctx, path)
	if err != nil {
		var statusErr *client.UnexpectedStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode() == http.StatusForbidden {
			skip, err = true, nil
			log.Printf("skip %v: %v", path, statusErr)
			return
		}
		err = fmt.Errorf("index of %q: %w", path, err)
	}
	return
}

func (self *Download) itemHandler(ctx context.Context, path string,
	item client.ArchiveItem,
) (h func() error, err error) {
	fullPath, err := url.JoinPath(path, item.Name)
	if err != nil {
		return nil, fmt.Errorf("url join of %v and %v: %w",
			path, item.Name, err)
	}

	switch item.Type {
	case "dir":
		h = func() error { return self.processIndex(ctx, fullPath, nil) }
	case "file":
		h = func() error {
			if self.NeedFile(item.Name) {
				return self.downloadFile(ctx, path, item.Name, fullPath)
			}
			return nil
		}
	}
	return
}

func (self *Download) NeedFile(fname string) bool {
	if len(self.needFiles) == 0 {
		return true
	}
	_, ok := self.needFiles[fname]
	return ok
}

func (self *Download) downloadFile(ctx context.Context, parentPath, fname,
	fullPath string,
) error {
	resp, err := self.client.GetArchiveFile(ctx, fullPath)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("download %v", fullPath)
	if err = self.storage.Save(parentPath, fname, resp.Body); err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	return nil
}
