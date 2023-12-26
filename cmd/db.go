package cmd

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/caarlos0/env/v10"
	"github.com/cespare/xxhash/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/dsh2dsh/edgar/client"
	"github.com/dsh2dsh/edgar/internal/repo"
)

const uploadProcs = 4 // number of parallel uploads

var (
	// SchemaSQL contains db/schema.sql via main.go
	SchemaSQL string

	databaseCmd = cobra.Command{
		Use:   "db",
		Short: "Database staff",
		Long: `All sub-commands require EDGAR_DB_URL environment variable set:

  EDGAR_DB_URL="postgres://username:password@localhost:5432/database_name"

Before using any of sub-commands, please create database:

  $ createuser -U postgres -e -P edgar
  $ createdb -U postgres -O edgar -E UTF8 --locale en_US.UTF-8 -T template0 edgar

and initialize it:

  $ edgar db init
`,
	}

	initCmd = cobra.Command{
		Use:   "init",
		Short: "Initialize database before first usage",
		Run: func(cmd *cobra.Command, args []string) {
			cobra.CheckErr(createTables(SchemaSQL))
			log.Println("all done.")
		},
	}

	uploadCmd = cobra.Command{
		Use:   "upload",
		Short: "Fetch all companies and their facts from EDGAR API",
		Run: func(cmd *cobra.Command, args []string) {
			connURL, err := connString()
			cobra.CheckErr(err)

			ctx := context.Background()
			db, err := pgxpool.New(ctx, connURL)
			cobra.CheckErr(err)
			defer db.Close()
			cobra.CheckErr(db.Ping(ctx))

			edgar, err := newClient()
			cobra.CheckErr(err)

			uploader := NewUpload(edgar, repo.New(db)).WithProcsLimit(uploadProcs)
			cobra.CheckErr(uploader.Upload())
		},
	}
)

func init() {
	databaseCmd.AddCommand(&initCmd)
	databaseCmd.AddCommand(&uploadCmd)
	rootCmd.AddCommand(&databaseCmd)
}

func createTables(scheme string) error {
	connURL, err := connString()
	if err != nil {
		return err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	_, err = conn.Exec(ctx, scheme)
	if err != nil {
		return fmt.Errorf("create DB scheme': %w", err)
	}

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("close DB': %w", err)
	}

	return nil
}

func connString() (string, error) {
	cfg := struct {
		ConnURL string `env:"EDGAR_DB_URL,notEmpty"`
	}{}
	if err := env.Parse(&cfg); err != nil {
		return "", fmt.Errorf("parse edgar envs: %w", err)
	}
	return cfg.ConnURL, nil
}

func NewUpload(edgar *client.Client, repo *repo.Repo) *Upload {
	return &Upload{
		edgar: edgar,
		repo:  repo,

		group: new(singleflight.Group),
		procs: 1,
	}
}

type Upload struct {
	edgar *client.Client
	repo  *repo.Repo

	group *singleflight.Group
	mu    sync.RWMutex
	procs int

	knownFacts map[string]*knownFact
}

func (self *Upload) WithProcsLimit(n int) *Upload {
	self.procs = n
	return self
}

func (self *Upload) Upload() error {
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(self.procs)

	log.Println("requesting list of companies...")
	companies, err := self.edgar.CompanyTickers(ctx)
	if err != nil {
		return fmt.Errorf("list of companies: %w", err)
	}
	log.Printf("got %v companies", len(companies))

	for i, company := range companies {
		if ctx.Err() != nil {
			break
		}
		cik := company.CIK
		log.Printf("processing company %v/%v CIK=%v: %v (%v)...", i, len(companies), cik,
			company.Ticker, company.Title)
		g.Go(func() error { return self.processCompany(ctx, cik) })
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("upload edgar facts: %w", err)
	}
	return nil
}

func (self *Upload) processCompany(ctx context.Context, cik uint32) error {
	company, err := self.edgar.CompanyFacts(ctx, cik)
	if err != nil {
		return fmt.Errorf("facts of CIK=%v: %w", cik, err)
	}
	log.Printf("got facts of CIK=%v: %q", company.CIK, company.EntityName)

	isCompanyNew, err := self.repo.AddCompany(ctx, company.CIK, company.EntityName)
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	} else if isCompanyNew {
		log.Printf("new company added: CIK=%v %q", company.CIK, company.EntityName)
	}

	for taxName, facts := range company.Facts {
		for factName, fact := range facts {
			_, err := self.addFact(ctx, taxName, factName, fact.Label,
				fact.Description)
			if err != nil {
				return fmt.Errorf("add fact for company CIK=%v: %w", company.CIK, err)
			}
		}
	}

	return nil
}

func (self *Upload) addFact(ctx context.Context, tax, name, label, descr string,
) (uint32, error) {
	labelHash := xxhash.Sum64String(label)
	descrHash := xxhash.Sum64String(descr)

	factKey, fact, ok := self.knownFact(tax, name)
	if !ok {
		fact, err := self.createFact(ctx, factKey, tax, name, labelHash, descrHash)
		return fact.Id, err
	}

	err := fact.AddLabel(ctx, labelHash, descrHash, func() error {
		log.Printf("creating new label for fact %q: %#x, %#x...",
			factKey, labelHash, descrHash)
		//nolint:wrapcheck // well wrap below
		return self.repo.AddLabel(ctx, fact.Id, label, descr, labelHash, descrHash)
	})
	if err != nil {
		err = fmt.Errorf("failed add label for fact %q: %w", factKey, err)
	}

	return fact.Id, err
}

func (self *Upload) knownFact(tax, name string) (key string, fact *knownFact, ok bool) {
	key = strings.Join([]string{tax, name}, ":")
	self.mu.RLock()
	defer self.mu.RUnlock()
	fact, ok = self.knownFacts[key]
	return
}

func (self *Upload) createFact(ctx context.Context, key, tax, name string,
	labelHash, descrHash uint64,
) (*knownFact, error) {
	v, err, _ := self.group.Do(key, func() (interface{}, error) {
		log.Printf("creating new fact %q...", key)
		factId, err := self.repo.AddFact(ctx, tax, name)
		if err != nil {
			return nil, fmt.Errorf("failed create fact %q: %w", key, err)
		}
		fact := &knownFact{
			Id:        factId,
			LabelHash: labelHash,
			DescrHash: descrHash,
		}
		self.mu.Lock()
		defer self.mu.Unlock()
		self.knownFacts[key] = fact
		return fact, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already wrapped inside Do
	}

	return v.(*knownFact), nil
}

// --------------------------------------------------

type knownFact struct {
	Id        uint32
	LabelHash uint64
	DescrHash uint64

	moreLabels map[uint64]map[uint64]struct{}
	mu         sync.Mutex
}

func (self *knownFact) AddLabel(ctx context.Context, labelHash, descrHash uint64,
	callback func() error,
) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.LabelHash == labelHash && self.DescrHash == descrHash {
		return nil
	} else if len(self.moreLabels) > 0 {
		if descrMap, ok := self.moreLabels[labelHash]; ok {
			if _, ok = descrMap[descrHash]; ok {
				return nil
			}
		}
	}

	if err := callback(); err != nil {
		return fmt.Errorf("callback: %w", err)
	}

	if self.moreLabels == nil {
		self.moreLabels = map[uint64]map[uint64]struct{}{
			labelHash: {descrHash: {}},
		}
	} else if _, ok := self.moreLabels[labelHash]; !ok {
		self.moreLabels[labelHash] = map[uint64]struct{}{descrHash: {}}
	} else {
		self.moreLabels[labelHash][descrHash] = struct{}{}
	}
	return nil
}
