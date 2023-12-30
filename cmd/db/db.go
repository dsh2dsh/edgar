package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/caarlos0/env/v10"
	"github.com/cespare/xxhash/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/dsh2dsh/edgar/client"
	"github.com/dsh2dsh/edgar/cmd/internal/common"
	"github.com/dsh2dsh/edgar/internal/repo"
)

const uploadProcs = 4 // number of parallel uploads

var (
	// SchemaSQL contains db/schema.sql via main.go
	SchemaSQL string

	Cmd = cobra.Command{
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

			edgar, err := common.NewClient()
			cobra.CheckErr(err)

			uploader := NewUpload(edgar, repo.New(db)).WithProcsLimit(uploadProcs)
			cobra.CheckErr(uploader.Upload())
		},
	}
)

func init() {
	Cmd.AddCommand(&initCmd)
	Cmd.AddCommand(&uploadCmd)
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

func NewUpload(edgar *client.Client, repo Repo) *Upload {
	return &Upload{
		edgar: edgar,
		repo:  repo,

		knownFacts: newFacts(),
		knownUnits: newFactUnits(),

		procs: 1,
	}
}

type Repo interface {
	AddCompany(ctx context.Context, cik uint32, name string) (bool, error)
	AddFact(ctx context.Context, tax, name string) (uint32, error)
	AddLabel(ctx context.Context, factId uint32, label, descr string,
		labelHash, descrHash uint64) error
	AddUnit(ctx context.Context, name string) (uint32, error)
	AddFactUnit(ctx context.Context, fact repo.FactUnit) error
	CopyFactUnits(ctx context.Context, length int,
		next func(i int) (repo.FactUnit, error)) error
}

type Upload struct {
	edgar *client.Client
	repo  Repo

	knownFacts facts
	knownUnits factUnits

	procs int
}

func (self *Upload) WithProcsLimit(n int) *Upload {
	self.procs = n
	return self
}

func (self *Upload) Upload() error {
	g, ctx := errgroup.WithContext(context.Background())
	g.SetLimit(self.procs)

	log.Println("fetch companies")
	companies, err := self.edgar.CompanyTickers(ctx)
	if err != nil {
		return fmt.Errorf("fetch companies: %w", err)
	}
	log.Printf("%v companies", len(companies))

	for i, company := range companies {
		if i == 2 { // TODO: remove
			break
		}
		if ctx.Err() != nil {
			break
		}
		cik := company.CIK
		log.Printf("%v/%v: company CIK=%v %q: %q", i+1, len(companies), cik,
			company.Ticker, company.Title)
		g.Go(func() error { return self.processCompanyFacts(ctx, cik) })
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("upload facts: %w", err)
	}
	return nil
}

func (self *Upload) processCompanyFacts(ctx context.Context, cik uint32) error {
	company, err := self.companyFacts(ctx, cik)
	if err != nil {
		var status *client.UnexpectedStatusError
		if errors.As(err, &status) && status.StatusCode() == http.StatusNotFound {
			log.Printf("skip company: %s", err)
			return nil
		}
		return err
	}

	for taxName, facts := range company.Facts {
		for factName, fact := range facts {
			factId, err := self.addFact(ctx, taxName, factName, fact.Label,
				fact.Description)
			if err != nil {
				return fmt.Errorf("processCompanyFacts: company CIK=%v: %w",
					company.CIK, err)
			}
			for unitName, factUnits := range fact.Units {
				unitId, err := self.addUnit(ctx, unitName)
				if err != nil {
					return err
				}
				log.Printf(`%v facts of CIK=%v, "%v:%v", %q`, len(factUnits),
					company.CIK, taxName, factName, unitName)
				err = self.addFactUnits(ctx, company.Id(), factId, unitId, factUnits)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (self *Upload) companyFacts(ctx context.Context, cik uint32,
) (client.CompanyFacts, error) {
	facts, err := self.edgar.CompanyFacts(ctx, cik)
	if err != nil {
		return facts, fmt.Errorf("facts of CIK=%v: %w", cik, err)
	}
	log.Printf("got facts: CIK=%v: %q", facts.CIK, facts.EntityName)

	unknownCompany, err := self.repo.AddCompany(ctx, facts.Id(), facts.EntityName)
	if err != nil {
		return facts, fmt.Errorf("companyFacts: %w", err)
	} else if unknownCompany {
		log.Printf("add company: CIK=%v %q", facts.CIK, facts.EntityName)
	}

	return facts, nil
}

func (self *Upload) addFact(ctx context.Context, tax, name, label, descr string,
) (uint32, error) {
	factKey := strings.Join([]string{tax, name}, ":")
	labelHash := xxhash.Sum64String(label)
	descrHash := xxhash.Sum64String(descr)

	addLabel := func(factId uint32) error {
		log.Printf("add label fact %q: label=%#x, descr=%#x", factKey, labelHash,
			descrHash)
		err := self.repo.AddLabel(ctx, factId, label, descr, labelHash, descrHash)
		if err != nil {
			return fmt.Errorf("failed add label fact %q: %w", factKey, err)
		}
		return nil
	}

	if fact, ok := self.knownFacts.Fact(factKey); ok {
		return fact.Id, fact.AddLabel(ctx, labelHash, descrHash, func() error {
			return addLabel(fact.Id)
		})
	}

	fact, err := self.knownFacts.Create(factKey, labelHash, descrHash,
		func() (uint32, error) {
			log.Printf("add fact %q: label=%#x, descr=%#x", factKey, labelHash,
				descrHash)
			factId, err := self.repo.AddFact(ctx, tax, name)
			if err != nil {
				return 0, err //nolint:wrapcheck // will wrap below
			}
			return factId, addLabel(factId)
		})
	if err != nil {
		return 0, fmt.Errorf("failed add fact %q: %w", factKey, err)
	}
	return fact.Id, nil
}

func (self *Upload) addUnit(ctx context.Context, name string) (uint32, error) {
	unitId, err := self.knownUnits.Id(ctx, name, func() (uint32, error) {
		log.Printf("add unit %q", name)
		//nolint:wrapcheck // will wrap below
		return self.repo.AddUnit(ctx, name)
	})
	if err != nil {
		return unitId, fmt.Errorf("failed add unit %q: %w", name, err)
	}
	return unitId, nil
}

func (self *Upload) addFactUnits(ctx context.Context, cik uint32,
	factId, unitId uint32, clientFacts []client.FactUnit,
) error {
	err := self.repo.CopyFactUnits(ctx, len(clientFacts),
		func(i int) (repo.FactUnit, error) {
			return self.repoFactUnit(cik, factId, unitId, &clientFacts[i])
		})
	if err != nil {
		return fmt.Errorf("failed add %v facts: cik=%v, factId=%v, unitId=%v: %w",
			len(clientFacts), cik, factId, unitId, err)
	}
	return nil
}

func (self *Upload) repoFactUnit(cik uint32, factId, unitId uint32,
	clientFact *client.FactUnit,
) (repo.FactUnit, error) {
	fact := repo.FactUnit{
		CIK:    cik,
		FactId: factId,
		UnitId: unitId,
		End:    time.Time{},
		Val:    clientFact.Val,
		Accn:   clientFact.Accn,
		FY:     clientFact.FY,
		FP:     clientFact.FP,
		Form:   clientFact.Form,
		Filed:  time.Time{},
	}

	if clientFact.Frame != "" {
		fact.WithFrame(clientFact.Frame)
	}

	err := clientFact.ParseTimes(func(startTime, endTime, filedTime time.Time) {
		if !startTime.IsZero() {
			fact.WithStart(startTime)
		}
		fact.End = endTime
		fact.Filed = filedTime
	})
	if err != nil {
		return fact, fmt.Errorf("convert FactUnit from client to repo: %w", err)
	}

	return fact, nil
}

// --------------------------------------------------

func newFacts() facts {
	return facts{knownFacts: make(map[string]*knownFact, 0)}
}

type facts struct {
	knownFacts map[string]*knownFact
	group      singleflight.Group
	mu         sync.RWMutex
}

func (self *facts) Fact(key string) (fact *knownFact, ok bool) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	fact, ok = self.knownFacts[key]
	return
}

func (self *facts) Create(key string, labelHash, descrHash uint64,
	genFactId func() (uint32, error),
) (*knownFact, error) {
	v, err, _ := self.group.Do(key, func() (interface{}, error) {
		if fact, ok := self.Fact(key); ok {
			return fact, nil
		}
		factId, err := genFactId()
		if err != nil {
			return nil, err
		}
		fact := newKnownFact(factId, labelHash, descrHash)
		self.mu.Lock()
		defer self.mu.Unlock()
		self.knownFacts[key] = fact
		return fact, nil
	})

	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped inside genFactId
	}
	return v.(*knownFact), nil
}

// --------------------------------------------------

func newKnownFact(id uint32, labelHash, descrHash uint64) *knownFact {
	return &knownFact{
		Id:        id,
		LabelHash: labelHash,
		DescrHash: descrHash,
	}
}

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
	if self.LabelHash == labelHash && self.DescrHash == descrHash {
		return nil
	}

	self.mu.Lock()
	defer self.mu.Unlock()

	if len(self.moreLabels) > 0 {
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

// --------------------------------------------------

func newFactUnits() factUnits {
	return factUnits{units: make(map[string]uint32, 0)}
}

type factUnits struct {
	units map[string]uint32
	group singleflight.Group
	mu    sync.RWMutex
}

func (self *factUnits) Id(ctx context.Context, name string,
	genUnitId func() (uint32, error),
) (uint32, error) {
	if unitId, ok := self.knownUnit(name); ok {
		return unitId, nil
	}
	return self.createUnit(ctx, name, genUnitId)
}

func (self *factUnits) knownUnit(name string) (id uint32, ok bool) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	id, ok = self.units[name]
	return
}

func (self *factUnits) createUnit(ctx context.Context, name string,
	genUnitId func() (uint32, error),
) (uint32, error) {
	v, err, _ := self.group.Do(name, func() (interface{}, error) {
		if unitId, ok := self.knownUnit(name); ok {
			return unitId, nil
		}

		id, err := genUnitId()
		if err != nil {
			return 0, err
		}

		self.mu.Lock()
		defer self.mu.Unlock()
		self.units[name] = id
		return id, nil
	})

	if err != nil {
		return 0, err //nolint:wrapcheck // wrapped inside genUnitId
	}
	return v.(uint32), nil
}
