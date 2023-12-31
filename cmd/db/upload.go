package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/edgar/client"
	"github.com/dsh2dsh/edgar/internal/repo"
)

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
