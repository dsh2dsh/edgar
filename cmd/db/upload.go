package db

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/edgar/client"
	"github.com/dsh2dsh/edgar/internal/repo"
)

const retryNum = 2 // how many times repeat API call after 504

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
	LastFiled(ctx context.Context) (map[uint32]time.Time, error)
	FactLabels(ctx context.Context) ([]repo.FactLabels, error)
	Units(ctx context.Context) (map[uint32]string, error)
	FiledCounts(ctx context.Context, cik uint32) (map[time.Time]uint32, error)
	ReplaceFactUnits(ctx context.Context, cik uint32, lastFiled time.Time,
		length int, next func(i int) (repo.FactUnit, error)) error
	AddLastUpdate(ctx context.Context, at time.Time) error
	LastUpdated(ctx context.Context) (lastUpdated time.Time, err error)
}

type Upload struct {
	edgar  *client.Client
	logger *slog.Logger
	repo   Repo

	knownFacts facts
	knownUnits factUnits
	lastFiled  map[uint32]time.Time
	unknown    []client.CompanyTicker

	procs int
}

func (self *Upload) WithLogger(l *slog.Logger) *Upload {
	self.logger = l
	return self
}

func (self *Upload) WithProcsLimit(n int) *Upload {
	self.procs = n
	return self
}

func (self *Upload) log(ctx context.Context) *slog.Logger {
	if l := ContextLogger(ctx, nil); l != nil {
		return l
	} else if self.logger == nil {
		return slog.Default()
	}
	return self.logger
}

func (self *Upload) Upload() error {
	ctx := context.Background()
	if err := self.preloadArtifacts(ctx); err != nil {
		return err
	}

	if err := self.uploadUnknownCompanies(ctx); err != nil {
		return fmt.Errorf("upload facts: %w", err)
	}
	self.log(ctx).Info("upload completed")
	return nil
}

func (self *Upload) preloadArtifacts(ctx context.Context) error {
	if err := self.preloadFacts(ctx); err != nil {
		return err
	} else if err := self.preloadUnits(ctx); err != nil {
		return err
	}

	self.log(ctx).Info("preload last filed companies")
	if lastFiled, err := self.repo.LastFiled(ctx); err != nil {
		return fmt.Errorf("preload last filed: %w", err)
	} else {
		self.lastFiled = lastFiled
	}
	self.log(ctx).Info("preloaded last filed companies",
		slog.Int("len", len(self.lastFiled)))

	if companies, err := self.unknownCompanies(ctx); err != nil {
		return err
	} else {
		self.unknown = companies
	}
	return nil
}

func (self *Upload) preloadFacts(ctx context.Context) error {
	self.log(ctx).Info("preload facts and labels")
	factLabels, err := self.repo.FactLabels(ctx)
	if err != nil {
		return fmt.Errorf("preload facts and labels: %w", err)
	}

	var extraLabelsCnt int
	for i := range factLabels {
		item := &factLabels[i]
		factKey := self.makeFactKey(item.FactTax, item.FactName)
		unknownFact := self.knownFacts.Preload(item.FactId, factKey, item.LabelHash,
			item.DescrHash)
		if !unknownFact {
			extraLabelsCnt++
		}
	}
	self.log(ctx).Info("preloaded facts and labels",
		slog.Int("len", self.knownFacts.Len()), slog.Int("extra", extraLabelsCnt))
	return nil
}

func (self *Upload) preloadUnits(ctx context.Context) error {
	self.log(ctx).Info("preload units")
	units, err := self.repo.Units(ctx)
	if err != nil {
		return fmt.Errorf("preload units: %w", err)
	}

	for id, name := range units {
		self.knownUnits.Preload(id, name)
	}
	self.log(ctx).Info("preloaded units", slog.Int("len", len(units)))
	return nil
}

func (self *Upload) unknownCompanies(ctx context.Context,
) ([]client.CompanyTicker, error) {
	self.log(ctx).Info("looking for unknown companies")
	companies, err := self.companies(ctx)
	if err != nil {
		return nil, err
	}

	unknownIdx := slices.IndexFunc(companies,
		func(c client.CompanyTicker) bool { return !self.loadedCompany(c.CIK) })
	if unknownIdx < 0 {
		return nil, nil
	} else if unknownIdx > 0 {
		self.log(ctx).Info("skip loaded companies", slog.Int("skipped", unknownIdx))
	}

	unknownCompanies := companies[unknownIdx:]
	self.log(ctx).Info("found unknown companies", slog.Int("length",
		len(unknownCompanies)))
	return unknownCompanies, nil
}

func (self *Upload) companies(ctx context.Context) ([]client.CompanyTicker, error) {
	self.log(ctx).Info("fetch company tickers")
	companies, err := self.edgar.CompanyTickers(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch company tickers: %w", err)
	}
	self.log(ctx).Info("fetched tickers", slog.Int("length", len(companies)))
	return self.sortCompanies(ctx, companies), nil
}

func (self *Upload) sortCompanies(ctx context.Context,
	companies []client.CompanyTicker,
) []client.CompanyTicker {
	slices.SortFunc(companies, func(a, b client.CompanyTicker) int {
		switch {
		case self.loadedCompany(a.CIK) && self.loadedCompany(b.CIK):
			return cmp.Compare(a.CIK, b.CIK)
		case !self.loadedCompany(a.CIK) && !self.loadedCompany(b.CIK):
			return cmp.Compare(a.CIK, b.CIK)
		case !self.loadedCompany(b.CIK):
			return -1
		}
		return 1
	})

	uniqCompanies := slices.CompactFunc(companies,
		func(a, b client.CompanyTicker) bool { return a.CIK == b.CIK })
	if len(uniqCompanies) < len(companies) {
		self.log(ctx).Info("compactified tickers",
			slog.Int("before", len(companies)), slog.Int("after", len(uniqCompanies)))
	}

	return uniqCompanies
}

func (self *Upload) makeFactKey(tax, name string) string {
	return strings.Join([]string{tax, name}, ":")
}

func (self *Upload) loadedCompany(cik uint32) bool {
	_, ok := self.lastFiled[cik]
	return ok
}

func (self *Upload) uploadUnknownCompanies(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(self.procs)

	for i := 0; i < len(self.unknown); i++ {
		if ctx.Err() != nil {
			break
		}
		company := &self.unknown[i]
		cik, title := company.CIK, company.Title
		l := self.log(ctx).With(
			slog.String("progress", fmt.Sprintf("%v/%v", i+1, len(self.unknown))),
			slog.Uint64("CIK", uint64(cik)))
		g.Go(func() error {
			return self.processCompanyFacts(ContextWithLogger(ctx, l), cik, title)
		})
	}
	return g.Wait() //nolint:wrapcheck // returned not from external package
}

func (self *Upload) processCompanyFacts(ctx context.Context, cik uint32,
	title string,
) error {
	self.log(ctx).Info("fetch company facts", slog.String("title", title))
	companyFacts, err := self.companyFacts(ctx, cik, title)
	if err != nil {
		return err
	} else if companyFacts == nil {
		return nil
	}

	err = self.iterateCompanyFacts(ctx, cik, companyFacts, self.addFactUnits)
	if err != nil {
		return fmt.Errorf("processCompanyFacts: %w", err)
	}
	return nil
}

func (self *Upload) companyFacts(ctx context.Context, cik uint32, title string,
) (map[string]map[string]client.CompanyFact, error) {
	facts, err := self.retryCompanyFacts(ctx, retryNum, cik)
	if err != nil {
		var s *client.UnexpectedStatusError
		if errors.As(err, &s) && s.StatusCode() == http.StatusNotFound {
			self.log(ctx).Info("skip company", slog.Any("cause", err))
			return nil, nil
		}
		return nil, err
	}

	if facts.EntityName == "" {
		self.log(ctx).LogAttrs(ctx, slog.LevelWarn, "empty entityName",
			slog.String("title", title))
	} else {
		title = facts.EntityName
	}

	if facts.Id() != cik {
		self.log(ctx).LogAttrs(ctx, slog.LevelWarn, "wrong cik",
			slog.Uint64("cik", uint64(facts.Id())))
	}

	unknownCompany, err := self.repo.AddCompany(ctx, cik, title)
	if err != nil {
		return nil, fmt.Errorf("companyFacts: %w", err)
	} else if unknownCompany {
		self.log(ctx).Info("add company")
	}

	return facts.Facts, nil
}

func (self *Upload) retryCompanyFacts(ctx context.Context, tries int, cik uint32,
) (facts client.CompanyFacts, err error) {
	var skipErr error
	ok, err := self.retryFunc(ctx, tries,
		func(ctx context.Context, i int) (bool, error) {
			if facts, err = self.edgar.CompanyFacts(ctx, cik); err == nil {
				return true, nil
			}
			var s *client.UnexpectedStatusError
			if errors.As(err, &s) && s.StatusCode() == http.StatusGatewayTimeout {
				self.log(ctx).Info("retry company facts", slog.Any("cause", err))
				skipErr = err
				return false, nil
			}
			return false, fmt.Errorf(
				"failed fetch company facts (CIK=%v): %w", cik, err)
		})
	if err == nil && !ok {
		err = fmt.Errorf("tried may times fetch company facts: %w", skipErr)
	}
	return
}

func (self *Upload) retryFunc(ctx context.Context, max int,
	fn func(ctx context.Context, i int) (bool, error),
) (bool, error) {
	for i := 0; i < max; i++ {
		if ctx.Err() != nil {
			return false, fmt.Errorf("stop retrying: %w", ctx.Err())
		}
		l := self.log(ctx).With(slog.Int("try", i+1))
		if ok, err := fn(ContextWithLogger(ctx, l), i); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

func (self *Upload) iterateCompanyFacts(ctx context.Context, cik uint32,
	companyFacts map[string]map[string]client.CompanyFact,
	fn func(ctx context.Context, cik, factId, unitId uint32,
		factUnits []client.FactUnit) error,
) error {
	for taxName, facts := range companyFacts {
		for factName, fact := range facts {
			factId, err := self.addFact(ctx, taxName, factName, fact.Label,
				fact.Description)
			if err != nil {
				return fmt.Errorf("iterateCompanyFacts: company CIK=%v: %w", cik, err)
			}
			for unitName, factUnits := range fact.Units {
				unitId, err := self.addUnit(ctx, unitName)
				if err != nil {
					return err
				}
				err = fn(ctx, cik, factId, unitId, factUnits)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (self *Upload) addFact(ctx context.Context, tax, name, label, descr string,
) (uint32, error) {
	factKey := self.makeFactKey(tax, name)
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
		return fact.Id, fact.AddLabel(labelHash, descrHash, func() error {
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
