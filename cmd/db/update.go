package db

import (
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dsh2dsh/edgar/client"
	"github.com/dsh2dsh/edgar/client/index"
	"github.com/dsh2dsh/edgar/internal/repo"
)

const (
	indexPath   = "edgar/full-index"
	masterIndex = "master.gz"
)

func (self *Upload) Update() error {
	ctx := context.Background()
	lastUpdated, err := self.preloadUpdateArtefacts(ctx)
	if err != nil {
		return err
	}

	if err := self.updateWithProgress(ctx); err != nil {
		return err
	}

	if len(self.unknown) > 0 {
		self.log(ctx).Info("upload new companies", slog.Int("length",
			len(self.unknown)))
		if err := self.uploadUnknownCompanies(ctx); err != nil {
			return fmt.Errorf("upload new companies: %w", err)
		}
	}

	if err := self.saveLastUpdated(ctx, lastUpdated); err != nil {
		return err
	}
	self.log(ctx).Info("update completed")
	return nil
}

func (self *Upload) preloadUpdateArtefacts(ctx context.Context,
) (lastUpdated time.Time, err error) {
	if err = self.preloadArtifacts(ctx); err != nil {
		return
	}

	if lastUpdated, err = self.repo.LastUpdated(ctx); err != nil {
		err = fmt.Errorf("failed get last updated: %w", err)
		return
	} else if lastUpdated.IsZero() {
		lastUpdated = self.mostRecentFiled()
	}

	lastUpdated, err = self.refreshLastFiled(ctx, lastUpdated)
	if err != nil {
		return
	}
	return
}

func (self *Upload) mostRecentFiled() time.Time {
	var t time.Time
	for _, lastFiled := range self.lastFiled {
		if lastFiled.After(t) {
			t = lastFiled
		}
	}
	return t
}

func (self *Upload) refreshLastFiled(ctx context.Context, since time.Time,
) (lastUpdated time.Time, err error) {
	self.log(ctx).Info("looking for updated companies", slog.String("since",
		since.Format(time.DateOnly)))

	masterPath := filepath.Join(indexPath, masterIndex)
	lastUpdated, fillings, err := self.indexFillings(ctx, masterPath)
	if err != nil {
		return
	}

	self.log(ctx).Info("EDGAR index last updated",
		slog.String("at", lastUpdated.Format(time.DateOnly)),
		slog.String("path", masterPath))

	updateCompanies, err := self.hasUpdatesUntil(ctx, since, lastUpdated,
		self.hasUpdates(since, fillings, make(map[uint32]struct{},
			len(self.lastFiled))))
	if err != nil {
		return
	}

	self.purgeLastFiled(ctx, updateCompanies)
	self.log(ctx).Info("got updated companies",
		slog.Int("length", len(updateCompanies)),
		slog.Int("actual", len(self.lastFiled)))
	self.log(ctx).Info("got new companies", slog.Int("length", len(self.unknown)))
	return
}

func (self *Upload) indexFillings(ctx context.Context, path string,
) (lastFiled time.Time, companies map[uint32]time.Time, err error) {
	l := self.log(ctx).With(slog.String("path", path))
	l.Info("fetch index file")

	resp, err := self.edgar.GetArchiveFile(ctx, path)
	if err != nil {
		err = fmt.Errorf("failed fetch index file %q: %w", path, err)
		return
	}
	defer resp.Body.Close()

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		err = fmt.Errorf("failed gunzip %q: %w", path, err)
		return
	}

	f := index.NewFile(gz)
	if err = f.ReadHeaders(); err != nil {
		err = fmt.Errorf("failed read headers from %q: %w", path, err)
		return
	}

	companies, err = f.CompaniesLastFiled()
	if err != nil {
		err = fmt.Errorf("failed fetch companies from %q: %w", path, err)
		return
	}
	lastFiled = f.LastFiled()
	l.Info("fetched index file",
		slog.String("lastFiled", lastFiled.Format(time.DateOnly)),
		slog.Int("companies", len(companies)))
	return
}

func (self *Upload) hasUpdates(since time.Time, fillings map[uint32]time.Time,
	companies map[uint32]struct{},
) map[uint32]struct{} {
	for cik, filed := range fillings {
		if !filed.Before(since) {
			companies[cik] = struct{}{}
		}
	}
	return companies
}

func (self *Upload) hasUpdatesUntil(ctx context.Context, since time.Time,
	lastUpdated time.Time, companies map[uint32]struct{},
) (map[uint32]struct{}, error) {
	self.log(ctx).Info("checking index files for updates",
		slog.String("since", since.Format(time.DateOnly)),
		slog.String("until", lastUpdated.Format(time.DateOnly)))
	qtr := client.NewQtr(since)
	lastQtr := client.NewQtr(lastUpdated)

	for path := qtr.Path(); ; path = qtr.Next() {
		masterPath := filepath.Join(indexPath, path, masterIndex)
		_, fillings, err := self.indexFillings(ctx, masterPath)
		if err != nil {
			return nil, err
		}
		companies = self.hasUpdates(since, fillings, companies)
		if path == lastQtr.Path() {
			break
		}
	}
	return companies, nil
}

func (self *Upload) purgeLastFiled(ctx context.Context,
	updateCompanies map[uint32]struct{},
) {
	for cik := range self.lastFiled {
		if _, ok := updateCompanies[cik]; !ok {
			delete(self.lastFiled, cik)
		}
	}

	if len(self.unknown) > 0 {
		self.unknown = slices.DeleteFunc(self.unknown,
			func(c client.CompanyTicker) bool {
				_, ok := updateCompanies[c.CIK]
				return !ok
			})
	}
}

func (self *Upload) updateWithProgress(ctx context.Context) error {
	if len(self.lastFiled) == 0 {
		self.log(ctx).Info("nothing to update")
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var progress atomic.Uint32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		self.logProgress(ctx, &progress)
	}()

	self.log(ctx).Info("update all known companies")
	err := self.updateKnownCompanies(ctx, &progress)
	cancel()
	wg.Wait()
	return err
}

func (self *Upload) logProgress(ctx context.Context, progress *atomic.Uint32) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	self.log(ctx).Info("start periodic progress logging")
	for {
		select {
		case <-ctx.Done():
			self.log(ctx).Info("stop periodic progress logging")
			return
		case <-tick.C:
			self.log(ctx).Info("looking for new facts",
				slog.String("progress",
					fmt.Sprintf("%v/%v", progress.Load(), len(self.lastFiled))))
		}
	}
}

func (self *Upload) updateKnownCompanies(ctx context.Context,
	progress *atomic.Uint32,
) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(self.procs)

	for cik := range self.lastFiled {
		if ctx.Err() != nil {
			break
		}
		cik := cik
		cnt := progress.Add(1)
		l := self.log(ctx).With(
			slog.String("progress", fmt.Sprintf("%v/%v", cnt, len(self.lastFiled))),
			slog.Uint64("CIK", uint64(cik)))
		g.Go(func() error {
			return self.updateCompanyFacts(ContextWithLogger(ctx, l), cik)
		})
	}
	return g.Wait() //nolint:wrapcheck // returned not from external package
}

func (self *Upload) updateCompanyFacts(ctx context.Context, cik uint32) error {
	replaceFiled, facts, err := self.repoFactsUpdate(ctx, cik)
	if err != nil {
		return err
	} else if len(facts) == 0 {
		return nil
	}

	nextFunc := func(i int) (repo.FactUnit, error) { return facts[i], nil }
	if replaceFiled.IsZero() {
		err = self.repo.CopyFactUnits(ctx, len(facts), nextFunc)
	} else {
		err = self.repo.ReplaceFactUnits(ctx, cik, replaceFiled, len(facts), nextFunc)
	}
	if err != nil {
		return fmt.Errorf("updateCompanyFacts: company CIK=%v: %w", cik, err)
	}
	return nil
}

func (self *Upload) repoFactsUpdate(ctx context.Context, cik uint32,
) (replaceFiled time.Time, facts []repo.FactUnit, err error) {
	lastCnt, facts, err := self.companyFactsUpdate(ctx, cik)
	if err != nil {
		return
	} else if lastCnt == uint32(len(facts)) {
		facts = nil
		return
	}

	lastFiled := self.lastFiled[cik]
	startIdx := slices.IndexFunc(facts,
		func(fact repo.FactUnit) bool { return fact.Filed.After(lastFiled) })
	if uint32(startIdx) == lastCnt {
		self.log(ctx).Info("append new facts",
			slog.Int("length", len(facts)-startIdx),
			slog.Int("was", int(lastCnt)), slog.Int("got", len(facts)),
			slog.Int("start", startIdx))
		facts = facts[startIdx:]
	} else {
		self.log(ctx).Info("replace last filed facts",
			slog.Int("length", len(facts)), slog.Int("was", int(lastCnt)),
			slog.Int("start", startIdx))
		replaceFiled = lastFiled
	}
	return
}

func (self *Upload) companyFactsUpdate(ctx context.Context, cik uint32,
) (lastCnt uint32, facts []repo.FactUnit, err error) {
	done := make(chan error)
	go func() {
		self.log(ctx).Debug("load company filed fact counts")
		if counts, err := self.repo.FiledCounts(ctx, cik); err == nil {
			lastFiled := self.lastFiled[cik]
			lastCnt = counts[lastFiled]
		}
		done <- err
	}()

	self.log(ctx).Debug("fetch company facts")
	companyFacts, err := self.retryCompanyFacts(ctx, retryNum, cik)
	if err != nil {
		err = fmt.Errorf("companyFactsUpdate: company CIK=%v: %w", cik, err)
		<-done
		return
	}

	facts, err = self.freshRepoFacts(ctx, cik, companyFacts.Facts)
	if err != nil {
		err = fmt.Errorf("companyFactsUpdate: company CIK=%v: %w", cik, err)
		<-done
	} else if err = <-done; err != nil {
		err = fmt.Errorf("companyFactsUpdate: company CIK=%v: %w", cik, err)
	}
	return
}

func (self *Upload) freshRepoFacts(ctx context.Context, cik uint32,
	facts map[string]map[string]client.CompanyFact,
) ([]repo.FactUnit, error) {
	lastFiled := self.lastFiled[cik]
	repoFacts := make([]repo.FactUnit, 0)

	err := self.iterateCompanyFacts(ctx, cik, facts,
		func(ctx context.Context, cik, factId, unitId uint32,
			factUnits []client.FactUnit,
		) error {
			for i := range factUnits {
				repoFact, err := self.repoFactUnit(cik, factId, unitId, &factUnits[i])
				if err != nil {
					return err
				}
				if !repoFact.Filed.Before(lastFiled) {
					repoFacts = append(repoFacts, repoFact)
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(repoFacts, func(a, b repo.FactUnit) int {
		if a.Filed.Before(b.Filed) {
			return -1
		} else if a.Filed.After(b.Filed) {
			return 1
		}
		return 0
	})

	return repoFacts, nil
}

func (self *Upload) saveLastUpdated(ctx context.Context, lastUpdated time.Time,
) error {
	self.log(ctx).Info("add last updated", slog.String("at",
		lastUpdated.Format(time.DateOnly)))
	if err := self.repo.AddLastUpdate(ctx, lastUpdated); err != nil {
		return fmt.Errorf("failed add last updated: %w", err)
	}
	return nil
}
