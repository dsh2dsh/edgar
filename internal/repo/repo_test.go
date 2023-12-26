package repo

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/caarlos0/env/v10"
	"github.com/cespare/xxhash/v2"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	mocks "github.com/dsh2dsh/edgar/internal/mocks/repo"
)

const (
	appleCIK  = 320193
	appleName = "Apple Inc."
)

func TestRepoSuite(t *testing.T) {
	cfg := struct {
		ConnURL string `env:"EDGAR_DB_URL,notEmpty"`
	}{}
	//nolint:wrapcheck
	require.NoError(t, dotenv.Load(func() error { return env.Parse(&cfg) }))

	conn, err := pgx.Connect(context.Background(), cfg.ConnURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close(context.Background()))
	})

	suite.Run(t, &RepoTestSuite{db: conn})
}

type RepoTestSuite struct {
	suite.Suite
	db   Postgreser
	repo *Repo
}

func (self *RepoTestSuite) SetupSuite() {
	self.createTestSchema()
}

func (self *RepoTestSuite) createTestSchema() {
	ctx := context.Background()
	_, err := self.db.Exec(ctx, `
CREATE TEMPORARY TABLE companies (
  cik         INTEGER PRIMARY KEY,
  entity_name TEXT    NOT NULL
)`)
	self.Require().NoError(err)

	_, err = self.db.Exec(ctx, `
CREATE TEMPORARY TABLE facts (
  id        SERIAL PRIMARY KEY,
  fact_tax  TEXT   NOT NULL,
  fact_name TEXT   NOT NULL,
  UNIQUE (fact_tax, fact_name)
)`)
	self.Require().NoError(err)

	_, err = self.db.Exec(ctx, `
CREATE TEMPORARY TABLE fact_labels (
  id         SERIAL  PRIMARY KEY,
  fact_id    INTEGER NOT NULL REFERENCES facts(id),
  fact_label TEXT    NOT NULL,
  descr      TEXT    NOT NULL,
  xxhash1    NUMERIC NOT NULL,
  xxhash2    NUMERIC NOT NULL,
  UNIQUE(fact_id, xxhash1, xxhash2)
)`)
	self.Require().NoError(err)
}

func (self *RepoTestSuite) SetupTest() {
	self.repo = New(self.db)
}

func (self *RepoTestSuite) TearDownTest() {
	allTables := []string{"companies", "facts"}
	for _, tname := range allTables {
		sql := fmt.Sprintf("TRUNCATE %s CASCADE", tname)
		_, err := self.db.Exec(context.Background(), sql)
		self.Require().NoError(err)

	}
}

// --------------------------------------------------

func (self *RepoTestSuite) TestRepo_AddCompany() {
	added, err := self.repo.AddCompany(context.Background(), appleCIK, appleName)
	self.Require().NoError(err)
	self.True(added)

	added, err = self.repo.AddCompany(context.Background(), appleCIK, appleName)
	self.Require().NoError(err)
	self.False(added)
}

func TestRepo_AddCompany_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)
	db.EXPECT().Exec(ctx, mock.Anything, mock.Anything, mock.Anything).Return(
		pgconn.CommandTag{}, wantErr)

	added, err := repo.AddCompany(ctx, appleCIK, appleName)
	require.ErrorIs(t, err, wantErr)
	assert.False(t, added)
}

func (self *RepoTestSuite) TestRepo_AddFact() {
	const factTax = "us-gaap"
	const factName = "AccountsPayable"
	ctx := context.Background()

	factId, err := self.repo.AddFact(ctx, factTax, factName)
	self.Require().NoError(err)
	self.NotZero(factId)

	factId, err = self.repo.AddFact(ctx, factTax, factName)
	self.Require().NoError(err)
	self.NotZero(factId)

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
				return rows, err //nolint:wrapcheck
			}).Once()
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

	factId, err = self.repo.AddFact(ctx, factTax, factName)
	self.Require().Error(err)
	self.Zero(factId)

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return self.db.Query(ctx, sql, args...) //nolint:wrapcheck
			}).Once()

	wantErr := errors.New("test error")
	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, wantErr).Once()

	factId, err = self.repo.AddFact(ctx, factTax, factName)
	self.Require().Error(err)
	self.Zero(factId)

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return self.db.Query(ctx, sql, args...) //nolint:wrapcheck
			}).Once()

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
				return rows, err //nolint:wrapcheck
			}).Once()

	factId, err = self.repo.AddFact(ctx, factTax, factName)
	self.Require().Error(err)
	self.Zero(factId)
}

func TestRepo_AddFact_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)
	db.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, wantErr)

	id, err := repo.AddFact(ctx, "us-gaap", "AccountsPayable")
	require.ErrorIs(t, err, wantErr)
	assert.Zero(t, id)
}

func (self *RepoTestSuite) TestRepo_AddLabel() {
	const factTax = "us-gaap"
	const factName = "AccountsPayable"
	const label = "Accounts Payable (Deprecated 2009-01-31)"
	const descr = "Carrying value as of the balance sheet date of liabilities incurred (and for which invoices have typically been received) and payable to vendors for goods and services received that are used in an entity's business. For classified balance sheets, used to reflect the current portion of the liabilities (due within one year or within the normal operating cycle if longer); for unclassified balance sheets, used to reflect the total liabilities (regardless of due date)."

	ctx := context.Background()
	factId, err := self.repo.AddFact(ctx, factTax, factName)
	self.Require().NoError(err)

	labelHash := xxhash.Sum64String(label)
	self.T().Logf("labelHash: %#x", labelHash)
	descrHash := xxhash.Sum64String(descr)
	self.T().Logf("descrHash: %#x", descrHash)

	self.Require().NoError(self.repo.AddLabel(
		ctx, factId, label, descr, labelHash, descrHash))

	rows, err := self.db.Query(ctx,
		`SELECT xxhash1, xxhash2 FROM fact_labels WHERE fact_id = $1`, factId)
	self.Require().NoError(err)

	type hashes struct {
		LabelHash uint64
		DescrHash uint64
	}
	gotHashes, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[hashes])
	self.Require().NoError(err)
	wantHashes := hashes{
		LabelHash: labelHash,
		DescrHash: descrHash,
	}
	self.Equal(wantHashes, gotHashes)

	// ERROR: duplicate key value violates unique constraint
	// "fact_labels_fact_id_xxhash1_xxhash2_key" (SQLSTATE 23505)
	self.Require().NoError(self.repo.AddLabel(
		ctx, factId, label, descr, labelHash, descrHash))

	// ERROR: insert or update on table "fact_labels" violates foreign key
	// constraint "fact_labels_fact_id_fkey" (SQLSTATE 23503)
	self.Require().Error(self.repo.AddLabel(
		ctx, 0, label, descr, labelHash, descrHash))
}
