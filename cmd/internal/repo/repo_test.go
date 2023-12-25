package repo

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/caarlos0/env/v10"
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
	schemaName = "repo_test"
	appleCIK   = 320193
	appleName  = "Apple Inc."
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
	_, err := self.db.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName)
	self.Require().NoError(err)

	_, err = self.db.Exec(context.Background(), "CREATE SCHEMA "+schemaName)
	self.Require().NoError(err)

	_, err = self.db.Exec(context.Background(), `
CREATE TEMPORARY TABLE companies (
  cik         INTEGER PRIMARY KEY,
  entity_name TEXT    NOT NULL
)`)
	self.Require().NoError(err)

	_, err = self.db.Exec(context.Background(), `
CREATE TEMPORARY TABLE facts (
  id        SERIAL PRIMARY KEY,
  fact_tax  TEXT   NOT NULL,
  fact_name TEXT   NOT NULL,
  UNIQUE (fact_tax, fact_name)
);
`)
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

func (self *RepoTestSuite) TearDownSuite() {
	_, err := self.db.Exec(context.Background(), "DROP SCHEMA "+schemaName)
	self.Require().NoError(err)
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
	self.Zero(factId)

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
				return rows, err //nolint:wrapcheck
			})
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

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
