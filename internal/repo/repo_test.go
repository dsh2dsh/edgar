package repo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/caarlos0/env/v10"
	"github.com/cespare/xxhash/v2"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	mocks "github.com/dsh2dsh/edgar/internal/mocks/repo"
)

const (
	appleCIK  = 320193
	appleName = "Apple Inc."

	factTax  = "us-gaap"
	factName = "AccountsPayable"

	unitName = "USD"
)

func TestRepoSuite(t *testing.T) {
	cfg := struct {
		ConnURL string `env:"EDGAR_DB_URL,notEmpty"`
	}{}
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

	_, err = self.db.Exec(ctx, `
CREATE TEMPORARY TABLE units (
  id        SERIAL PRIMARY KEY,
  unit_name TEXT   NOT NULL UNIQUE
)`)
	self.Require().NoError(err)

	_, err = self.db.Exec(ctx, `
CREATE TEMPORARY TABLE fact_units (
  company_cik INTEGER NOT NULL REFERENCES companies(cik),
  fact_id     INTEGER NOT NULL REFERENCES facts(id),
  unit_id     INTEGER NOT NULL REFERENCES units(id),
  fact_start  DATE,
  fact_end    DATE    NOT NULL,
  val         NUMERIC NOT NULL,
  accn        TEXT    NOT NULL,
  fy          INTEGER NOT NULL,
  fp          TEXT    NOT NULL,
  form        TEXT    NOT NULL,
  filed       DATE    NOT NULL,
  frame       TEXT,
  PRIMARY KEY (company_cik, fact_id, unit_id)
)`)
	self.Require().NoError(err)
}

func (self *RepoTestSuite) SetupTest() {
	self.repo = New(self.db)
}

func (self *RepoTestSuite) TearDownTest() {
	allTables := []string{"companies", "facts", "fact_labels", "units", "fact_units"}
	for _, tname := range allTables {
		sql := fmt.Sprintf("TRUNCATE %s CASCADE", tname)
		_, err := self.db.Exec(context.Background(), sql)
		self.Require().NoError(err)
	}
}

// --------------------------------------------------

func (self *RepoTestSuite) TestRepo_AddCompany() {
	self.addTestCompany(context.Background())
	added, err := self.repo.AddCompany(context.Background(), appleCIK, appleName)
	self.Require().NoError(err)
	self.False(added)
}

func (self *RepoTestSuite) addTestCompany(ctx context.Context) {
	added, err := self.repo.AddCompany(ctx, appleCIK, appleName)
	self.Require().NoError(err)
	self.True(added)
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
	ctx := context.Background()
	self.addTestFact(ctx)

	factId, err := self.repo.AddFact(ctx, factTax, factName)
	self.Require().NoError(err)
	self.NotZero(factId)

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
				return rows, err
			}).Once()
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

	factId, err = self.repo.AddFact(ctx, factTax, factName)
	self.Require().Error(err)
	self.Zero(factId)

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				return self.db.Query(ctx, sql, args...)
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
				return self.db.Query(ctx, sql, args...)
			}).Once()

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(
			func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
				rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
				return rows, err
			}).Once()

	factId, err = self.repo.AddFact(ctx, factTax, factName)
	self.Require().Error(err)
	self.Zero(factId)
}

func (self *RepoTestSuite) addTestFact(ctx context.Context) uint32 {
	factId, err := self.repo.AddFact(ctx, factTax, factName)
	self.Require().NoError(err)
	self.NotZero(factId)
	return factId
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
	const label = "Accounts Payable (Deprecated 2009-01-31)"
	const descr = "Carrying value as of the balance sheet date of liabilities incurred (and for which invoices have typically been received) and payable to vendors for goods and services received that are used in an entity's business. For classified balance sheets, used to reflect the current portion of the liabilities (due within one year or within the normal operating cycle if longer); for unclassified balance sheets, used to reflect the total liabilities (regardless of due date)."

	ctx := context.Background()
	factId := self.addTestFact(ctx)

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

func (self *RepoTestSuite) TestRepo_AddUnit() {
	ctx := context.Background()
	self.addTestUnit(ctx)

	unitId, err := self.repo.AddUnit(ctx, unitName)
	self.Require().NoError(err)
	self.NotZero(unitId)

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
			return rows, err
		}).Once()
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

	unitId, err = self.repo.AddUnit(ctx, unitName)
	self.Require().Error(err)
	self.Zero(unitId)

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			return self.db.Query(ctx, sql, args...)
		}).Once()

	wantErr := errors.New("test error")
	m.EXPECT().Query(ctx, mock.Anything, mock.Anything).Return(nil, wantErr).Once()

	unitId, err = self.repo.AddUnit(ctx, unitName)
	self.Require().Error(err)
	self.Zero(unitId)

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			return self.db.Query(ctx, sql, args...)
		}).Once()

	m.EXPECT().Query(ctx, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
			return rows, err
		}).Once()

	unitId, err = self.repo.AddUnit(ctx, unitName)
	self.Require().Error(err)
	self.Zero(unitId)
}

func (self *RepoTestSuite) addTestUnit(ctx context.Context) uint32 {
	unitId, err := self.repo.AddUnit(ctx, unitName)
	self.Require().NoError(err)
	self.NotZero(unitId)
	return unitId
}

func TestRepo_AddUnit_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)
	db.EXPECT().Query(ctx, mock.Anything, mock.Anything).Return(nil, wantErr)

	id, err := repo.AddUnit(ctx, "USD")
	require.ErrorIs(t, err, wantErr)
	assert.Zero(t, id)
}

func (self *RepoTestSuite) TestRepo_AddFactUnit() {
	ctx := context.Background()
	self.addTestCompany(ctx)
	factId := self.addTestFact(ctx)
	unitId := self.addTestUnit(ctx)

	fullFact := FactUnit{
		CIK:    appleCIK,
		FactId: factId,
		UnitId: unitId,
		End:    time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC),
		Val:    5520000000,
		Accn:   "0001193125-09-153165",
		FY:     2009,
		FP:     "Q3",
		Form:   "10-Q",
		Filed:  time.Date(2009, 7, 22, 0, 0, 0, 0, time.UTC),
	}
	fullFact.WithStart(time.Date(2008, 9, 27, 0, 0, 0, 0, time.UTC)).
		WithFrame("CY2008Q3I")

	tests := []struct {
		name      string
		fact      FactUnit
		prepare   func(t *testing.T, fact *FactUnit)
		keepTable bool
		wantErr   bool
	}{
		{
			name:    "empty fact error",
			wantErr: true,
		},
		{
			name:    "requires CIK",
			fact:    FactUnit{FactId: factId, UnitId: unitId},
			wantErr: true,
		},
		{
			name:    "requires FactId and UnitId",
			fact:    FactUnit{CIK: appleCIK},
			wantErr: true,
		},
		{
			name:    "requires FactId",
			fact:    FactUnit{CIK: appleCIK, UnitId: unitId},
			wantErr: true,
		},
		{
			name:    "requires UnitId",
			fact:    FactUnit{CIK: appleCIK, FactId: factId},
			wantErr: true,
		},
		{
			name:      "with all Id",
			fact:      FactUnit{CIK: appleCIK, FactId: factId, UnitId: unitId},
			keepTable: true,
		},
		{
			name:    "duplicate key",
			fact:    FactUnit{CIK: appleCIK, FactId: factId, UnitId: unitId},
			wantErr: true,
		},
		{
			name: "with all fields",
			fact: fullFact,
		},
		{
			name: "without Start",
			fact: fullFact,
			prepare: func(t *testing.T, fact *FactUnit) {
				fact.Start = pgtype.Date{}
			},
		},
		{
			name: "without Frame",
			fact: fullFact,
			prepare: func(t *testing.T, fact *FactUnit) {
				fact.Frame = pgtype.Text{}
			},
		},
	}

	for _, tt := range tests {
		self.Run(tt.name, func() {
			fact := tt.fact
			if !tt.keepTable {
				self.T().Cleanup(func() {
					_, err := self.db.Exec(context.Background(), "TRUNCATE fact_units")
					self.Require().NoError(err)
				})
			}
			if tt.prepare != nil {
				tt.prepare(self.T(), &fact)
			}
			err := self.repo.AddFactUnit(ctx, fact)
			if tt.wantErr {
				self.Require().Error(err)
			} else {
				self.Require().NoError(err)
				rows, err := self.db.Query(ctx, `SELECT * FROM fact_units`)
				self.Require().NoError(err)
				gotFact, err := pgx.CollectExactlyOneRow(rows,
					pgx.RowToStructByName[FactUnit])
				self.Require().NoError(err)
				self.Equal(fact, gotFact)
			}
		})
	}
}
