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

	factLabel = "Accounts Payable (Deprecated 2009-01-31)"
	factDescr = "Carrying value as of the balance sheet date of liabilities incurred (and for which invoices have typically been received) and payable to vendors for goods and services received that are used in an entity's business. For classified balance sheets, used to reflect the current portion of the liabilities (due within one year or within the normal operating cycle if longer); for unclassified balance sheets, used to reflect the total liabilities (regardless of due date)."

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
  frame       TEXT
);

CREATE INDEX ON fact_units (company_cik, filed);`)
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
	ctx := context.Background()
	factId := self.addTestFact(ctx)
	labelHash, descrHash := self.addTestLabel(factId)
	self.T().Logf("labelHash: %#x", labelHash)
	self.T().Logf("descrHash: %#x", descrHash)

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
		ctx, factId, factLabel, factDescr, labelHash, descrHash))

	// ERROR: insert or update on table "fact_labels" violates foreign key
	// constraint "fact_labels_fact_id_fkey" (SQLSTATE 23503)
	self.Require().Error(self.repo.AddLabel(
		ctx, 0, factLabel, factDescr, labelHash, descrHash))
}

func (self *RepoTestSuite) addTestLabel(factId uint32) (uint64, uint64) {
	labelHash := xxhash.Sum64String(factLabel)
	descrHash := xxhash.Sum64String(factDescr)
	self.Require().NoError(self.repo.AddLabel(
		context.Background(), factId, factLabel, factDescr, labelHash, descrHash))
	return labelHash, descrHash
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
		name    string
		fact    FactUnit
		prepare func(t *testing.T, fact *FactUnit)
		wantErr bool
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
			name: "with all Id",
			fact: FactUnit{CIK: appleCIK, FactId: factId, UnitId: unitId},
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
			self.T().Cleanup(func() {
				_, err := self.db.Exec(context.Background(), "TRUNCATE fact_units")
				self.Require().NoError(err)
			})
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

func (self *RepoTestSuite) TestRepo_CopyFactUnits() {
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

	facts := []FactUnit{fullFact, fullFact, fullFact}
	err := self.repo.CopyFactUnits(ctx, len(facts), func(i int) (FactUnit, error) {
		return facts[i], nil
	})
	self.Require().NoError(err)

	rows, err := self.db.Query(ctx, `SELECT * FROM fact_units`)
	self.Require().NoError(err)
	gotFacts, err := pgx.CollectRows(rows, pgx.RowToStructByName[FactUnit])
	self.Require().NoError(err)
	self.Equal(facts, gotFacts)

	wantErr := errors.New("test error")
	err = self.repo.CopyFactUnits(ctx, len(facts), func(i int) (FactUnit, error) {
		return facts[i], wantErr
	})
	self.Require().Error(err)
}

func TestRepo_CopyFactUnits_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)

	db.EXPECT().CopyFrom(ctx, pgx.Identifier{"fact_units"}, mock.Anything,
		mock.Anything).Return(0, wantErr)

	facts := []FactUnit{{}, {}, {}}
	err := repo.CopyFactUnits(ctx, len(facts), func(i int) (FactUnit, error) {
		return facts[i], nil
	})
	require.ErrorIs(t, err, wantErr)
}

func TestRepo_CopyFactUnits_wrongN(t *testing.T) {
	db := mocks.NewMockPostgreser(t)
	repo := New(db)

	ctx := context.Background()
	db.EXPECT().CopyFrom(ctx, pgx.Identifier{"fact_units"}, mock.Anything,
		mock.Anything).Return(0, nil)

	facts := []FactUnit{{}, {}, {}}
	err := repo.CopyFactUnits(ctx, len(facts), func(i int) (FactUnit, error) {
		return facts[i], nil
	})
	require.Error(t, err)
}

func (self *RepoTestSuite) TestRepo_LastFiled() {
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

	filed := []time.Time{
		time.Date(2009, 7, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2009, 7, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2009, 7, 18, 0, 0, 0, 0, time.UTC),
		time.Date(2009, 7, 22, 0, 0, 0, 0, time.UTC),
		time.Date(2009, 7, 4, 0, 0, 0, 0, time.UTC),
	}
	facts := make([]FactUnit, len(filed))
	for i := range filed {
		facts[i] = fullFact
		facts[i].Filed = filed[i]
	}

	err := self.repo.CopyFactUnits(ctx, len(facts), func(i int) (FactUnit, error) {
		return facts[i], nil
	})
	self.Require().NoError(err)

	lastFiled, err := self.repo.LastFiled(ctx)
	self.Require().NoError(err)
	self.Len(lastFiled, 1)
	self.Equal(time.Date(2009, 7, 22, 0, 0, 0, 0, time.UTC), lastFiled[appleCIK])

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
			return rows, err
		})
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

	_, err = self.repo.LastFiled(ctx)
	self.Require().Error(err)
}

func TestRepo_LastFiled_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)

	db.EXPECT().Query(ctx, mock.Anything).Return(nil, wantErr).Once()

	lastFiled, err := repo.LastFiled(ctx)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, lastFiled)
}

func (self *RepoTestSuite) TestRepo_FactLabels() {
	ctx := context.Background()
	self.addTestCompany(ctx)
	factId := self.addTestFact(ctx)
	labelHash, descrHash := self.addTestLabel(factId)

	factLabels, err := self.repo.FactLabels(ctx)
	self.Require().NoError(err)
	self.Len(factLabels, 1)

	rows, err := self.db.Query(ctx,
		`SELECT id FROM fact_labels WHERE fact_id = $1`, factId)
	self.Require().NoError(err)
	labelId, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[uint32])
	self.Require().NoError(err)

	testFact := FactLabels{
		FactId:    factId,
		FactTax:   factTax,
		FactName:  factName,
		LabelId:   labelId,
		LabelHash: labelHash,
		DescrHash: descrHash,
	}
	self.Equal([]FactLabels{testFact}, factLabels)

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
			return rows, err
		})
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

	_, err = self.repo.FactLabels(ctx)
	self.Require().Error(err)
}

func TestRepo_FactLabels_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)

	db.EXPECT().Query(ctx, mock.Anything).Return(nil, wantErr).Once()

	factLabels, err := repo.FactLabels(ctx)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, factLabels)
}

func (self *RepoTestSuite) TestRepo_Units() {
	ctx := context.Background()
	unitId := self.addTestUnit(ctx)

	units, err := self.repo.Units(ctx)
	self.Require().NoError(err)
	self.Len(units, 1)
	self.Equal(map[uint32]string{unitId: unitName}, units)

	m := mocks.NewMockPostgreser(self.T())
	m.EXPECT().Query(ctx, mock.Anything).RunAndReturn(
		func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
			rows, err := self.db.Query(ctx, "SELECT 'not SERIAL'")
			return rows, err
		})
	self.repo.db = m
	self.T().Cleanup(func() { self.repo.db = self.db })

	units, err = self.repo.Units(ctx)
	self.Require().Error(err)
	self.Nil(units)
}

func TestRepo_Units_error(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	db := mocks.NewMockPostgreser(t)
	repo := New(db)

	db.EXPECT().Query(ctx, mock.Anything).Return(nil, wantErr).Once()

	units, err := repo.Units(ctx)
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, units)
}
