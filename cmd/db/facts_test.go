package db

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFacts_Fact(t *testing.T) {
	const factKey = "us-gaap:AccountsPayable"

	tests := []struct {
		name       string
		assertCall func(t *testing.T, facts *facts) (*knownFact, error)
		errorIs    error
		wantFact   *knownFact
	}{
		{
			name: "Fact hit",
			assertCall: func(t *testing.T, facts *facts) (*knownFact, error) {
				assert.Equal(t, 0, facts.Len())
				facts.knownFacts[factKey] = newKnownFact(0, 1, 1)
				assert.Equal(t, 1, facts.Len())
				fact, ok := facts.Fact(factKey)
				assert.True(t, ok)
				return fact, nil
			},
			wantFact: newKnownFact(0, 1, 1),
		},
		{
			name: "Fact miss",
			assertCall: func(t *testing.T, facts *facts) (*knownFact, error) {
				assert.Equal(t, 0, facts.Len())
				fact, ok := facts.Fact(factKey)
				assert.False(t, ok)
				return fact, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			facts := newFacts()
			fact, err := tt.assertCall(t, &facts)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantFact, fact)
		})
	}
}

func TestFacts_Create(t *testing.T) {
	const factKey = "us-gaap:AccountsPayable"

	callbackCalled := func(t *testing.T, factId uint32, err error) func() (uint32, error) {
		var called bool
		t.Cleanup(func() { assert.True(t, called, "callback wasn't called") })
		return func() (uint32, error) {
			called = true
			return factId, err
		}
	}

	callbackNotCalled := func(t *testing.T) func() (uint32, error) {
		return func() (uint32, error) {
			assert.Fail(t, "Shouldn't be called")
			return 0, nil
		}
	}

	wantErr := errors.New("test error")

	tests := []struct {
		name       string
		assertCall func(t *testing.T, facts *facts) (*knownFact, error)
		errorIs    error
		wantFact   *knownFact
		wantFacts  map[string]*knownFact
	}{
		{
			name: "hit inside group",
			assertCall: func(t *testing.T, facts *facts) (*knownFact, error) {
				facts.knownFacts[factKey] = newKnownFact(1, 2, 2)
				fact, err := facts.Create(factKey, 2, 2, callbackNotCalled(t))
				return fact, err
			},
			wantFact:  newKnownFact(1, 2, 2),
			wantFacts: map[string]*knownFact{factKey: newKnownFact(1, 2, 2)},
		},
		{
			name: "with callback",
			assertCall: func(t *testing.T, facts *facts) (*knownFact, error) {
				fact, err := facts.Create(factKey, 2, 2, callbackCalled(t, 1, nil))
				return fact, err
			},
			wantFact:  newKnownFact(1, 2, 2),
			wantFacts: map[string]*knownFact{factKey: newKnownFact(1, 2, 2)},
		},
		{
			name: "with callback error",
			assertCall: func(t *testing.T, facts *facts) (*knownFact, error) {
				fact, err := facts.Create(factKey, 2, 2, callbackCalled(t, 0, wantErr))
				return fact, err
			},
			errorIs:   wantErr,
			wantFacts: map[string]*knownFact{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			facts := newFacts()
			fact, err := tt.assertCall(t, &facts)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
				require.Nil(t, fact)
			} else {
				require.NoError(t, err)
				require.NotNil(t, fact)
			}
			assert.Equal(t, tt.wantFact, fact)
			assert.Equal(t, tt.wantFacts, facts.knownFacts)
		})
	}
}

func TestFacts_Preload(t *testing.T) {
	const factKey = "us-gaap:AccountsPayable"

	tests := []struct {
		name        string
		assertCall  func(t *testing.T, facts *facts) bool
		unknownFact bool
		wantFacts   func() map[string]*knownFact
	}{
		{
			name: "new fact",
			assertCall: func(t *testing.T, facts *facts) bool {
				return facts.Preload(1, factKey, 2, 2)
			},
			unknownFact: true,
			wantFacts: func() map[string]*knownFact {
				return map[string]*knownFact{factKey: newKnownFact(1, 2, 2)}
			},
		},
		{
			name: "more labels",
			assertCall: func(t *testing.T, facts *facts) bool {
				facts.knownFacts[factKey] = newKnownFact(1, 2, 2)
				return facts.Preload(1, factKey, 2, 3)
			},
			wantFacts: func() map[string]*knownFact {
				fact := newKnownFact(1, 2, 2)
				fact.AddMoreLabel(2, 3)
				return map[string]*knownFact{factKey: fact}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			facts := newFacts()
			unknownFact := tt.assertCall(t, &facts)
			assert.Equal(t, tt.unknownFact, unknownFact)
			assert.Equal(t, tt.wantFacts(), facts.knownFacts)
		})
	}
}

func TestKnownFact_AddLabel(t *testing.T) {
	callbackCalled := func(t *testing.T, err error) func() error {
		var called bool
		t.Cleanup(func() { assert.True(t, called, "callback wasn't called") })
		return func() error {
			called = true
			return err
		}
	}

	callbackNotCalled := func(t *testing.T) func() error {
		return func() error {
			assert.Fail(t, "Shouldn't be called")
			return nil
		}
	}

	wantErr := errors.New("test error")
	tests := []struct {
		name       string
		assertCall func(t *testing.T, fact *knownFact) error
		errorIs    error
		wantFact   *knownFact
	}{
		{
			name: "direct hit",
			assertCall: func(t *testing.T, fact *knownFact) error {
				return fact.AddLabel(1, 1, callbackNotCalled(t))
			},
			wantFact: &knownFact{LabelHash: 1, DescrHash: 1},
		},
		{
			name: "moreLabels",
			assertCall: func(t *testing.T, fact *knownFact) error {
				fact.moreLabels = map[uint64]map[uint64]struct{}{2: {2: {}}}
				return fact.AddLabel(2, 2, callbackNotCalled(t))
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}}},
			},
		},
		{
			name: "with callback",
			assertCall: func(t *testing.T, fact *knownFact) error {
				return fact.AddLabel(2, 2, callbackCalled(t, nil))
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}}},
			},
		},
		{
			name: "with callback error",
			assertCall: func(t *testing.T, fact *knownFact) error {
				return fact.AddLabel(2, 2, callbackCalled(t, wantErr))
			},
			errorIs:  wantErr,
			wantFact: &knownFact{LabelHash: 1, DescrHash: 1},
		},
		{
			name: "added into moreLabels 1",
			assertCall: func(t *testing.T, fact *knownFact) error {
				fact.moreLabels = map[uint64]map[uint64]struct{}{}
				return fact.AddLabel(2, 2, callbackCalled(t, nil))
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}}},
			},
		},
		{
			name: "added into moreLabels 2",
			assertCall: func(t *testing.T, fact *knownFact) error {
				fact.moreLabels = map[uint64]map[uint64]struct{}{2: {2: {}}}
				return fact.AddLabel(2, 3, callbackCalled(t, nil))
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}, 3: {}}},
			},
		},
		{
			name: "with mutex",
			assertCall: func(t *testing.T, fact *knownFact) error {
				sig := make(chan struct{})
				done := make(chan struct{})
				err := fact.AddLabel(2, 2, func() error {
					go func() {
						close(sig)
						err := fact.AddLabel(2, 3, callbackCalled(t, nil))
						require.NoError(t, err)
						close(done)
					}()
					<-sig
					return nil
				})
				<-done
				return err
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}, 3: {}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fact := newKnownFact(0, 1, 1)
			err := tt.assertCall(t, fact)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantFact, fact)
		})
	}
}

func TestKnownFact_AddMoreLabel(t *testing.T) {
	tests := []struct {
		name       string
		assertCall func(t *testing.T, fact *knownFact)
		errorIs    error
		wantFact   *knownFact
	}{
		{
			name: "moreLabels",
			assertCall: func(t *testing.T, fact *knownFact) {
				fact.moreLabels = map[uint64]map[uint64]struct{}{2: {2: {}}}
				fact.AddMoreLabel(2, 2)
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}}},
			},
		},
		{
			name: "added into moreLabels",
			assertCall: func(t *testing.T, fact *knownFact) {
				fact.AddMoreLabel(2, 2)
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}}},
			},
		},
		{
			name: "added into moreLabels 1",
			assertCall: func(t *testing.T, fact *knownFact) {
				fact.moreLabels = map[uint64]map[uint64]struct{}{}
				fact.AddMoreLabel(2, 2)
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}}},
			},
		},
		{
			name: "added into moreLabels 2",
			assertCall: func(t *testing.T, fact *knownFact) {
				fact.moreLabels = map[uint64]map[uint64]struct{}{2: {2: {}}}
				fact.AddMoreLabel(2, 3)
			},
			wantFact: &knownFact{
				LabelHash:  1,
				DescrHash:  1,
				moreLabels: map[uint64]map[uint64]struct{}{2: {2: {}, 3: {}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fact := newKnownFact(0, 1, 1)
			tt.assertCall(t, fact)
			assert.Equal(t, tt.wantFact, fact)
		})
	}
}

func TestFactUnits_Id(t *testing.T) {
	callbackCalled := func(t *testing.T, id uint32, err error) func() (uint32, error) {
		var called bool
		t.Cleanup(func() { assert.True(t, called, "callback wasn't called") })
		return func() (uint32, error) {
			called = true
			return id, err
		}
	}

	callbackNotCalled := func(t *testing.T) func() (uint32, error) {
		return func() (uint32, error) {
			assert.Fail(t, "Shouldn't be called")
			return 0, nil
		}
	}

	ctx := context.Background()
	wantErr := errors.New("test error")

	tests := []struct {
		name       string
		assertCall func(t *testing.T, facts *factUnits) (uint32, error)
		errorIs    error
		wantUnitId uint32
		wantUnits  map[string]uint32
	}{
		{
			name: "direct hit",
			assertCall: func(t *testing.T, facts *factUnits) (uint32, error) {
				facts.units["USD"] = 1
				return facts.Id(ctx, "USD", callbackNotCalled(t))
			},
			wantUnitId: 1,
			wantUnits:  map[string]uint32{"USD": 1},
		},
		{
			name: "with callback",
			assertCall: func(t *testing.T, facts *factUnits) (uint32, error) {
				return facts.Id(ctx, "USD", callbackCalled(t, 1, nil))
			},
			wantUnitId: 1,
			wantUnits:  map[string]uint32{"USD": 1},
		},
		{
			name: "with callback error",
			assertCall: func(t *testing.T, facts *factUnits) (uint32, error) {
				return facts.Id(ctx, "USD", callbackCalled(t, 0, wantErr))
			},
			errorIs:    wantErr,
			wantUnitId: 0,
			wantUnits:  map[string]uint32{},
		},
		{
			name: "hit inside group",
			assertCall: func(t *testing.T, facts *factUnits) (uint32, error) {
				facts.units["USD"] = 1
				return facts.createUnit(ctx, "USD", callbackNotCalled(t))
			},
			wantUnitId: 1,
			wantUnits:  map[string]uint32{"USD": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			facts := newFactUnits()
			unitId, err := tt.assertCall(t, &facts)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantUnitId, unitId)
			assert.Equal(t, tt.wantUnits, facts.units)
		})
	}
}
