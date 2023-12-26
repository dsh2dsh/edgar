package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	ctx := context.Background()
	wantErr := errors.New("test error")

	tests := []struct {
		name        string
		prepareFact func(fact *knownFact)
		assertCall  func(t *testing.T, fact *knownFact) error
		errorIs     error
		wantFact    *knownFact
	}{
		{
			name: "direct hit",
			assertCall: func(t *testing.T, fact *knownFact) error {
				return fact.AddLabel(ctx, 1, 1, callbackNotCalled(t))
			},
			wantFact: &knownFact{LabelHash: 1, DescrHash: 1},
		},
		{
			name: "moreLabels",
			assertCall: func(t *testing.T, fact *knownFact) error {
				fact.moreLabels = map[uint64]map[uint64]struct{}{2: {2: {}}}
				return fact.AddLabel(ctx, 2, 2, callbackNotCalled(t))
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
				return fact.AddLabel(ctx, 2, 2, callbackCalled(t, nil))
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
				return fact.AddLabel(ctx, 2, 2, callbackCalled(t, wantErr))
			},
			errorIs:  wantErr,
			wantFact: &knownFact{LabelHash: 1, DescrHash: 1},
		},
		{
			name: "added into moreLabels 1",
			assertCall: func(t *testing.T, fact *knownFact) error {
				fact.moreLabels = map[uint64]map[uint64]struct{}{}
				return fact.AddLabel(ctx, 2, 2, callbackCalled(t, nil))
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
				return fact.AddLabel(ctx, 2, 3, callbackCalled(t, nil))
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
				err := fact.AddLabel(ctx, 2, 2, func() error {
					go func() {
						close(sig)
						err := fact.AddLabel(ctx, 2, 3, callbackCalled(t, nil))
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
			fact := &knownFact{LabelHash: 1, DescrHash: 1}
			if tt.prepareFact != nil {
				tt.prepareFact(fact)
			}
			err := tt.assertCall(t, fact)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantFact, fact)
		})
	}
}
