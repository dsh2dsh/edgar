package db

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/singleflight"
)

func newFacts() facts {
	return facts{knownFacts: make(map[string]*knownFact, 0)}
}

type facts struct {
	knownFacts map[string]*knownFact
	group      singleflight.Group
	mu         sync.RWMutex
}

func (self *facts) Len() int {
	self.mu.RLock()
	defer self.mu.RUnlock()
	return len(self.knownFacts)
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

func (self *facts) Preload(factId uint32, key string,
	labelHash, descrHash uint64,
) bool {
	if fact, ok := self.knownFacts[key]; ok {
		fact.AddMoreLabel(labelHash, descrHash)
		return false
	}
	self.knownFacts[key] = newKnownFact(factId, labelHash, descrHash)
	return true
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

func (self *knownFact) AddLabel(labelHash, descrHash uint64,
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

	self.AddMoreLabel(labelHash, descrHash)
	return nil
}

func (self *knownFact) AddMoreLabel(labelHash, descrHash uint64) {
	if self.moreLabels == nil {
		self.moreLabels = map[uint64]map[uint64]struct{}{
			labelHash: {descrHash: {}},
		}
	} else if _, ok := self.moreLabels[labelHash]; !ok {
		self.moreLabels[labelHash] = map[uint64]struct{}{descrHash: {}}
	} else {
		self.moreLabels[labelHash][descrHash] = struct{}{}
	}
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
		self.Preload(id, name)
		return id, nil
	})

	if err != nil {
		return 0, err //nolint:wrapcheck // wrapped inside genUnitId
	}
	return v.(uint32), nil
}

func (self *factUnits) Preload(id uint32, name string) {
	self.units[name] = id
}
