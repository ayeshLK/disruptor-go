// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disruptor

import "sync/atomic"

// InitialSequence is the logical position before the first ring entry.
const InitialSequence int64 = -1

// Sequence is a padded, atomically updated logical position. A Sequence must
// not be copied after first use; pass it by pointer.
type Sequence struct {
	_       [64]byte
	value   atomic.Int64
	_       [64]byte
	signals atomic.Pointer[sequenceSignals]
}

type sequenceSignals []*producerWaiter

// NewSequence creates an atomic sequence at initial.
func NewSequence(initial int64) *Sequence {
	s := new(Sequence)
	s.value.Store(initial)
	return s
}

// Load atomically returns the current sequence.
func (s *Sequence) Load() int64 { return s.value.Load() }

// Store atomically replaces the current sequence.
func (s *Sequence) Store(value int64) {
	s.store(value)
	s.signalAll()
}

// Add atomically adds delta and returns the new sequence.
func (s *Sequence) Add(delta int64) int64 {
	value := s.value.Add(delta)
	s.signalAll()
	return value
}

// CompareAndSwap atomically replaces old with new when the current value equals old.
func (s *Sequence) CompareAndSwap(old, new int64) bool {
	if !s.compareAndSwap(old, new) {
		return false
	}
	s.signalAll()
	return true
}

func (s *Sequence) store(value int64) { s.value.Store(value) }

func (s *Sequence) compareAndSwap(old, new int64) bool {
	return s.value.CompareAndSwap(old, new)
}

func (s *Sequence) addSignal(signal *producerWaiter) {
	for {
		current := s.signals.Load()
		if current != nil {
			for _, registered := range *current {
				if registered == signal {
					return
				}
			}
		}
		next := make(sequenceSignals, 0, signalCount(current)+1)
		if current != nil {
			next = append(next, (*current)...)
		}
		next = append(next, signal)
		if s.signals.CompareAndSwap(current, &next) {
			return
		}
	}
}

func (s *Sequence) removeSignal(signal *producerWaiter) {
	for {
		current := s.signals.Load()
		if current == nil {
			return
		}
		index := -1
		for i, registered := range *current {
			if registered == signal {
				index = i
				break
			}
		}
		if index < 0 {
			return
		}
		next := make(sequenceSignals, 0, len(*current)-1)
		next = append(next, (*current)[:index]...)
		next = append(next, (*current)[index+1:]...)
		if s.signals.CompareAndSwap(current, &next) {
			return
		}
	}
}

func (s *Sequence) signalAll() {
	if signals := s.signals.Load(); signals != nil {
		for _, signal := range *signals {
			signal.signalAll()
		}
	}
}

func signalCount(signals *sequenceSignals) int {
	if signals == nil {
		return 0
	}
	return len(*signals)
}

type sequenceReader interface {
	Load() int64
}

type sequenceGroup []*Sequence

func (g sequenceGroup) Load() int64 {
	if len(g) == 0 {
		return InitialSequence
	}
	minimum := g[0].Load()
	for _, sequence := range g[1:] {
		if value := sequence.Load(); value < minimum {
			minimum = value
		}
	}
	return minimum
}

func minimumSequence(sequences []*Sequence, fallback int64) int64 {
	if len(sequences) == 0 {
		return fallback
	}
	minimum := sequences[0].Load()
	for _, sequence := range sequences[1:] {
		if value := sequence.Load(); value < minimum {
			minimum = value
		}
	}
	return minimum
}
