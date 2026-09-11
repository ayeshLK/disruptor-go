package disruptor

import "sync/atomic"

// InitialSequence is the logical position before the first ring entry.
const InitialSequence int64 = -1

// Sequence is a padded, atomically updated logical position. A Sequence must
// not be copied after first use; pass it by pointer.
type Sequence struct {
	_     [64]byte
	value atomic.Int64
	_     [64]byte
}

// NewSequence creates an atomic sequence at initial.
func NewSequence(initial int64) *Sequence {
	s := new(Sequence)
	s.value.Store(initial)
	return s
}

// Load atomically returns the current sequence.
func (s *Sequence) Load() int64 { return s.value.Load() }

// Store atomically replaces the current sequence.
func (s *Sequence) Store(value int64) { s.value.Store(value) }

// Add atomically adds delta and returns the new sequence.
func (s *Sequence) Add(delta int64) int64 { return s.value.Add(delta) }

// CompareAndSwap atomically replaces old with new when the current value equals old.
func (s *Sequence) CompareAndSwap(old, new int64) bool {
	return s.value.CompareAndSwap(old, new)
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
