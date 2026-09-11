package disruptor

import (
	"context"
	"runtime"
	"sync/atomic"
)

// ProducerType selects the claim and publication protocol.
type ProducerType uint8

const (
	// SingleProducer selects the non-atomic claim path for exactly one producer goroutine.
	SingleProducer ProducerType = iota
	// MultiProducer selects the CAS claim path for concurrent producer goroutines.
	MultiProducer
)

type sequenceList []*Sequence

// Sequencer coordinates producer claims, publication, capacity, and consumer gates.
type Sequencer interface {
	Next(context.Context, int64) (int64, error)
	TryNext(int64) (int64, error)
	Publish(int64, int64)
	IsAvailable(int64) bool
	HighestPublished(int64, int64) int64
	Cursor() *Sequence
	BufferSize() int64
	RemainingCapacity() int64
	AddGatingSequences(...*Sequence)
	RemoveGatingSequence(*Sequence) bool
	NewBarrier(...*Sequence) *SequenceBarrier
	Close()
}

type sequencerBase struct {
	bufferSize int64
	cursor     *Sequence
	wait       WaitStrategy
	gating     atomic.Pointer[sequenceList]
	closed     atomic.Bool
}

func newSequencerBase(size int64, wait WaitStrategy) *sequencerBase {
	if wait == nil {
		wait = BlockingWait()
	}
	b := &sequencerBase{bufferSize: size, cursor: NewSequence(InitialSequence), wait: wait}
	empty := sequenceList{}
	b.gating.Store(&empty)
	return b
}

func (b *sequencerBase) Cursor() *Sequence { return b.cursor }
func (b *sequencerBase) BufferSize() int64 { return b.bufferSize }

func (b *sequencerBase) gates() []*Sequence { return *b.gating.Load() }

func (b *sequencerBase) AddGatingSequences(sequences ...*Sequence) {
	for {
		current := b.gating.Load()
		next := make(sequenceList, 0, len(*current)+len(sequences))
		next = append(next, (*current)...)
		cursor := b.cursor.Load()
		for _, sequence := range sequences {
			if sequence == nil {
				continue
			}
			sequence.Store(cursor)
			next = append(next, sequence)
		}
		if b.gating.CompareAndSwap(current, &next) {
			return
		}
	}
}

func (b *sequencerBase) RemoveGatingSequence(target *Sequence) bool {
	for {
		current := b.gating.Load()
		found := -1
		for i, sequence := range *current {
			if sequence == target {
				found = i
				break
			}
		}
		if found < 0 {
			return false
		}
		next := make(sequenceList, 0, len(*current)-1)
		next = append(next, (*current)[:found]...)
		next = append(next, (*current)[found+1:]...)
		if b.gating.CompareAndSwap(current, &next) {
			return true
		}
	}
}

func (b *sequencerBase) minimumGate(fallback int64) int64 {
	return minimumSequence(b.gates(), fallback)
}

func (b *sequencerBase) NewBarrier(dependencies ...*Sequence) *SequenceBarrier {
	var dependent sequenceReader = b.cursor
	if len(dependencies) > 0 {
		dependent = sequenceGroup(dependencies)
	}
	return &SequenceBarrier{sequencer: nil, wait: b.wait, cursor: b.cursor, dependent: dependent}
}

func (b *sequencerBase) Close() {
	b.closed.Store(true)
	b.wait.signalAll()
}

func (b *sequencerBase) waitForCapacity(ctx context.Context, wrapPoint, fallback int64) (int64, error) {
	for {
		if b.closed.Load() {
			return 0, ErrClosed
		}
		minimum := b.minimumGate(fallback)
		if wrapPoint <= minimum {
			return minimum, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			runtime.Gosched()
		}
	}
}
