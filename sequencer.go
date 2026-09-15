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

import (
	"context"
	"runtime"
	"sync"
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

type discardSequencer interface {
	Discard(int64, int64)
	IsDiscarded(int64) bool
}

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
	Shutdown(context.Context) error
}

type sequencerBase struct {
	bufferSize   int64
	cursor       *Sequence
	wait         WaitStrategy
	producerWait *producerWaiter
	gating       atomic.Pointer[sequenceList]
	gatingMu     sync.Mutex
	closed       atomic.Bool
	sealed       atomic.Bool
	closedCh     chan struct{}
	closeOnce    sync.Once
}

func newSequencerBase(size int64, wait WaitStrategy, producerWait ProducerWaitMode) *sequencerBase {
	if wait == nil {
		wait = BlockingWait()
	}
	b := &sequencerBase{
		bufferSize:   size,
		cursor:       NewSequence(InitialSequence),
		wait:         wait,
		producerWait: newProducerWaiter(producerWait),
		closedCh:     make(chan struct{}),
	}
	empty := sequenceList{}
	b.gating.Store(&empty)
	return b
}

func (b *sequencerBase) Cursor() *Sequence { return b.cursor }
func (b *sequencerBase) BufferSize() int64 { return b.bufferSize }

func (b *sequencerBase) gates() []*Sequence { return *b.gating.Load() }

func (b *sequencerBase) AddGatingSequences(sequences ...*Sequence) {
	b.gatingMu.Lock()
	defer b.gatingMu.Unlock()
	if b.closed.Load() {
		return
	}
	current := b.gating.Load()
	next := make(sequenceList, 0, len(*current)+len(sequences))
	next = append(next, (*current)...)
	cursor := b.cursor.Load()
	for _, sequence := range sequences {
		if sequence == nil {
			continue
		}
		sequence.Store(cursor)
		if b.producerWait.needsSignals() {
			sequence.addSignal(b.producerWait)
		}
		next = append(next, sequence)
	}
	b.gating.Store(&next)
}

func (b *sequencerBase) RemoveGatingSequence(target *Sequence) bool {
	b.gatingMu.Lock()
	defer b.gatingMu.Unlock()
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
	b.gating.Store(&next)
	if b.producerWait.needsSignals() {
		stillRegistered := false
		for _, sequence := range next {
			if sequence == target {
				stillRegistered = true
				break
			}
		}
		if !stillRegistered {
			target.removeSignal(b.producerWait)
		}
		b.producerWait.signalAll()
	}
	return true
}

func (b *sequencerBase) minimumGate(fallback int64) int64 {
	return minimumSequence(b.gates(), fallback)
}

func (b *sequencerBase) NewBarrier(dependencies ...*Sequence) *SequenceBarrier {
	var dependent sequenceReader = b.cursor
	if len(dependencies) > 0 {
		dependent = sequenceGroup(dependencies)
	}
	return &SequenceBarrier{sequencer: nil, wait: b.wait, cursor: b.cursor, dependent: dependent, closed: &b.closed, closedCh: b.closedCh}
}

func (b *sequencerBase) Close() {
	b.sealed.Store(true)
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.closedCh)
		b.producerWait.signalAll()
		b.wait.signalAll()
		b.removeGateSignals()
	})
}

func (b *sequencerBase) waitForCapacity(ctx context.Context, wrapPoint, fallback int64) (int64, error) {
	for {
		if b.sealed.Load() {
			return 0, ErrClosed
		}
		prepared := b.producerWait.prepare()
		minimum := b.minimumGate(fallback)
		if wrapPoint <= minimum {
			return minimum, nil
		}
		if err := b.producerWait.wait(ctx, prepared, b.closedCh); err != nil {
			return 0, err
		}
	}
}

func (b *sequencerBase) beginShutdown() error {
	if b.closed.Load() || !b.sealed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	b.producerWait.signalAll()
	return nil
}

func (b *sequencerBase) removeGateSignals() {
	if !b.producerWait.needsSignals() {
		return
	}
	b.gatingMu.Lock()
	defer b.gatingMu.Unlock()
	gates := b.gates()
	seen := make(map[*Sequence]struct{}, len(gates))
	for _, sequence := range gates {
		if _, exists := seen[sequence]; exists {
			continue
		}
		seen[sequence] = struct{}{}
		sequence.removeSignal(b.producerWait)
	}
}

func (b *sequencerBase) awaitShutdown(ctx context.Context, boundary int64) error {
	defer b.Close()
	gates := b.gates()
	for minimumSequence(gates, boundary) < boundary {
		if b.closed.Load() {
			return ErrClosed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			runtime.Gosched()
		}
	}
	return nil
}
