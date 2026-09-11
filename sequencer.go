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
	bufferSize int64
	cursor     *Sequence
	wait       WaitStrategy
	gating     atomic.Pointer[sequenceList]
	closed     atomic.Bool
	sealed     atomic.Bool
	closedCh   chan struct{}
	closeOnce  sync.Once
}

func newSequencerBase(size int64, wait WaitStrategy) *sequencerBase {
	if wait == nil {
		wait = BlockingWait()
	}
	b := &sequencerBase{
		bufferSize: size,
		cursor:     NewSequence(InitialSequence),
		wait:       wait,
		closedCh:   make(chan struct{}),
	}
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
	return &SequenceBarrier{sequencer: nil, wait: b.wait, cursor: b.cursor, dependent: dependent, closed: &b.closed, closedCh: b.closedCh}
}

func (b *sequencerBase) Close() {
	b.sealed.Store(true)
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		close(b.closedCh)
		b.wait.signalAll()
	})
}

func (b *sequencerBase) waitForCapacity(ctx context.Context, wrapPoint, fallback int64) (int64, error) {
	for {
		if b.sealed.Load() {
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

func (b *sequencerBase) beginShutdown() error {
	if b.closed.Load() || !b.sealed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	return nil
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
