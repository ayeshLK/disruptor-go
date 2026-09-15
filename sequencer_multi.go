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
	"math/bits"
	"sync/atomic"
)

type multiProducerSequencer struct {
	*sequencerBase
	gateCache  *Sequence
	available  []atomic.Int64
	states     []atomic.Uint32
	indexMask  uint64
	indexShift uint
}

func newMultiProducerSequencer(size int64, wait WaitStrategy, producerWait ProducerWaitMode) *multiProducerSequencer {
	available := make([]atomic.Int64, size)
	for i := range available {
		available[i].Store(-1)
	}
	return &multiProducerSequencer{
		sequencerBase: newSequencerBase(size, wait, producerWait),
		gateCache:     NewSequence(InitialSequence),
		available:     available,
		states:        make([]atomic.Uint32, size),
		indexMask:     uint64(size - 1),
		indexShift:    uint(bits.TrailingZeros64(uint64(size))),
	}
}

func (s *multiProducerSequencer) Next(ctx context.Context, count int64) (int64, error) {
	if count < 1 || count > s.bufferSize {
		return 0, ErrInvalidClaimSize
	}
	for {
		if s.sealed.Load() {
			return 0, ErrClosed
		}
		current := s.cursor.Load()
		next := current + count
		wrapPoint := next - s.bufferSize
		cached := s.gateCache.Load()
		if wrapPoint > cached || cached > current {
			prepared := s.producerWait.prepare()
			minimum := s.minimumGate(current)
			if wrapPoint > minimum {
				if err := s.producerWait.wait(ctx, prepared, s.closedCh); err != nil {
					return 0, err
				}
				continue
			}
			s.gateCache.store(minimum)
		}
		if s.cursor.compareAndSwap(current, next) {
			s.prepare(current+1, next)
			return next, nil
		}
	}
}

func (s *multiProducerSequencer) TryNext(count int64) (int64, error) {
	if count < 1 || count > s.bufferSize {
		return 0, ErrInvalidClaimSize
	}
	for {
		if s.sealed.Load() {
			return 0, ErrClosed
		}
		current := s.cursor.Load()
		next := current + count
		wrapPoint := next - s.bufferSize
		cached := s.gateCache.Load()
		if wrapPoint > cached || cached > current {
			minimum := s.minimumGate(current)
			s.gateCache.store(minimum)
			if wrapPoint > minimum {
				return 0, ErrInsufficientCapacity
			}
		}
		if s.cursor.compareAndSwap(current, next) {
			s.prepare(current+1, next)
			return next, nil
		}
	}
}

func (s *multiProducerSequencer) Publish(low, high int64) {
	s.resolve(low, high, publicationPublished)
	s.wait.signalAll()
}

func (s *multiProducerSequencer) Discard(low, high int64) {
	s.resolve(low, high, publicationDiscarded)
	s.wait.signalAll()
}

func (s *multiProducerSequencer) IsAvailable(sequence int64) bool {
	return s.resolution(sequence) != publicationUnresolved
}

func (s *multiProducerSequencer) IsDiscarded(sequence int64) bool {
	return s.resolution(sequence) == publicationDiscarded
}

func (s *multiProducerSequencer) HighestPublished(lower, available int64) int64 {
	if lower > available {
		return available
	}
	for sequence := lower; ; sequence++ {
		if s.resolution(sequence) == publicationUnresolved {
			return sequence - 1
		}
		if sequence == available {
			return available
		}
	}
}

func (s *multiProducerSequencer) RemainingCapacity() int64 {
	produced := s.cursor.Load()
	consumed := s.minimumGate(produced)
	return s.bufferSize - (produced - consumed)
}

func (s *multiProducerSequencer) NewBarrier(dependencies ...*Sequence) *SequenceBarrier {
	barrier := s.sequencerBase.NewBarrier(dependencies...)
	barrier.sequencer = s
	return barrier
}

func (s *multiProducerSequencer) prepare(low, high int64) {
	if low > high {
		return
	}
	for sequence := low; ; sequence++ {
		index := s.index(sequence)
		preparePublication(&s.available[index], &s.states[index], s.flag(sequence))
		if sequence == high {
			return
		}
	}
}

func (s *multiProducerSequencer) resolve(low, high int64, resolution uint32) {
	if low > high {
		return
	}
	for sequence := low; ; sequence++ {
		index := s.index(sequence)
		resolvePublication(&s.available[index], &s.states[index], s.flag(sequence), resolution)
		if sequence == high {
			return
		}
	}
}

func (s *multiProducerSequencer) resolution(sequence int64) uint32 {
	index := s.index(sequence)
	return publicationState(&s.available[index], &s.states[index], s.flag(sequence))
}

func (s *multiProducerSequencer) Shutdown(ctx context.Context) error {
	if err := s.beginShutdown(); err != nil {
		return err
	}
	return s.awaitShutdown(ctx, s.cursor.Load())
}

func (s *multiProducerSequencer) index(sequence int64) int {
	return int(uint64(sequence) & s.indexMask)
}

func (s *multiProducerSequencer) flag(sequence int64) int64 {
	return int64(uint64(sequence) >> s.indexShift)
}
