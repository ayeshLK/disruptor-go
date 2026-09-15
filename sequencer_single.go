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

import "context"

type singleProducerSequencer struct {
	*sequencerBase
	nextValue    int64
	cachedGate   int64
	publications []publicationSlot
}

func newSingleProducerSequencer(size int64, wait WaitStrategy, producerWait ProducerWaitMode) *singleProducerSequencer {
	return &singleProducerSequencer{
		sequencerBase: newSequencerBase(size, wait, producerWait),
		nextValue:     InitialSequence,
		cachedGate:    InitialSequence,
		publications:  make([]publicationSlot, size),
	}
}

func (s *singleProducerSequencer) Next(ctx context.Context, count int64) (int64, error) {
	if s.sealed.Load() {
		return 0, ErrClosed
	}
	if count < 1 || count > s.bufferSize {
		return 0, ErrInvalidClaimSize
	}
	next := s.nextValue + count
	wrapPoint := next - s.bufferSize
	if wrapPoint > s.cachedGate || s.cachedGate > s.nextValue {
		minimum, err := s.waitForCapacity(ctx, wrapPoint, s.nextValue)
		if err != nil {
			return 0, err
		}
		s.cachedGate = minimum
	}
	s.nextValue = next
	s.prepare(next-count+1, next)
	return next, nil
}

func (s *singleProducerSequencer) TryNext(count int64) (int64, error) {
	if s.sealed.Load() {
		return 0, ErrClosed
	}
	if count < 1 || count > s.bufferSize {
		return 0, ErrInvalidClaimSize
	}
	next := s.nextValue + count
	wrapPoint := next - s.bufferSize
	if wrapPoint > s.cachedGate || s.cachedGate > s.nextValue {
		minimum := s.minimumGate(s.nextValue)
		s.cachedGate = minimum
		if wrapPoint > minimum {
			return 0, ErrInsufficientCapacity
		}
	}
	s.nextValue = next
	s.prepare(next-count+1, next)
	return next, nil
}

func (s *singleProducerSequencer) Publish(low, high int64) {
	s.resolve(low, high, publicationPublished)
	s.advanceCursor(high)
	s.wait.signalAll()
}

func (s *singleProducerSequencer) Discard(low, high int64) {
	s.resolve(low, high, publicationDiscarded)
	s.advanceCursor(high)
	s.wait.signalAll()
}

func (s *singleProducerSequencer) IsAvailable(sequence int64) bool {
	cursor := s.cursor.Load()
	if sequence > cursor || sequence <= cursor-s.bufferSize {
		return false
	}
	return s.resolution(sequence) != publicationUnresolved
}

func (s *singleProducerSequencer) IsDiscarded(sequence int64) bool {
	return s.resolution(sequence) == publicationDiscarded
}

func (s *singleProducerSequencer) HighestPublished(lower, available int64) int64 {
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

func (s *singleProducerSequencer) RemainingCapacity() int64 {
	consumed := s.minimumGate(s.nextValue)
	return s.bufferSize - (s.nextValue - consumed)
}

func (s *singleProducerSequencer) NewBarrier(dependencies ...*Sequence) *SequenceBarrier {
	barrier := s.sequencerBase.NewBarrier(dependencies...)
	barrier.sequencer = s
	return barrier
}

func (s *singleProducerSequencer) prepare(low, high int64) {
	if low > high {
		return
	}
	for sequence := low; ; sequence++ {
		slot := &s.publications[s.index(sequence)]
		preparePublication(&slot.marker, &slot.state, sequence)
		if sequence == high {
			return
		}
	}
}

func (s *singleProducerSequencer) resolve(low, high int64, resolution uint32) {
	if low > high {
		return
	}
	for sequence := low; ; sequence++ {
		slot := &s.publications[s.index(sequence)]
		resolvePublication(&slot.marker, &slot.state, sequence, resolution)
		if sequence == high {
			return
		}
	}
}

func (s *singleProducerSequencer) resolution(sequence int64) uint32 {
	slot := &s.publications[s.index(sequence)]
	return publicationState(&slot.marker, &slot.state, sequence)
}

func (s *singleProducerSequencer) index(sequence int64) int {
	return int(uint64(sequence) & uint64(s.bufferSize-1))
}

func (s *singleProducerSequencer) advanceCursor(sequence int64) {
	if sequence > s.cursor.Load() {
		s.cursor.store(sequence)
	}
}

func (s *singleProducerSequencer) Shutdown(ctx context.Context) error {
	if err := s.beginShutdown(); err != nil {
		return err
	}
	return s.awaitShutdown(ctx, s.nextValue)
}
