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
	nextValue  int64
	cachedGate int64
}

func newSingleProducerSequencer(size int64, wait WaitStrategy) *singleProducerSequencer {
	return &singleProducerSequencer{
		sequencerBase: newSequencerBase(size, wait),
		nextValue:     InitialSequence,
		cachedGate:    InitialSequence,
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
	return next, nil
}

func (s *singleProducerSequencer) Publish(_, high int64) {
	s.cursor.Store(high)
	s.wait.signalAll()
}

func (s *singleProducerSequencer) IsAvailable(sequence int64) bool {
	cursor := s.cursor.Load()
	return sequence <= cursor && sequence > cursor-s.bufferSize
}

func (*singleProducerSequencer) HighestPublished(_ int64, available int64) int64 { return available }

func (s *singleProducerSequencer) RemainingCapacity() int64 {
	consumed := s.minimumGate(s.nextValue)
	return s.bufferSize - (s.nextValue - consumed)
}

func (s *singleProducerSequencer) NewBarrier(dependencies ...*Sequence) *SequenceBarrier {
	barrier := s.sequencerBase.NewBarrier(dependencies...)
	barrier.sequencer = s
	return barrier
}

func (s *singleProducerSequencer) Shutdown(ctx context.Context) error {
	if err := s.beginShutdown(); err != nil {
		return err
	}
	return s.awaitShutdown(ctx, s.nextValue)
}
