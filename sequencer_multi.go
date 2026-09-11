package disruptor

import (
	"context"
	"math/bits"
	"runtime"
	"sync/atomic"
)

type multiProducerSequencer struct {
	*sequencerBase
	gateCache  *Sequence
	available  []atomic.Int64
	indexMask  uint64
	indexShift uint
}

func newMultiProducerSequencer(size int64, wait WaitStrategy) *multiProducerSequencer {
	available := make([]atomic.Int64, size)
	for i := range available {
		available[i].Store(-1)
	}
	return &multiProducerSequencer{
		sequencerBase: newSequencerBase(size, wait),
		gateCache:     NewSequence(InitialSequence),
		available:     available,
		indexMask:     uint64(size - 1),
		indexShift:    uint(bits.TrailingZeros64(uint64(size))),
	}
}

func (s *multiProducerSequencer) Next(ctx context.Context, count int64) (int64, error) {
	if count < 1 || count > s.bufferSize {
		return 0, ErrInvalidClaimSize
	}
	for {
		if s.closed.Load() {
			return 0, ErrClosed
		}
		current := s.cursor.Load()
		next := current + count
		wrapPoint := next - s.bufferSize
		cached := s.gateCache.Load()
		if wrapPoint > cached || cached > current {
			minimum := s.minimumGate(current)
			if wrapPoint > minimum {
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				default:
					runtime.Gosched()
					continue
				}
			}
			s.gateCache.Store(minimum)
		}
		if s.cursor.CompareAndSwap(current, next) {
			return next, nil
		}
	}
}

func (s *multiProducerSequencer) TryNext(count int64) (int64, error) {
	if count < 1 || count > s.bufferSize {
		return 0, ErrInvalidClaimSize
	}
	for {
		if s.closed.Load() {
			return 0, ErrClosed
		}
		current := s.cursor.Load()
		next := current + count
		wrapPoint := next - s.bufferSize
		cached := s.gateCache.Load()
		if wrapPoint > cached || cached > current {
			minimum := s.minimumGate(current)
			s.gateCache.Store(minimum)
			if wrapPoint > minimum {
				return 0, ErrInsufficientCapacity
			}
		}
		if s.cursor.CompareAndSwap(current, next) {
			return next, nil
		}
	}
}

func (s *multiProducerSequencer) Publish(low, high int64) {
	for sequence := low; sequence <= high; sequence++ {
		s.available[s.index(sequence)].Store(s.flag(sequence))
	}
	s.wait.signalAll()
}

func (s *multiProducerSequencer) IsAvailable(sequence int64) bool {
	return s.available[s.index(sequence)].Load() == s.flag(sequence)
}

func (s *multiProducerSequencer) HighestPublished(lower, available int64) int64 {
	for sequence := lower; sequence <= available; sequence++ {
		if !s.IsAvailable(sequence) {
			return sequence - 1
		}
	}
	return available
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

func (s *multiProducerSequencer) index(sequence int64) int {
	return int(uint64(sequence) & s.indexMask)
}

func (s *multiProducerSequencer) flag(sequence int64) int64 {
	return int64(uint64(sequence) >> s.indexShift)
}
