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
	if s.closed.Load() {
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
	if s.closed.Load() {
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
