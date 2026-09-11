package disruptor

import (
	"context"
	"sync/atomic"
)

// SequenceBarrier coordinates a consumer with the producer cursor and optional
// upstream consumer sequences.
type SequenceBarrier struct {
	sequencer Sequencer
	wait      WaitStrategy
	cursor    *Sequence
	dependent sequenceReader
	alerted   atomic.Bool
}

// WaitFor blocks until desired is available, the context is cancelled, or the
// barrier is alerted. Multi-producer barriers never cross a publication gap.
func (b *SequenceBarrier) WaitFor(ctx context.Context, desired int64) (int64, error) {
	if b.alerted.Load() {
		return 0, ErrAlerted
	}
	available, err := b.wait.waitFor(ctx, desired, b.cursor, b.dependent, &b.alerted)
	if err != nil || available < desired {
		return available, err
	}
	return b.sequencer.HighestPublished(desired, available), nil
}

// Cursor returns the slowest dependency position observed by the barrier.
func (b *SequenceBarrier) Cursor() int64 { return b.dependent.Load() }

// Alert unblocks current waiters with ErrAlerted.
func (b *SequenceBarrier) Alert() {
	b.alerted.Store(true)
	b.wait.signalAll()
}

// ClearAlert permits the barrier to wait again.
func (b *SequenceBarrier) ClearAlert() { b.alerted.Store(false) }

// IsAlerted reports whether the barrier is currently alerted.
func (b *SequenceBarrier) IsAlerted() bool { return b.alerted.Load() }
