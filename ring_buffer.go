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

// EventFactory creates one reusable event for each physical ring slot.
type EventFactory[T any] func() T

// EventTranslator initializes a claimed event before its sequence is published.
type EventTranslator[T any] func(T, int64) error

// RingBuffer is a preallocated, power-of-two store coordinated by logical
// sequences. The same physical event is reused on every lap.
//
// A handler must not retain or mutate an event after it advances its consumer
// sequence. Prefer a pointer type for T when callers need in-place translation.
type RingBuffer[T any] struct {
	entries   []T
	indexMask uint64
	sequencer Sequencer
	discarder discardSequencer
}

// New creates a ring buffer with preallocated events and the selected producer mode.
// Producer claims yield while waiting for capacity unless configured otherwise.
func New[T any](size int64, producerType ProducerType, factory EventFactory[T], wait WaitStrategy, options ...RingOption) (*RingBuffer[T], error) {
	if size < 1 || size&(size-1) != 0 {
		return nil, ErrInvalidBufferSize
	}
	if factory == nil {
		return nil, ErrNilFactory
	}
	config := ringConfig{producerWait: ProducerWaitYielding}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	entries := make([]T, int(size))
	for i := range entries {
		entries[i] = factory()
	}

	var sequencer Sequencer
	switch producerType {
	case SingleProducer:
		sequencer = newSingleProducerSequencer(size, wait, config.producerWait)
	case MultiProducer:
		sequencer = newMultiProducerSequencer(size, wait, config.producerWait)
	default:
		return nil, ErrInvalidProducerType
	}
	return &RingBuffer[T]{entries: entries, indexMask: uint64(size - 1), sequencer: sequencer, discarder: sequencer.(discardSequencer)}, nil
}

// Get returns the reusable event stored at sequence's physical ring slot.
func (r *RingBuffer[T]) Get(sequence int64) T {
	return r.entries[int(uint64(sequence)&r.indexMask)]
}

// Next blocks until one sequence can be claimed or ctx is cancelled.
func (r *RingBuffer[T]) Next(ctx context.Context) (int64, error) {
	return r.sequencer.Next(ctx, 1)
}

// NextN blocks until count contiguous sequences can be claimed and returns the highest.
func (r *RingBuffer[T]) NextN(ctx context.Context, count int64) (int64, error) {
	return r.sequencer.Next(ctx, count)
}

// TryNext claims one sequence without blocking.
func (r *RingBuffer[T]) TryNext() (int64, error) { return r.sequencer.TryNext(1) }

// TryNextN claims count contiguous sequences without blocking and returns the highest.
func (r *RingBuffer[T]) TryNextN(count int64) (int64, error) {
	return r.sequencer.TryNext(count)
}

// PublishSequence makes one claimed sequence visible to consumers. Every
// successful raw claim must be resolved by either publishing or discarding it.
func (r *RingBuffer[T]) PublishSequence(sequence int64) {
	r.sequencer.Publish(sequence, sequence)
}

// PublishRange makes every claimed sequence from low through high visible.
// Every successful raw claim must be resolved by either publishing or
// discarding it.
func (r *RingBuffer[T]) PublishRange(low, high int64) {
	r.sequencer.Publish(low, high)
}

// DiscardSequence resolves a claimed sequence without delivering its event to
// processors. Duplicate resolution calls are ignored; the first terminal
// resolution wins.
func (r *RingBuffer[T]) DiscardSequence(sequence int64) {
	r.discarder.Discard(sequence, sequence)
}

// DiscardRange resolves every claimed sequence from low through high without
// delivering their events to processors. Duplicate resolution calls are
// ignored; the first terminal resolution wins.
func (r *RingBuffer[T]) DiscardRange(low, high int64) {
	r.discarder.Discard(low, high)
}

// IsDiscarded reports whether sequence was resolved as discarded in its current
// ring lap. Discarded sequences are visible to barriers but are skipped by
// BatchProcessor handlers.
func (r *RingBuffer[T]) IsDiscarded(sequence int64) bool {
	return r.discarder.IsDiscarded(sequence)
}

// Publish claims one sequence, invokes translator on its preallocated event,
// and publishes the sequence even when translator returns an error.
func (r *RingBuffer[T]) Publish(ctx context.Context, translator EventTranslator[T]) (err error) {
	if translator == nil {
		return ErrNilTranslator
	}
	sequence, err := r.Next(ctx)
	if err != nil {
		return err
	}
	defer r.PublishSequence(sequence)
	return translator(r.Get(sequence), sequence)
}

// TryPublish is the non-blocking form of Publish.
func (r *RingBuffer[T]) TryPublish(translator EventTranslator[T]) (err error) {
	if translator == nil {
		return ErrNilTranslator
	}
	sequence, err := r.TryNext()
	if err != nil {
		return err
	}
	defer r.PublishSequence(sequence)
	return translator(r.Get(sequence), sequence)
}

// Cursor returns the producer cursor.
func (r *RingBuffer[T]) Cursor() int64 { return r.sequencer.Cursor().Load() }

// BufferSize returns the fixed number of physical ring slots.
func (r *RingBuffer[T]) BufferSize() int64 { return r.sequencer.BufferSize() }

// RemainingCapacity returns the number of sequences claimable without overtaking a gate.
func (r *RingBuffer[T]) RemainingCapacity() int64 { return r.sequencer.RemainingCapacity() }

// NewBarrier creates a consumer barrier over the producer cursor or dependencies.
func (r *RingBuffer[T]) NewBarrier(dependencies ...*Sequence) *SequenceBarrier {
	return r.sequencer.NewBarrier(dependencies...)
}

// AddGatingSequences prevents producers from wrapping over the supplied consumers.
func (r *RingBuffer[T]) AddGatingSequences(sequences ...*Sequence) {
	r.sequencer.AddGatingSequences(sequences...)
}

// RemoveGatingSequence removes a gate and reports whether it was registered.
func (r *RingBuffer[T]) RemoveGatingSequence(sequence *Sequence) bool {
	return r.sequencer.RemoveGatingSequence(sequence)
}

// Shutdown prevents future claims, waits for the gating sequences present when
// it begins to acknowledge the final claimed sequence, and then closes consumer
// waits. Publisher operations must have returned before Shutdown is called.
// Shutdown returns a context error if draining does not complete; the ring is
// closed whether draining succeeds or fails.
func (r *RingBuffer[T]) Shutdown(ctx context.Context) error {
	return r.sequencer.Shutdown(ctx)
}

// Close immediately prevents future claims and unblocks waits for unavailable
// sequences with ErrClosed. Use Shutdown to drain registered gating sequences
// before closing consumers.
func (r *RingBuffer[T]) Close() { r.sequencer.Close() }
