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

// PollState describes the result of a non-blocking EventPoller.Poll call.
type PollState uint8

const (
	// PollIdle indicates that the producer has not published another sequence.
	PollIdle PollState = iota
	// PollGating indicates that publication or an upstream dependency is ahead
	// of the poller's next sequence, but the sequence is not yet consumable.
	PollGating
	// PollProcessing indicates that at least one sequence was acknowledged.
	PollProcessing
)

// EventPoller consumes currently available events without owning a goroutine.
// Call Poll from one application-controlled event loop. An EventPoller must not
// be copied after first use; pass it by pointer.
type EventPoller[T any] struct {
	ring         *RingBuffer[T]
	barrier      *SequenceBarrier
	handler      EventHandler[T]
	sequence     *Sequence
	maxBatchSize int64
}

// NewEventPoller creates a non-blocking consumer for ring using barrier.
// Processor options, including WithMaxBatchSize, configure each poll batch.
func NewEventPoller[T any](ring *RingBuffer[T], barrier *SequenceBarrier, handler EventHandler[T], options ...ProcessorOption) (*EventPoller[T], error) {
	if handler == nil {
		return nil, ErrNilHandler
	}
	config := processorConfig{maxBatchSize: int64(^uint64(0) >> 1)}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &EventPoller[T]{ring: ring, barrier: barrier, handler: handler, sequence: NewSequence(InitialSequence), maxBatchSize: config.maxBatchSize}, nil
}

// Sequence returns the poller's last fully acknowledged batch position.
func (p *EventPoller[T]) Sequence() *Sequence { return p.sequence }

// Poll processes up to the configured batch limit without waiting. It returns
// PollIdle when no new producer sequence exists, PollGating when a publication
// gap or upstream dependency prevents progress, and PollProcessing after a
// batch is acknowledged. A handler error or panic leaves that batch
// unacknowledged for a later poll and is returned as HandlerError or
// HandlerPanicError. Visible events are processed before a closed ring returns
// ErrClosed; an alert returns ErrAlerted.
func (p *EventPoller[T]) Poll() (PollState, error) {
	if p.barrier.IsAlerted() {
		return PollIdle, ErrAlerted
	}
	next := p.sequence.Load() + 1
	producerAvailable := p.barrier.cursor.Load()
	dependentAvailable := p.barrier.dependent.Load()
	if dependentAvailable < next {
		if p.barrier.closed.Load() {
			return PollIdle, ErrClosed
		}
		if producerAvailable >= next {
			return PollGating, nil
		}
		return PollIdle, nil
	}

	available := p.barrier.sequencer.HighestPublished(next, dependentAvailable)
	if available < next {
		if p.barrier.closed.Load() {
			return PollIdle, ErrClosed
		}
		return PollGating, nil
	}
	end := available
	if available-next+1 > p.maxBatchSize {
		end = next + p.maxBatchSize - 1
	}
	if err := processBatch(p.ring, p.handler, next, end); err != nil {
		return PollProcessing, err
	}
	p.sequence.Store(end)
	return PollProcessing, nil
}
