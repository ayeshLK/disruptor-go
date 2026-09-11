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
	"errors"
	"fmt"
	"sync/atomic"
)

// EventHandler processes one published event in sequence order.
type EventHandler[T any] func(event T, sequence int64, endOfBatch bool) error

type processorConfig struct{ maxBatchSize int64 }

// ProcessorOption configures a BatchProcessor.
type ProcessorOption func(*processorConfig) error

// WithMaxBatchSize limits the number of events acknowledged as one batch.
func WithMaxBatchSize(size int64) ProcessorOption {
	return func(config *processorConfig) error {
		if size < 1 {
			return ErrInvalidBatchSize
		}
		config.maxBatchSize = size
		return nil
	}
}

const (
	processorIdle int32 = iota
	processorRunning
	processorHalted
)

// BatchProcessor waits on a barrier, handles all currently available events in
// order, then advances its consumer sequence once per batch.
type BatchProcessor[T any] struct {
	ring         *RingBuffer[T]
	barrier      *SequenceBarrier
	handler      EventHandler[T]
	sequence     *Sequence
	maxBatchSize int64
	state        atomic.Int32
}

// NewBatchProcessor creates an ordered consumer for ring using barrier.
func NewBatchProcessor[T any](ring *RingBuffer[T], barrier *SequenceBarrier, handler EventHandler[T], options ...ProcessorOption) (*BatchProcessor[T], error) {
	if handler == nil {
		return nil, ErrNilHandler
	}
	config := processorConfig{maxBatchSize: int64(^uint64(0) >> 1)}
	for _, option := range options {
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	return &BatchProcessor[T]{ring: ring, barrier: barrier, handler: handler, sequence: NewSequence(InitialSequence), maxBatchSize: config.maxBatchSize}, nil
}

// Sequence returns the processor's last fully acknowledged batch position.
func (p *BatchProcessor[T]) Sequence() *Sequence { return p.sequence }

// Running reports whether Run currently owns the processor lifecycle.
func (p *BatchProcessor[T]) Running() bool { return p.state.Load() == processorRunning }

// Run processes events until Halt is called, the context is cancelled, the ring
// is closed, or a handler returns an error. Ring closure returns ErrClosed. On
// handler error the current batch is not acknowledged, so restarting the
// processor replays that batch.
func (p *BatchProcessor[T]) Run(ctx context.Context) error {
	if !p.state.CompareAndSwap(processorIdle, processorRunning) {
		return ErrAlreadyRunning
	}
	p.barrier.ClearAlert()
	defer p.state.Store(processorIdle)

	next := p.sequence.Load() + 1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		available, err := p.barrier.WaitFor(ctx, next)
		if err != nil {
			if errors.Is(err, ErrAlerted) && p.state.Load() == processorHalted {
				return nil
			}
			return err
		}
		end := available
		if available-next+1 > p.maxBatchSize {
			end = next + p.maxBatchSize - 1
		}
		for sequence := next; sequence <= end; sequence++ {
			if err := p.handler(p.ring.Get(sequence), sequence, sequence == end); err != nil {
				return fmt.Errorf("disruptor: handler failed at sequence %d: %w", sequence, err)
			}
		}
		p.sequence.Store(end)
		next = end + 1
	}
}

// Halt alerts the barrier and causes the active Run call to return.
func (p *BatchProcessor[T]) Halt() {
	p.state.Store(processorHalted)
	p.barrier.Alert()
}
