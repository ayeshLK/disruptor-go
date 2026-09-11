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

// ProducerWaitMode controls how a blocking producer claim waits for consumer
// gating sequences to release capacity.
type ProducerWaitMode uint8

const (
	// ProducerWaitYielding yields to the Go scheduler while capacity is unavailable.
	// It is the default and preserves the original producer wait behavior.
	ProducerWaitYielding ProducerWaitMode = iota
	// ProducerWaitBlocking sleeps until a gate advances, a gate is removed, the
	// context is cancelled, or the ring closes.
	ProducerWaitBlocking
	// ProducerWaitBusySpin continuously checks capacity and is intended for
	// latency-sensitive workloads with a dedicated CPU core.
	ProducerWaitBusySpin
)

type ringConfig struct{ producerWait ProducerWaitMode }

// RingOption configures a RingBuffer.
type RingOption func(*ringConfig) error

// WithProducerWait selects how blocking producer claims wait for capacity.
func WithProducerWait(mode ProducerWaitMode) RingOption {
	return func(config *ringConfig) error {
		if mode > ProducerWaitBusySpin {
			return ErrInvalidProducerWaitMode
		}
		config.producerWait = mode
		return nil
	}
}

type producerWaiter struct {
	mode    ProducerWaitMode
	mu      sync.Mutex
	ch      chan struct{}
	waiting atomic.Int64
	waits   atomic.Uint64
}

func newProducerWaiter(mode ProducerWaitMode) *producerWaiter {
	w := &producerWaiter{mode: mode}
	if mode == ProducerWaitBlocking {
		w.ch = make(chan struct{})
	}
	return w
}

func (w *producerWaiter) needsSignals() bool { return w.mode == ProducerWaitBlocking }

func (w *producerWaiter) prepare() <-chan struct{} {
	if w.mode != ProducerWaitBlocking {
		return nil
	}
	w.mu.Lock()
	ch := w.ch
	w.mu.Unlock()
	return ch
}

func (w *producerWaiter) wait(ctx context.Context, prepared <-chan struct{}, closed <-chan struct{}) error {
	switch w.mode {
	case ProducerWaitBlocking:
		w.waits.Add(1)
		w.waiting.Add(1)
		defer w.waiting.Add(-1)
		select {
		case <-prepared:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return ErrClosed
		}
	case ProducerWaitBusySpin:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return ErrClosed
		default:
			return nil
		}
	default:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closed:
			return ErrClosed
		default:
			runtime.Gosched()
			return nil
		}
	}
}

func (w *producerWaiter) signalAll() {
	if w.mode != ProducerWaitBlocking {
		return
	}
	w.mu.Lock()
	close(w.ch)
	w.ch = make(chan struct{})
	w.mu.Unlock()
}
