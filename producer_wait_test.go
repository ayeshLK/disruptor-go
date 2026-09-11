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
	"runtime"
	"testing"
	"time"
)

type producerClaimResult struct {
	sequence int64
	err      error
}

func TestProducerWaitConfiguration(t *testing.T) {
	ring, err := New(1, SingleProducer, func() *testEvent { return new(testEvent) }, BlockingWait())
	if err != nil {
		t.Fatal(err)
	}
	if got := producerWaiterFor(t, ring).mode; got != ProducerWaitYielding {
		t.Fatalf("default producer wait: got %d", got)
	}
	ring.Close()

	if _, err = New(1, SingleProducer, func() *testEvent { return new(testEvent) }, BlockingWait(), WithProducerWait(ProducerWaitMode(255))); !errors.Is(err, ErrInvalidProducerWaitMode) {
		t.Fatalf("invalid producer wait: got %v", err)
	}
}

func TestBlockingProducerWaitWakesForGateUpdates(t *testing.T) {
	updates := map[string]func(*Sequence){
		"store": func(gate *Sequence) { gate.Store(0) },
		"add":   func(gate *Sequence) { gate.Add(1) },
		"cas": func(gate *Sequence) {
			if !gate.CompareAndSwap(InitialSequence, 0) {
				t.Fatal("gate compare-and-swap failed")
			}
		},
	}
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		for name, update := range updates {
			t.Run(producerName(producer)+"/"+name, func(t *testing.T) {
				ring, gate := fullProducerWaitTestRing(t, producer)
				defer ring.Close()
				result := claimInBackground(ring, context.Background())
				waitForProducerWaiters(t, ring, 1)

				update(gate)
				got := receiveProducerClaim(t, result)
				if got.err != nil || got.sequence != 1 {
					t.Fatalf("wrapped claim: sequence=%d err=%v", got.sequence, got.err)
				}
			})
		}
	}
}

func TestBlockingProducerWaitCancellationAndClose(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer)+"/cancel", func(t *testing.T) {
			ring, _ := fullProducerWaitTestRing(t, producer)
			defer ring.Close()
			ctx, cancel := context.WithCancel(context.Background())
			result := claimInBackground(ring, ctx)
			waitForProducerWaiters(t, ring, 1)
			cancel()
			if err := receiveProducerClaim(t, result).err; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled claim: got %v", err)
			}
		})

		t.Run(producerName(producer)+"/close", func(t *testing.T) {
			ring, _ := fullProducerWaitTestRing(t, producer)
			result := claimInBackground(ring, context.Background())
			waitForProducerWaiters(t, ring, 1)
			ring.Close()
			if err := receiveProducerClaim(t, result).err; !errors.Is(err, ErrClosed) {
				t.Fatalf("closed claim: got %v", err)
			}
		})
	}
}
func TestBlockingProducerWaitWakesForShutdown(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring, gate := fullProducerWaitTestRing(t, producer)
			result := claimInBackground(ring, context.Background())
			waitForProducerWaiters(t, ring, 1)

			shutdownResult := make(chan error, 1)
			go func() { shutdownResult <- ring.Shutdown(context.Background()) }()
			if err := receiveProducerClaim(t, result).err; !errors.Is(err, ErrClosed) {
				t.Fatalf("claim during shutdown: got %v", err)
			}
			gate.Store(0)
			select {
			case err := <-shutdownResult:
				if err != nil {
					t.Fatalf("shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for shutdown")
			}
		})
	}
}

func TestBlockingProducerWaitRechecksAfterSpuriousWake(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring, gate := fullProducerWaitTestRing(t, producer)
			defer ring.Close()
			result := claimInBackground(ring, context.Background())
			waiter := producerWaiterFor(t, ring)
			waitForProducerWaiters(t, ring, 1)

			waiter.signalAll()
			waitForProducerWaitCalls(t, waiter, 2)
			select {
			case got := <-result:
				t.Fatalf("spurious wake claimed capacity: sequence=%d err=%v", got.sequence, got.err)
			default:
			}

			gate.Store(0)
			if got := receiveProducerClaim(t, result); got.err != nil || got.sequence != 1 {
				t.Fatalf("claim after gate advance: sequence=%d err=%v", got.sequence, got.err)
			}
		})
	}
}

func TestBlockingProducerWaitUsesSlowestGate(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newProducerWaitTestRing(t, producer, ProducerWaitBlocking)
			defer ring.Close()
			first := NewSequence(InitialSequence)
			second := NewSequence(InitialSequence)
			ring.AddGatingSequences(first, second)
			fillProducerWaitTestRing(t, ring)
			result := claimInBackground(ring, context.Background())
			waiter := producerWaiterFor(t, ring)
			waitForProducerWaiters(t, ring, 1)

			first.Store(0)
			waitForProducerWaitCalls(t, waiter, 2)
			select {
			case got := <-result:
				t.Fatalf("claim passed slow gate: sequence=%d err=%v", got.sequence, got.err)
			default:
			}

			second.Store(0)
			if got := receiveProducerClaim(t, result); got.err != nil || got.sequence != 1 {
				t.Fatalf("claim after both gates: sequence=%d err=%v", got.sequence, got.err)
			}
		})
	}
}

func TestBlockingProducerWaitWakesWhenSlowGateIsRemoved(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newProducerWaitTestRing(t, producer, ProducerWaitBlocking)
			defer ring.Close()
			fast := NewSequence(InitialSequence)
			slow := NewSequence(InitialSequence)
			ring.AddGatingSequences(fast, slow)
			fillProducerWaitTestRing(t, ring)
			fast.Store(0)
			result := claimInBackground(ring, context.Background())
			waitForProducerWaiters(t, ring, 1)

			if !ring.RemoveGatingSequence(slow) {
				t.Fatal("slow gate was not registered")
			}
			if got := receiveProducerClaim(t, result); got.err != nil || got.sequence != 1 {
				t.Fatalf("claim after gate removal: sequence=%d err=%v", got.sequence, got.err)
			}
		})
	}
}

func TestBlockingProducerWaitSupportsConcurrentMultiProducerClaims(t *testing.T) {
	ring, err := New(4, MultiProducer, func() *testEvent { return new(testEvent) }, BlockingWait(), WithProducerWait(ProducerWaitBlocking))
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	high, err := ring.NextN(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	ring.PublishRange(0, high)

	const producers = 4
	results := make([]<-chan producerClaimResult, producers)
	for i := range results {
		results[i] = claimInBackground(ring, context.Background())
	}
	waitForProducerWaiters(t, ring, producers)
	gate.Store(3)

	seen := make(map[int64]bool, producers)
	for _, result := range results {
		got := receiveProducerClaim(t, result)
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.sequence < 4 || got.sequence > 7 || seen[got.sequence] {
			t.Fatalf("invalid concurrent claim %d; seen=%v", got.sequence, seen)
		}
		seen[got.sequence] = true
		ring.PublishSequence(got.sequence)
	}
}

func newProducerWaitTestRing(t *testing.T, producer ProducerType, mode ProducerWaitMode) *RingBuffer[*testEvent] {
	t.Helper()
	ring, err := New(1, producer, func() *testEvent { return new(testEvent) }, BlockingWait(), WithProducerWait(mode))
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func fullProducerWaitTestRing(t *testing.T, producer ProducerType) (*RingBuffer[*testEvent], *Sequence) {
	t.Helper()
	ring := newProducerWaitTestRing(t, producer, ProducerWaitBlocking)
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	fillProducerWaitTestRing(t, ring)
	return ring, gate
}

func fillProducerWaitTestRing(t *testing.T, ring *RingBuffer[*testEvent]) {
	t.Helper()
	sequence, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ring.PublishSequence(sequence)
}

func claimInBackground(ring *RingBuffer[*testEvent], ctx context.Context) <-chan producerClaimResult {
	result := make(chan producerClaimResult, 1)
	go func() {
		sequence, err := ring.Next(ctx)
		result <- producerClaimResult{sequence: sequence, err: err}
	}()
	return result
}

func producerWaiterFor(t *testing.T, ring *RingBuffer[*testEvent]) *producerWaiter {
	t.Helper()
	switch sequencer := ring.sequencer.(type) {
	case *singleProducerSequencer:
		return sequencer.producerWait
	case *multiProducerSequencer:
		return sequencer.producerWait
	default:
		t.Fatalf("unknown sequencer %T", ring.sequencer)
		return nil
	}
}

func waitForProducerWaiters(t *testing.T, ring *RingBuffer[*testEvent], expected int64) {
	t.Helper()
	waiter := producerWaiterFor(t, ring)
	deadline := time.Now().Add(5 * time.Second)
	for waiter.waiting.Load() != expected {
		if time.Now().After(deadline) {
			t.Fatalf("producer waiters: got %d, want %d", waiter.waiting.Load(), expected)
		}
		runtime.Gosched()
	}
}

func waitForProducerWaitCalls(t *testing.T, waiter *producerWaiter, expected uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for waiter.waits.Load() < expected {
		if time.Now().After(deadline) {
			t.Fatalf("producer wait calls: got %d, want at least %d", waiter.waits.Load(), expected)
		}
		runtime.Gosched()
	}
}

func receiveProducerClaim(t *testing.T, result <-chan producerClaimResult) producerClaimResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for producer claim")
		return producerClaimResult{}
	}
}

func TestProducerWaitModesPreserveWraparound(t *testing.T) {
	for name, mode := range map[string]ProducerWaitMode{
		"yielding":  ProducerWaitYielding,
		"blocking":  ProducerWaitBlocking,
		"busy-spin": ProducerWaitBusySpin,
	} {
		for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
			t.Run(name+"/"+producerName(producer), func(t *testing.T) {
				ring := newProducerWaitTestRing(t, producer, mode)
				defer ring.Close()
				gate := NewSequence(InitialSequence)
				ring.AddGatingSequences(gate)
				fillProducerWaitTestRing(t, ring)
				result := claimInBackground(ring, context.Background())

				gate.Store(0)
				got := receiveProducerClaim(t, result)
				if got.err != nil || got.sequence != 1 {
					t.Fatalf("wrapped claim: sequence=%d err=%v", got.sequence, got.err)
				}
			})
		}
	}
}
