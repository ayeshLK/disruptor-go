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
	"testing"
)

func TestDiscardSkipsBatchProcessor(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 4, producer, YieldingWait())
			processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(_ *testEvent, sequence int64, endOfBatch bool) error {
				if sequence != 1 || !endOfBatch {
					t.Errorf("handler received sequence=%d endOfBatch=%t", sequence, endOfBatch)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ring.AddGatingSequences(processor.Sequence())
			result := make(chan error, 1)
			go func() { result <- processor.Run(context.Background()) }()

			sequence, err := ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.DiscardSequence(sequence)
			sequence, err = ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.Get(sequence).Value = sequence
			ring.PublishSequence(sequence)

			waitForSequence(t, processor.Sequence(), sequence)
			if !ring.IsDiscarded(0) {
				t.Fatal("discarded sequence was not reported")
			}
			processor.Halt()
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMultiProducerDiscardClosesPublicationGap(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, YieldingWait())
	defer ring.Close()
	barrier := ring.NewBarrier()
	zero, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	one, err := ring.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ring.PublishSequence(one)

	available, err := barrier.WaitFor(context.Background(), zero)
	if err != nil || available != InitialSequence {
		t.Fatalf("unresolved gap: available=%d err=%v", available, err)
	}
	ring.DiscardSequence(zero)
	available, err = barrier.WaitFor(context.Background(), zero)
	if err != nil || available != one {
		t.Fatalf("discarded gap: available=%d err=%v", available, err)
	}
	if !ring.IsDiscarded(zero) || ring.IsDiscarded(one) {
		t.Fatalf("discard state: zero=%t one=%t", ring.IsDiscarded(zero), ring.IsDiscarded(one))
	}
}

func TestDiscardResolutionFirstWins(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, YieldingWait())
	defer ring.Close()
	high, err := ring.NextN(context.Background(), 2)
	if err != nil || high != 1 {
		t.Fatalf("claim: high=%d err=%v", high, err)
	}

	ring.DiscardSequence(0)
	ring.PublishSequence(0)
	if !ring.IsDiscarded(0) {
		t.Fatal("publish resurrected a discarded sequence")
	}
	ring.PublishSequence(1)
	ring.DiscardSequence(1)
	if ring.IsDiscarded(1) {
		t.Fatal("discard changed a published sequence")
	}
	available, err := ring.NewBarrier().WaitFor(context.Background(), 0)
	if err != nil || available != 1 {
		t.Fatalf("resolved range: available=%d err=%v", available, err)
	}
}

func TestDiscardRangeResolvesEveryClaim(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, YieldingWait())
	defer ring.Close()
	high, err := ring.NextN(context.Background(), 3)
	if err != nil || high != 2 {
		t.Fatalf("claim: high=%d err=%v", high, err)
	}
	ring.DiscardRange(0, 1)
	ring.PublishSequence(2)
	available, err := ring.NewBarrier().WaitFor(context.Background(), 0)
	if err != nil || available != high {
		t.Fatalf("discard range: available=%d err=%v", available, err)
	}
	if !ring.IsDiscarded(0) || !ring.IsDiscarded(1) || ring.IsDiscarded(2) {
		t.Fatalf("discard range state: zero=%t one=%t two=%t", ring.IsDiscarded(0), ring.IsDiscarded(1), ring.IsDiscarded(2))
	}
}

func TestDiscardWraparoundResetsStaleState(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 2, producer, YieldingWait())
			defer ring.Close()
			gate := NewSequence(InitialSequence)
			ring.AddGatingSequences(gate)

			sequence, err := ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.DiscardSequence(sequence)
			sequence, err = ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.Get(sequence).Value = sequence
			ring.PublishSequence(sequence)
			gate.Store(sequence)

			sequence, err = ring.Next(context.Background())
			if err != nil || sequence != 2 {
				t.Fatalf("wrapped claim: sequence=%d err=%v", sequence, err)
			}
			ring.Get(sequence).Value = sequence
			ring.PublishSequence(sequence)
			available, err := ring.NewBarrier().WaitFor(context.Background(), sequence)
			if err != nil || available != sequence {
				t.Fatalf("wrapped publication: available=%d err=%v", available, err)
			}
			if ring.IsDiscarded(0) || ring.IsDiscarded(sequence) || ring.Get(sequence).Value != sequence {
				t.Fatalf("stale discard state: old=%t current=%t value=%d", ring.IsDiscarded(0), ring.IsDiscarded(sequence), ring.Get(sequence).Value)
			}
		})
	}
}

func TestMultiProducerOutOfOrderDiscardAndPublish(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, YieldingWait())
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(event *testEvent, sequence int64, endOfBatch bool) error {
		if sequence != event.Value {
			t.Errorf("event identity: sequence=%d value=%d", sequence, event.Value)
		}
		if sequence == 2 && !endOfBatch {
			t.Error("last delivered event was not marked end of batch")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ring.AddGatingSequences(processor.Sequence())
	result := make(chan error, 1)
	go func() { result <- processor.Run(context.Background()) }()

	high, err := ring.NextN(context.Background(), 4)
	if err != nil || high != 3 {
		t.Fatalf("claim: high=%d err=%v", high, err)
	}
	for sequence := int64(0); sequence <= high; sequence++ {
		ring.Get(sequence).Value = sequence
	}
	ring.PublishSequence(1)
	ring.DiscardSequence(0)
	ring.DiscardSequence(3)
	ring.PublishSequence(2)

	waitForSequence(t, processor.Sequence(), high)
	processor.Halt()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	ring.Close()
}

func TestDiscardedClaimDrainsShutdownAfterPanic(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 4, producer, BlockingWait())
			processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			ring.AddGatingSequences(processor.Sequence())
			result := make(chan error, 1)
			go func() { result <- processor.Run(context.Background()) }()

			sequence, err := ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			func() {
				defer func() {
					ring.DiscardSequence(sequence)
					if recover() == nil {
						t.Error("producer panic was not recovered")
					}
				}()
				panic("abandoned claim")
			}()
			waitForSequence(t, processor.Sequence(), sequence)

			if err := ring.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := <-result; !errors.Is(err, ErrClosed) {
				t.Fatalf("processor shutdown: %v", err)
			}
		})
	}
}

func TestDiscardAfterClaimCancellation(t *testing.T) {
	ring := newTestRing(t, 4, MultiProducer, YieldingWait())
	defer ring.Close()
	ctx, cancel := context.WithCancel(context.Background())
	sequence, err := ring.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	ring.DiscardSequence(sequence)
	available, err := ring.NewBarrier().WaitFor(context.Background(), sequence)
	if err != nil || available != sequence {
		t.Fatalf("discard after cancellation: available=%d err=%v", available, err)
	}
}

func TestDiscardDoesNotAllocate(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 1024, producer, BusySpinWait())
			allocations := testing.AllocsPerRun(1000, func() {
				sequence, err := ring.TryNext()
				if err != nil {
					t.Fatal(err)
				}
				ring.DiscardSequence(sequence)
			})
			if allocations != 0 {
				t.Fatalf("claim/discard allocations: got %v, want 0", allocations)
			}
		})
	}
}
