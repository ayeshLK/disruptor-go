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
	"fmt"
	"testing"
)

const maxSequenceValue = int64(1<<63 - 1)

func TestProtocolManyLapsPreserveEventIdentity(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		for _, size := range []int64{2, 4, 8} {
			t.Run(fmt.Sprintf("%s/size-%d", producerName(producer), size), func(t *testing.T) {
				ring := newTestRing(t, size, producer, YieldingWait())
				defer ring.Close()
				gate := NewSequence(InitialSequence)
				ring.AddGatingSequences(gate)
				barrier := ring.NewBarrier()
				slots := make([]*testEvent, size)
				for sequence := int64(0); sequence < size*32; sequence++ {
					if got := ring.RemainingCapacity(); got != size {
						t.Fatalf("sequence %d: capacity before claim=%d, want %d", sequence, got, size)
					}
					claimed, err := ring.TryNext()
					if err != nil || claimed != sequence {
						t.Fatalf("claim %d: sequence=%d err=%v", sequence, claimed, err)
					}
					event := ring.Get(claimed)
					if previous := slots[int(sequence%size)]; previous != nil && event != previous {
						t.Fatalf("sequence %d: slot was not reused", sequence)
					}
					slots[int(sequence%size)] = event
					event.Value = sequence
					event.Check = sequence*31 + 7
					ring.PublishSequence(claimed)
					if got := ring.RemainingCapacity(); got != size-1 {
						t.Fatalf("sequence %d: capacity after claim=%d, want %d", sequence, got, size-1)
					}
					available, err := barrier.WaitFor(context.Background(), sequence)
					if err != nil || available != sequence {
						t.Fatalf("sequence %d: available=%d err=%v", sequence, available, err)
					}
					if event.Value != sequence || event.Check != sequence*31+7 {
						t.Fatalf("sequence %d: event value=%d check=%d", sequence, event.Value, event.Check)
					}
					gate.Store(sequence)
				}
			})
		}
	}
}

func TestMultiProducerPublicationGapAcrossWrap(t *testing.T) {
	ring := newTestRing(t, 4, MultiProducer, YieldingWait())
	defer ring.Close()
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	barrier := ring.NewBarrier()

	high, err := ring.NextN(context.Background(), 4)
	if err != nil || high != 3 {
		t.Fatalf("initial claim: high=%d err=%v", high, err)
	}
	for sequence := int64(0); sequence < 2; sequence++ {
		ring.Get(sequence).Check = sequence
		ring.PublishSequence(sequence)
	}
	ring.Get(3).Check = 3
	ring.PublishSequence(3)
	gate.Store(0)

	wrapped, err := ring.TryNext()
	if err != nil || wrapped != 4 {
		t.Fatalf("wrapped claim: sequence=%d err=%v", wrapped, err)
	}
	if ring.Get(wrapped) != ring.Get(0) {
		t.Fatal("wrapped claim did not reuse slot zero")
	}
	ring.Get(wrapped).Check = wrapped
	ring.PublishRange(4, 4)

	available, err := barrier.WaitFor(context.Background(), 2)
	if err != nil || available != 1 {
		t.Fatalf("publication gap: available=%d err=%v", available, err)
	}
	if got := ring.sequencer.HighestPublished(2, 4); got != 1 {
		t.Fatalf("highest published across gap: got %d, want 1", got)
	}

	ring.Get(2).Check = 2
	ring.PublishSequence(2)
	available, err = barrier.WaitFor(context.Background(), 2)
	if err != nil || available != 4 {
		t.Fatalf("closed publication gap: available=%d err=%v", available, err)
	}
	for sequence := int64(2); sequence <= 4; sequence++ {
		if got := ring.Get(sequence).Check; got != sequence {
			t.Fatalf("wrapped event: check=%d, want %d", got, sequence)
		}
	}
}

func TestPipelineDependencyVisibilityAcrossWraps(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 4, producer, YieldingWait())
			upstream, err := NewBatchProcessor(ring, ring.NewBarrier(), func(event *testEvent, sequence int64, _ bool) error {
				event.Check = event.Value*3 + sequence
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			downstream, err := NewBatchProcessor(ring, ring.NewBarrier(upstream.Sequence()), func(event *testEvent, sequence int64, _ bool) error {
				if event.Check != event.Value*3+sequence {
					return fmt.Errorf("sequence %d: got check %d", sequence, event.Check)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ring.AddGatingSequences(downstream.Sequence())
			upstreamResult := make(chan error, 1)
			downstreamResult := make(chan error, 1)
			go func() { upstreamResult <- upstream.Run(context.Background()) }()
			go func() { downstreamResult <- downstream.Run(context.Background()) }()

			const total int64 = 256
			for sequence := int64(0); sequence < total; sequence++ {
				current := sequence
				if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
					event.Value = current
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			waitForSequence(t, downstream.Sequence(), total-1)
			upstream.Halt()
			downstream.Halt()
			if err := <-upstreamResult; err != nil {
				t.Fatal(err)
			}
			if err := <-downstreamResult; err != nil {
				t.Fatal(err)
			}
			ring.Close()
		})
	}
}

func TestBroadcastGatesProtectWrappedClaims(t *testing.T) {
	ring, err := New(2, SingleProducer, func() *testEvent { return new(testEvent) }, BlockingWait(), WithProducerWait(ProducerWaitBlocking))
	if err != nil {
		t.Fatal(err)
	}
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	processors := make([]*BatchProcessor[*testEvent], 2)
	results := make([]chan error, 2)
	for index := range processors {
		index := index
		processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(_ *testEvent, sequence int64, _ bool) error {
			if sequence == 0 {
				close(started[index])
				<-release[index]
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		processors[index] = processor
		results[index] = make(chan error, 1)
		ring.AddGatingSequences(processor.Sequence())
		go func() { results[index] <- processor.Run(context.Background()) }()
	}

	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	<-started[0]
	<-started[1]
	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}

	claim := claimInBackground(ring, context.Background())
	waitForProducerWaiters(t, ring, 1)
	close(release[0])
	waitForSequence(t, processors[0].Sequence(), 0)
	select {
	case got := <-claim:
		t.Fatalf("claim passed slow broadcast gate: sequence=%d err=%v", got.sequence, got.err)
	default:
	}
	close(release[1])
	got := receiveProducerClaim(t, claim)
	if got.err != nil || got.sequence != 2 {
		t.Fatalf("claim after both broadcast gates: sequence=%d err=%v", got.sequence, got.err)
	}
	ring.PublishSequence(got.sequence)
	waitForSequence(t, processors[0].Sequence(), got.sequence)
	waitForSequence(t, processors[1].Sequence(), got.sequence)
	for _, processor := range processors {
		processor.Halt()
	}
	for index, result := range results {
		if err := <-result; err != nil {
			t.Fatalf("processor %d: %v", index, err)
		}
	}
	ring.Close()
}

func TestSequenceBoundaryUsesRepresentableValues(t *testing.T) {
	multi := newMultiProducerSequencer(4, YieldingWait(), ProducerWaitYielding)
	for _, sequence := range []int64{maxSequenceValue - 1, maxSequenceValue} {
		index := multi.index(sequence)
		multi.available[index].Store(multi.flag(sequence))
		multi.states[index].Store(publicationPublished)
		if !multi.IsAvailable(sequence) {
			t.Fatalf("sequence %d was not available at signed boundary", sequence)
		}
	}
	if got := multi.HighestPublished(maxSequenceValue-1, maxSequenceValue-1); got != maxSequenceValue-1 {
		t.Fatalf("highest published boundary: got %d", got)
	}

	single := newSingleProducerSequencer(4, YieldingWait(), ProducerWaitYielding)
	single.nextValue = maxSequenceValue - 1
	single.cachedGate = maxSequenceValue - 4
	got, err := single.TryNext(1)
	if err != nil || got != maxSequenceValue {
		t.Fatalf("single producer boundary claim: sequence=%d err=%v", got, err)
	}
}
