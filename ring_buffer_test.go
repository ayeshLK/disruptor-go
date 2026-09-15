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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testEvent struct {
	Value int64
	Check int64
}

func newTestRing(t *testing.T, size int64, producer ProducerType, wait WaitStrategy) *RingBuffer[*testEvent] {
	t.Helper()
	ring, err := New(size, producer, func() *testEvent { return new(testEvent) }, wait)
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func TestNewValidatesConfiguration(t *testing.T) {
	for _, size := range []int64{-1, 0, 3, 12} {
		if _, err := New(size, SingleProducer, func() *testEvent { return new(testEvent) }, BlockingWait()); !errors.Is(err, ErrInvalidBufferSize) {
			t.Fatalf("size %d: got %v", size, err)
		}
	}
	if _, err := New[*testEvent](8, SingleProducer, nil, BlockingWait()); !errors.Is(err, ErrNilFactory) {
		t.Fatalf("nil factory: got %v", err)
	}
	if _, err := New(8, ProducerType(99), func() *testEvent { return new(testEvent) }, BlockingWait()); !errors.Is(err, ErrInvalidProducerType) {
		t.Fatalf("producer type: got %v", err)
	}
}

func TestSingleProducerClaimPublishAndWrap(t *testing.T) {
	ring := newTestRing(t, 2, SingleProducer, YieldingWait())
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	first := ring.Get(0)

	sequence, err := ring.TryNext()
	if err != nil || sequence != 0 {
		t.Fatalf("first claim: sequence=%d err=%v", sequence, err)
	}
	if ring.Cursor() != InitialSequence {
		t.Fatalf("claim became visible before publish: %d", ring.Cursor())
	}
	ring.PublishSequence(sequence)
	sequence, err = ring.TryNext()
	if err != nil || sequence != 1 {
		t.Fatalf("second claim: sequence=%d err=%v", sequence, err)
	}
	ring.PublishSequence(sequence)
	if _, err := ring.TryNext(); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("full ring: got %v", err)
	}

	gate.Store(0)
	sequence, err = ring.TryNext()
	if err != nil || sequence != 2 {
		t.Fatalf("wrapped claim: sequence=%d err=%v", sequence, err)
	}
	if ring.Get(sequence) != first {
		t.Fatal("sequence 2 did not reuse physical slot 0")
	}
}

func TestMultiProducerBarrierStopsAtPublicationGap(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, YieldingWait())
	barrier := ring.NewBarrier()
	zero, err := ring.TryNext()
	if err != nil {
		t.Fatal(err)
	}
	one, err := ring.TryNext()
	if err != nil {
		t.Fatal(err)
	}
	ring.PublishSequence(one)

	if available, err := barrier.WaitFor(context.Background(), zero); err != nil || available != InitialSequence {
		t.Fatalf("gap wait: available=%d err=%v", available, err)
	}
	ring.PublishSequence(zero)
	available, err := barrier.WaitFor(context.Background(), zero)
	if err != nil || available != one {
		t.Fatalf("closed gap: available=%d err=%v", available, err)
	}
}

func TestBatchProcessorOrdersAndBatches(t *testing.T) {
	ring := newTestRing(t, 64, SingleProducer, BlockingWait())
	var mu sync.Mutex
	got := make([]int64, 0, 10)
	ends := make([]int64, 0, 4)
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(event *testEvent, sequence int64, end bool) error {
		mu.Lock()
		got = append(got, event.Value)
		if end {
			ends = append(ends, sequence)
		}
		mu.Unlock()
		return nil
	}, WithMaxBatchSize(3))
	if err != nil {
		t.Fatal(err)
	}
	ring.AddGatingSequences(processor.Sequence())
	for value := int64(0); value < 10; value++ {
		sequence, err := ring.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ring.Get(sequence).Value = value
		ring.PublishSequence(sequence)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- processor.Run(context.Background()) }()
	waitForSequence(t, processor.Sequence(), 9)
	processor.Halt()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 10 {
		t.Fatalf("processed %d events", len(got))
	}
	for i, value := range got {
		if value != int64(i) {
			t.Fatalf("event %d: got %d", i, value)
		}
	}
	if len(ends) != 4 || ends[0] != 2 || ends[3] != 9 {
		t.Fatalf("batch ends: %v", ends)
	}
}

func TestPipelineDependencyVisibility(t *testing.T) {
	ring := newTestRing(t, 64, SingleProducer, YieldingWait())
	first, _ := NewBatchProcessor(ring, ring.NewBarrier(), func(event *testEvent, _ int64, _ bool) error {
		event.Check = event.Value * 2
		return nil
	})
	second, _ := NewBatchProcessor(ring, ring.NewBarrier(first.Sequence()), func(event *testEvent, _ int64, _ bool) error {
		if event.Check != event.Value*2 {
			return errors.New("upstream write not visible")
		}
		return nil
	})
	ring.AddGatingSequences(second.Sequence())
	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() { firstErr <- first.Run(context.Background()) }()
	go func() { secondErr <- second.Run(context.Background()) }()

	for value := int64(0); value < 1000; value++ {
		current := value
		if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
			event.Value = current
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	waitForSequence(t, second.Sequence(), 999)
	first.Halt()
	second.Halt()
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
	if err := <-secondErr; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentMultiProducerDelivery(t *testing.T) {
	const producers = 4
	const perProducer = 2000
	const total = producers * perProducer
	ring := newTestRing(t, 1024, MultiProducer, YieldingWait())
	var handled atomic.Int64
	var invalid atomic.Int64
	processor, _ := NewBatchProcessor(ring, ring.NewBarrier(), func(event *testEvent, sequence int64, _ bool) error {
		if event.Check != sequence {
			invalid.Add(1)
		}
		handled.Add(1)
		return nil
	})
	ring.AddGatingSequences(processor.Sequence())
	processorErr := make(chan error, 1)
	go func() { processorErr <- processor.Run(context.Background()) }()

	var publishers sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		publisher := producer
		publishers.Go(func() {
			for i := 0; i < perProducer; i++ {
				current := i
				if err := ring.Publish(context.Background(), func(event *testEvent, sequence int64) error {
					event.Value = int64(publisher*perProducer + current)
					event.Check = sequence
					return nil
				}); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		})
	}
	publishers.Wait()
	waitForSequence(t, processor.Sequence(), total-1)
	processor.Halt()
	if err := <-processorErr; err != nil {
		t.Fatal(err)
	}
	if got := handled.Load(); got != total {
		t.Fatalf("handled %d, want %d", got, total)
	}
	if got := invalid.Load(); got != 0 {
		t.Fatalf("invalid event identities: %d", got)
	}
}

func TestBlockedClaimHonorsContextAndClose(t *testing.T) {
	ring := newTestRing(t, 1, SingleProducer, YieldingWait())
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	sequence, _ := ring.Next(context.Background())
	ring.PublishSequence(sequence)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := ring.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked claim: got %v", err)
	}
	ring.Close()
	if _, err := ring.TryNext(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed ring: got %v", err)
	}
}

func waitForSequence(t *testing.T, sequence *Sequence, expected int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for sequence.Load() < expected {
		if time.Now().After(deadline) {
			t.Fatalf("timed out at sequence %d, want %d", sequence.Load(), expected)
		}
		time.Sleep(time.Microsecond)
	}
}
