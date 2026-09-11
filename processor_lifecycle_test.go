package disruptor

import (
	"context"
	"errors"
	"runtime"
	"testing"
)

func TestNewBatchProcessorValidatesConfiguration(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, YieldingWait())
	if _, err := NewBatchProcessor[*testEvent](ring, ring.NewBarrier(), nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("nil handler: got %v", err)
	}
	if _, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error { return nil }, WithMaxBatchSize(0)); !errors.Is(err, ErrInvalidBatchSize) {
		t.Fatalf("invalid batch size: got %v", err)
	}
}

func TestBatchProcessorRunningAndAlreadyRunning(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BlockingWait())
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- processor.Run(ctx) }()
	for !processor.Running() {
		runtime.Gosched()
	}
	if err := processor.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run: got %v", err)
	}
	processor.Halt()
	if err := <-done; err != nil {
		t.Fatalf("halted Run: got %v", err)
	}
	if processor.Running() {
		t.Fatal("processor still reports running")
	}
}

func TestBatchProcessorContextCancellation(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BlockingWait())
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := processor.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Run: got %v", err)
	}
}

func TestBatchProcessorFailureLeavesBatchReplayable(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, YieldingWait())
	for value := int64(0); value < 3; value++ {
		current := value
		if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
			event.Value = current
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	handlerErr := errors.New("handler failed")
	calls := 0
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error {
		calls++
		if calls == 2 {
			return handlerErr
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Run(context.Background()); !errors.Is(err, handlerErr) {
		t.Fatalf("handler failure: got %v", err)
	}
	if got := processor.Sequence().Load(); got != InitialSequence {
		t.Fatalf("failed batch acknowledged at %d", got)
	}
	seen := make([]int64, 0, 3)
	processor.handler = func(event *testEvent, _ int64, _ bool) error {
		seen = append(seen, event.Value)
		if len(seen) == 3 {
			processor.Halt()
		}
		return nil
	}
	if err := processor.Run(context.Background()); err != nil {
		t.Fatalf("replay Run: got %v", err)
	}
	if len(seen) != 3 || seen[0] != 0 || seen[2] != 2 {
		t.Fatalf("replayed events: %v", seen)
	}
}
