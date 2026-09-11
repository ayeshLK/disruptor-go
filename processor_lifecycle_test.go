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
	waitForProcessorRunning(t, processor)
	if err := processor.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run: got %v", err)
	}
	processor.Halt()
	if err := receiveProcessorResult(t, done); err != nil {
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
	done := make(chan error, 1)
	go func() { done <- processor.Run(ctx) }()
	waitForProcessorRunning(t, processor)
	cancel()
	if err := receiveProcessorResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Run: got %v", err)
	}
	done = make(chan error, 1)
	go func() { done <- processor.Run(context.Background()) }()
	waitForProcessorRunning(t, processor)
	processor.Halt()
	if err := receiveProcessorResult(t, done); err != nil {
		t.Fatalf("restarted Run: got %v", err)
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
	runErr := processor.Run(context.Background())
	if !errors.Is(runErr, handlerErr) {
		t.Fatalf("handler failure: got %v", runErr)
	}
	var failure *HandlerError
	if !errors.As(runErr, &failure) {
		t.Fatalf("handler failure type: got %T", runErr)
	}
	if failure.Sequence != 1 || failure.Err != handlerErr {
		t.Fatalf("handler failure details: got sequence=%d err=%v", failure.Sequence, failure.Err)
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

func TestBatchProcessorPanicLeavesBatchReplayable(t *testing.T) {
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

	panicErr := errors.New("handler panic")
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(_ *testEvent, sequence int64, _ bool) error {
		if sequence == 1 {
			panic(panicErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	runErr := processor.Run(context.Background())
	if !errors.Is(runErr, panicErr) {
		t.Fatalf("handler panic: got %v", runErr)
	}
	var failure *HandlerPanicError
	if !errors.As(runErr, &failure) {
		t.Fatalf("handler panic type: got %T", runErr)
	}
	if failure.Sequence != 1 || failure.Value != panicErr {
		t.Fatalf("handler panic details: got sequence=%d value=%v", failure.Sequence, failure.Value)
	}
	if got := processor.Sequence().Load(); got != InitialSequence {
		t.Fatalf("panicked batch acknowledged at %d", got)
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

func TestBatchProcessorIdleHaltDoesNotPreventRun(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BlockingWait())
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	processor.Halt()
	done := make(chan error, 1)
	go func() { done <- processor.Run(context.Background()) }()
	waitForProcessorRunning(t, processor)
	processor.Halt()
	if err := receiveProcessorResult(t, done); err != nil {
		t.Fatalf("Run after idle Halt: got %v", err)
	}
}

func TestBatchProcessorHaltDuringHandlerFinishesBatch(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BlockingWait())
	if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
		event.Value = 42
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error {
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- processor.Run(context.Background()) }()
	<-started
	processor.Halt()
	if !processor.Running() {
		t.Fatal("processor stopped owning lifecycle before handler returned")
	}
	close(release)
	if err := receiveProcessorResult(t, done); err != nil {
		t.Fatalf("halted Run: got %v", err)
	}
	if got := processor.Sequence().Load(); got != 0 {
		t.Fatalf("completed batch acknowledged at %d, want 0", got)
	}
}

func TestBatchProcessorExternalAlertCanRestart(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BlockingWait())
	barrier := ring.NewBarrier()
	processor, err := NewBatchProcessor(ring, barrier, func(*testEvent, int64, bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- processor.Run(context.Background()) }()
	waitForProcessorRunning(t, processor)
	barrier.Alert()
	if err := receiveProcessorResult(t, done); !errors.Is(err, ErrAlerted) {
		t.Fatalf("alerted Run: got %v", err)
	}

	go func() { done <- processor.Run(context.Background()) }()
	waitForProcessorRunning(t, processor)
	processor.Halt()
	if err := receiveProcessorResult(t, done); err != nil {
		t.Fatalf("restarted Run: got %v", err)
	}
}
