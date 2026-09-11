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
	"sync"
	"testing"
	"time"
)

type barrierWaitResult struct {
	available int64
	err       error
}

func closeWaitStrategies() map[string]func() WaitStrategy {
	return map[string]func() WaitStrategy{
		"busy-spin": BusySpinWait,
		"yielding":  YieldingWait,
		"sleeping": func() WaitStrategy {
			return SleepingWaitStrategy{Sleep: time.Hour}
		},
		"blocking": BlockingWait,
	}
}

func TestCloseUnblocksDirectBarrierWaits(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			for name, factory := range closeWaitStrategies() {
				t.Run(name, func(t *testing.T) {
					ring := newTestRing(t, 8, producer, factory())
					result := make(chan barrierWaitResult, 1)
					started := make(chan struct{})
					go func() {
						close(started)
						available, err := ring.NewBarrier().WaitFor(context.Background(), 0)
						result <- barrierWaitResult{available: available, err: err}
					}()
					<-started

					ring.Close()
					ring.Close()

					got := receiveBarrierWait(t, result)
					if !errors.Is(got.err, ErrClosed) {
						t.Fatalf("closed wait: available=%d err=%v", got.available, got.err)
					}
				})
			}
		})
	}
}

func TestCloseUnblocksDependentBarrierWaits(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 8, producer, BlockingWait())
			sequence, err := ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.PublishSequence(sequence)

			dependency := NewSequence(InitialSequence)
			result := make(chan barrierWaitResult, 1)
			go func() {
				available, err := ring.NewBarrier(dependency).WaitFor(context.Background(), sequence)
				result <- barrierWaitResult{available: available, err: err}
			}()

			ring.Close()
			got := receiveBarrierWait(t, result)
			if !errors.Is(got.err, ErrClosed) {
				t.Fatalf("closed dependent wait: available=%d err=%v", got.available, got.err)
			}
		})
	}
}

func TestPublishedEventsRemainVisibleAfterClose(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 8, producer, BlockingWait())
			sequence, err := ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.Get(sequence).Value = 42
			ring.PublishSequence(sequence)
			ring.Close()

			available, err := ring.NewBarrier().WaitFor(context.Background(), sequence)
			if err != nil || available != sequence {
				t.Fatalf("visible after close: available=%d err=%v", available, err)
			}
			if got := ring.Get(sequence).Value; got != 42 {
				t.Fatalf("event value after close: got %d, want 42", got)
			}
			if _, err := ring.NewBarrier().WaitFor(context.Background(), sequence+1); !errors.Is(err, ErrClosed) {
				t.Fatalf("wait beyond final cursor: got %v", err)
			}
		})
	}
}

func TestCloseTerminatesBatchProcessors(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			for name, factory := range closeWaitStrategies() {
				t.Run(name, func(t *testing.T) {
					ring := newTestRing(t, 8, producer, factory())
					processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error {
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}

					result := make(chan error, 1)
					go func() { result <- processor.Run(context.Background()) }()
					waitForProcessorRunning(t, processor)
					ring.Close()

					if err := receiveProcessorResult(t, result); !errors.Is(err, ErrClosed) {
						t.Fatalf("processor close: got %v", err)
					}
					if processor.Running() {
						t.Fatal("processor still reports running after close")
					}
					if err := processor.Run(context.Background()); !errors.Is(err, ErrClosed) {
						t.Fatalf("restarted processor after close: got %v", err)
					}
					if processor.Running() {
						t.Fatal("processor reports running after closed restart")
					}
				})
			}
		})
	}
}

func TestCloseOnSharedWaitStrategyIsRingScoped(t *testing.T) {
	wait := BlockingWait()
	first := newTestRing(t, 8, SingleProducer, wait)
	second := newTestRing(t, 8, SingleProducer, wait)
	firstResult := make(chan barrierWaitResult, 1)
	secondResult := make(chan barrierWaitResult, 1)

	go func() {
		available, err := first.NewBarrier().WaitFor(context.Background(), 0)
		firstResult <- barrierWaitResult{available: available, err: err}
	}()
	go func() {
		available, err := second.NewBarrier().WaitFor(context.Background(), 0)
		secondResult <- barrierWaitResult{available: available, err: err}
	}()

	first.Close()
	if got := receiveBarrierWait(t, firstResult); !errors.Is(got.err, ErrClosed) {
		t.Fatalf("first ring close: available=%d err=%v", got.available, got.err)
	}
	select {
	case got := <-secondResult:
		t.Fatalf("shared strategy closed second ring: available=%d err=%v", got.available, got.err)
	default:
	}

	sequence, err := second.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second.PublishSequence(sequence)
	if got := receiveBarrierWait(t, secondResult); got.err != nil || got.available != sequence {
		t.Fatalf("second ring publish: available=%d err=%v", got.available, got.err)
	}
}

func TestConcurrentCloseIsIdempotent(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, BlockingWait())
	var callers sync.WaitGroup
	for range 32 {
		callers.Go(ring.Close)
	}
	callers.Wait()

	if _, err := ring.TryNext(); !errors.Is(err, ErrClosed) {
		t.Fatalf("claim after close: got %v", err)
	}
	if _, err := ring.NewBarrier().WaitFor(context.Background(), 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("wait after close: got %v", err)
	}
}

func receiveBarrierWait(t *testing.T, result <-chan barrierWaitResult) barrierWaitResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for barrier")
		return barrierWaitResult{}
	}
}

func receiveProcessorResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for processor")
		return nil
	}
}

func waitForProcessorRunning(t *testing.T, processor *BatchProcessor[*testEvent]) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !processor.Running() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for processor to start")
		}
		runtime.Gosched()
	}
}
