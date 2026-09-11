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
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownDrainsFinalClaimedSequence(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 8, producer, BlockingWait())
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error {
				once.Do(func() { close(started) })
				<-release
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ring.AddGatingSequences(processor.Sequence())

			processorResult := make(chan error, 1)
			go func() { processorResult <- processor.Run(context.Background()) }()
			for value := int64(0); value < 2; value++ {
				current := value
				if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
					event.Value = current
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			<-started

			shutdownResult := make(chan error, 1)
			go func() { shutdownResult <- ring.Shutdown(context.Background()) }()
			waitForRingSealed(t, ring)
			if _, err := ring.TryNext(); !errors.Is(err, ErrClosed) {
				t.Fatalf("claim during shutdown: got %v", err)
			}
			select {
			case err := <-shutdownResult:
				t.Fatalf("shutdown completed before consumer drain: %v", err)
			default:
			}

			close(release)
			if err := receiveProcessorResult(t, shutdownResult); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			if got := processor.Sequence().Load(); got != 1 {
				t.Fatalf("consumer sequence: got %d, want 1", got)
			}
			if err := receiveProcessorResult(t, processorResult); !errors.Is(err, ErrClosed) {
				t.Fatalf("processor termination: got %v", err)
			}
		})
	}
}

func TestShutdownWaitsForEveryBroadcastGate(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BlockingWait())
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	processors := make([]*BatchProcessor[*testEvent], 2)
	results := make([]chan error, 2)
	for i := range processors {
		index := i
		processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error {
			select {
			case <-started[index]:
			default:
				close(started[index])
			}
			<-releases[index]
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		processors[i] = processor
		results[i] = make(chan error, 1)
		ring.AddGatingSequences(processor.Sequence())
		go func() { results[index] <- processor.Run(context.Background()) }()
	}
	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	<-started[0]
	<-started[1]

	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- ring.Shutdown(context.Background()) }()
	waitForRingSealed(t, ring)
	close(releases[0])
	waitForSequence(t, processors[0].Sequence(), 0)
	select {
	case err := <-shutdownResult:
		t.Fatalf("shutdown ignored slow broadcast gate: %v", err)
	default:
	}

	close(releases[1])
	if err := receiveProcessorResult(t, shutdownResult); err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		if err := receiveProcessorResult(t, result); !errors.Is(err, ErrClosed) {
			t.Fatalf("processor %d: got %v", i, err)
		}
	}
}

func TestShutdownDrainsTerminalPipelineGate(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, YieldingWait())
	downstreamStarted := make(chan struct{})
	releaseDownstream := make(chan struct{})
	upstream, err := NewBatchProcessor(ring, ring.NewBarrier(), func(event *testEvent, _ int64, _ bool) error {
		event.Check = event.Value * 2
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	downstream, err := NewBatchProcessor(ring, ring.NewBarrier(upstream.Sequence()), func(event *testEvent, _ int64, _ bool) error {
		if event.Check != event.Value*2 {
			return errors.New("upstream mutation not visible")
		}
		select {
		case <-downstreamStarted:
		default:
			close(downstreamStarted)
		}
		<-releaseDownstream
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

	if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
		event.Value = 21
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-downstreamStarted
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- ring.Shutdown(context.Background()) }()
	waitForRingSealed(t, ring)
	close(releaseDownstream)

	if err := receiveProcessorResult(t, shutdownResult); err != nil {
		t.Fatal(err)
	}
	if got := downstream.Sequence().Load(); got != 0 {
		t.Fatalf("terminal sequence: got %d, want 0", got)
	}
	for name, result := range map[string]<-chan error{
		"upstream":   upstreamResult,
		"downstream": downstreamResult,
	} {
		if err := receiveProcessorResult(t, result); !errors.Is(err, ErrClosed) {
			t.Fatalf("%s processor: got %v", name, err)
		}
	}
}

func TestShutdownCancellationClosesRing(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, SleepingWait())
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ring.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled shutdown: got %v", err)
	}
	if _, err := ring.TryNext(); !errors.Is(err, ErrClosed) {
		t.Fatalf("claim after cancelled shutdown: got %v", err)
	}
	if _, err := ring.NewBarrier(gate).WaitFor(context.Background(), 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("dependent wait after cancelled shutdown: got %v", err)
	}
	if err := ring.Shutdown(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("second shutdown: got %v", err)
	}
}

func TestShutdownReportsCloseAndHandlerFailure(t *testing.T) {
	t.Run("already closed", func(t *testing.T) {
		ring := newTestRing(t, 8, MultiProducer, BlockingWait())
		ring.Close()
		if err := ring.Shutdown(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("shutdown after close: got %v", err)
		}
	})

	t.Run("handler failure", func(t *testing.T) {
		ring := newTestRing(t, 8, SingleProducer, BlockingWait())
		handlerErr := errors.New("handler failed")
		processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error {
			return handlerErr
		})
		if err != nil {
			t.Fatal(err)
		}
		ring.AddGatingSequences(processor.Sequence())
		if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		processorResult := make(chan error, 1)
		go func() { processorResult <- processor.Run(context.Background()) }()
		if err := receiveProcessorResult(t, processorResult); !errors.Is(err, handlerErr) {
			t.Fatalf("processor failure: got %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := ring.Shutdown(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown after handler failure: got %v", err)
		}
	})
}

func TestShutdownWithoutGatesClosesImmediately(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, BusySpinWait())
	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := ring.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := ring.TryNext(); !errors.Is(err, ErrClosed) {
		t.Fatalf("claim after shutdown: got %v", err)
	}
}

func TestImmediateCloseAbortsShutdown(t *testing.T) {
	ring := newTestRing(t, 8, MultiProducer, BlockingWait())
	gate := NewSequence(InitialSequence)
	ring.AddGatingSequences(gate)
	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() { result <- ring.Shutdown(context.Background()) }()
	waitForRingSealed(t, ring)
	if err := ring.Shutdown(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("concurrent shutdown: got %v", err)
	}
	ring.Close()
	if err := receiveProcessorResult(t, result); !errors.Is(err, ErrClosed) {
		t.Fatalf("aborted shutdown: got %v", err)
	}
}

func waitForRingSealed(t *testing.T, ring *RingBuffer[*testEvent]) {
	t.Helper()
	var sealed *atomic.Bool
	switch sequencer := ring.sequencer.(type) {
	case *singleProducerSequencer:
		sealed = &sequencer.sealed
	case *multiProducerSequencer:
		sealed = &sequencer.sealed
	default:
		t.Fatalf("unknown sequencer %T", ring.sequencer)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !sealed.Load() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shutdown to seal claims")
		}
		runtime.Gosched()
	}
}
