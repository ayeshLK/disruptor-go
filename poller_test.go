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
	"reflect"
	"sync/atomic"
	"testing"
)

func TestEventPollerReportsIdleAndProcessing(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, YieldingWait())
	processed := make([]int64, 0, 2)
	poller, err := NewEventPoller(ring, ring.NewBarrier(), func(_ *testEvent, sequence int64, endOfBatch bool) error {
		processed = append(processed, sequence)
		if !endOfBatch {
			t.Errorf("sequence %d was not marked as the batch end", sequence)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	state, err := poller.Poll()
	if err != nil || state != PollIdle {
		t.Fatalf("idle poll: state=%d err=%v", state, err)
	}
	if err := ring.Publish(context.Background(), func(event *testEvent, sequence int64) error {
		event.Value = sequence
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	state, err = poller.Poll()
	if err != nil || state != PollProcessing {
		t.Fatalf("processing poll: state=%d err=%v", state, err)
	}
	state, err = poller.Poll()
	if err != nil || state != PollIdle {
		t.Fatalf("idle after processing: state=%d err=%v", state, err)
	}
	if !reflect.DeepEqual(processed, []int64{0}) || poller.Sequence().Load() != 0 {
		t.Fatalf("processed=%v sequence=%d", processed, poller.Sequence().Load())
	}
}

func TestEventPollerReportsPublicationAndDependencyGating(t *testing.T) {
	t.Run("publication gap", func(t *testing.T) {
		ring := newTestRing(t, 8, MultiProducer, YieldingWait())
		poller, err := NewEventPoller(ring, ring.NewBarrier(), func(*testEvent, int64, bool) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		first, err := ring.TryNext()
		if err != nil {
			t.Fatal(err)
		}
		second, err := ring.TryNext()
		if err != nil {
			t.Fatal(err)
		}
		ring.PublishSequence(second)
		state, err := poller.Poll()
		if err != nil || state != PollGating {
			t.Fatalf("gap poll: state=%d err=%v", state, err)
		}
		ring.PublishSequence(first)
		state, err = poller.Poll()
		if err != nil || state != PollProcessing {
			t.Fatalf("gap resolution: state=%d err=%v", state, err)
		}
	})

	t.Run("upstream dependency", func(t *testing.T) {
		ring := newTestRing(t, 8, SingleProducer, YieldingWait())
		upstream := NewSequence(InitialSequence)
		poller, err := NewEventPoller(ring, ring.NewBarrier(upstream), func(*testEvent, int64, bool) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		state, err := poller.Poll()
		if err != nil || state != PollGating {
			t.Fatalf("dependency poll: state=%d err=%v", state, err)
		}
		upstream.Store(0)
		state, err = poller.Poll()
		if err != nil || state != PollProcessing {
			t.Fatalf("dependency resolution: state=%d err=%v", state, err)
		}
	})
}

func TestEventPollerHonorsBatchLimitAndReplaysFailures(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, YieldingWait())
	for i := int64(0); i < 3; i++ {
		value := i
		if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
			event.Value = value
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	fail := atomic.Bool{}
	poller, err := NewEventPoller(ring, ring.NewBarrier(), func(_ *testEvent, sequence int64, endOfBatch bool) error {
		if sequence == 0 && !fail.Load() {
			return errors.New("poll failed")
		}
		if sequence == 1 && !endOfBatch {
			t.Errorf("sequence 1 should end the limited batch")
		}
		return nil
	}, WithMaxBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}

	state, err := poller.Poll()
	var handlerErr *HandlerError
	if state != PollProcessing || !errors.As(err, &handlerErr) || poller.Sequence().Load() != InitialSequence {
		t.Fatalf("failed batch: state=%d err=%v sequence=%d", state, err, poller.Sequence().Load())
	}
	fail.Store(true)
	state, err = poller.Poll()
	if err != nil || state != PollProcessing || poller.Sequence().Load() != 1 {
		t.Fatalf("replayed batch: state=%d err=%v sequence=%d", state, err, poller.Sequence().Load())
	}
	state, err = poller.Poll()
	if err != nil || state != PollProcessing || poller.Sequence().Load() != 2 {
		t.Fatalf("remaining batch: state=%d err=%v sequence=%d", state, err, poller.Sequence().Load())
	}
}

func TestEventPollerRecoversPanicsAndSupportsGating(t *testing.T) {
	ring := newTestRing(t, 4, SingleProducer, YieldingWait())
	poller, err := NewEventPoller(ring, ring.NewBarrier(), func(_ *testEvent, sequence int64, _ bool) error {
		if sequence == 0 {
			panic("poll panic")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ring.AddGatingSequences(poller.Sequence())
	for range 4 {
		if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	state, err := poller.Poll()
	var panicErr *HandlerPanicError
	if state != PollProcessing || !errors.As(err, &panicErr) || poller.Sequence().Load() != InitialSequence {
		t.Fatalf("panic batch: state=%d err=%v sequence=%d", state, err, poller.Sequence().Load())
	}
	if _, err := ring.TryNext(); !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("gate after failed poll: %v", err)
	}
}

func TestEventPollerWraparoundAndClose(t *testing.T) {
	ring := newTestRing(t, 2, SingleProducer, YieldingWait())
	values := make([]int64, 0, 4)
	poller, err := NewEventPoller(ring, ring.NewBarrier(), func(event *testEvent, _ int64, _ bool) error {
		values = append(values, event.Value)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ring.AddGatingSequences(poller.Sequence())
	for i := int64(0); i < 2; i++ {
		value := i
		if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
			event.Value = value
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if state, err := poller.Poll(); state != PollProcessing || err != nil {
		t.Fatalf("first batch: state=%d err=%v", state, err)
	}
	for i := int64(2); i < 4; i++ {
		value := i
		if err := ring.Publish(context.Background(), func(event *testEvent, _ int64) error {
			event.Value = value
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	ring.Close()
	if state, err := poller.Poll(); state != PollProcessing || err != nil {
		t.Fatalf("visible events after close: state=%d err=%v", state, err)
	}
	if state, err := poller.Poll(); state != PollIdle || !errors.Is(err, ErrClosed) {
		t.Fatalf("closed poll: state=%d err=%v", state, err)
	}
	if !reflect.DeepEqual(values, []int64{0, 1, 2, 3}) {
		t.Fatalf("values: %v", values)
	}
}

func TestEventPollerAlert(t *testing.T) {
	ring := newTestRing(t, 4, SingleProducer, YieldingWait())
	barrier := ring.NewBarrier()
	poller, err := NewEventPoller(ring, barrier, func(*testEvent, int64, bool) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	barrier.Alert()
	if state, err := poller.Poll(); state != PollIdle || !errors.Is(err, ErrAlerted) {
		t.Fatalf("alerted poll: state=%d err=%v", state, err)
	}
	barrier.ClearAlert()
	if err := ring.Publish(context.Background(), func(*testEvent, int64) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if state, err := poller.Poll(); state != PollProcessing || err != nil {
		t.Fatalf("poll after clear: state=%d err=%v", state, err)
	}
}
