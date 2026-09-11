package disruptor

import (
	"context"
	"errors"
	"testing"
)

func TestSequenceOperationsAndMinimums(t *testing.T) {
	first := NewSequence(4)
	second := NewSequence(7)
	if got := first.Add(2); got != 6 {
		t.Fatalf("add: got %d, want 6", got)
	}
	if !first.CompareAndSwap(6, 8) || first.CompareAndSwap(6, 9) {
		t.Fatal("compare-and-swap did not preserve atomic sequence semantics")
	}
	if got := (sequenceGroup{first, second}).Load(); got != 7 {
		t.Fatalf("group minimum: got %d, want 7", got)
	}
	if got := (sequenceGroup{}).Load(); got != InitialSequence {
		t.Fatalf("empty group: got %d, want %d", got, InitialSequence)
	}
	if got := minimumSequence(nil, 12); got != 12 {
		t.Fatalf("fallback minimum: got %d, want 12", got)
	}
}

func TestRingMetadataBatchClaimsAndGates(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 8, producer, nil)
			if ring.BufferSize() != 8 || ring.Cursor() != InitialSequence || ring.RemainingCapacity() != 8 {
				t.Fatalf("initial metadata: size=%d cursor=%d remaining=%d", ring.BufferSize(), ring.Cursor(), ring.RemainingCapacity())
			}
			gate := NewSequence(99)
			ring.AddGatingSequences(nil, gate)
			if got := gate.Load(); got != InitialSequence {
				t.Fatalf("new gate starts at %d, want %d", got, InitialSequence)
			}
			if !ring.RemoveGatingSequence(gate) || ring.RemoveGatingSequence(gate) {
				t.Fatal("gate removal result was incorrect")
			}
			capacityGate := NewSequence(InitialSequence)
			ring.AddGatingSequences(capacityGate)
			high, err := ring.NextN(context.Background(), 3)
			if err != nil || high != 2 {
				t.Fatalf("NextN: high=%d err=%v", high, err)
			}
			for sequence := int64(0); sequence <= high; sequence++ {
				ring.Get(sequence).Value = sequence
			}
			ring.PublishRange(0, high)
			if available, err := ring.NewBarrier().WaitFor(context.Background(), 0); err != nil || available != high {
				t.Fatalf("published range: available=%d err=%v", available, err)
			}
			high, err = ring.TryNextN(2)
			if err != nil || high != 4 {
				t.Fatalf("TryNextN: high=%d err=%v", high, err)
			}
			ring.PublishRange(3, high)
			if remaining := ring.RemainingCapacity(); remaining != 3 {
				t.Fatalf("remaining capacity: got %d, want 3", remaining)
			}
			for _, count := range []int64{0, 9} {
				if _, err := ring.NextN(context.Background(), count); !errors.Is(err, ErrInvalidClaimSize) {
					t.Fatalf("NextN(%d): got %v", count, err)
				}
				if _, err := ring.TryNextN(count); !errors.Is(err, ErrInvalidClaimSize) {
					t.Fatalf("TryNextN(%d): got %v", count, err)
				}
			}
		})
	}
}

func TestPublishHelpersPublishTranslatorFailures(t *testing.T) {
	translatorErr := errors.New("translation failed")
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring := newTestRing(t, 4, producer, YieldingWait())
			if err := ring.Publish(context.Background(), nil); !errors.Is(err, ErrNilTranslator) {
				t.Fatalf("nil Publish translator: got %v", err)
			}
			if err := ring.TryPublish(nil); !errors.Is(err, ErrNilTranslator) {
				t.Fatalf("nil TryPublish translator: got %v", err)
			}
			if err := ring.Publish(context.Background(), func(event *testEvent, sequence int64) error {
				event.Value = sequence
				return translatorErr
			}); !errors.Is(err, translatorErr) {
				t.Fatalf("Publish translator error: got %v", err)
			}
			if ring.Cursor() != 0 {
				t.Fatalf("failed Publish was not visible: cursor=%d", ring.Cursor())
			}
			if err := ring.TryPublish(func(event *testEvent, sequence int64) error {
				event.Value = sequence
				return translatorErr
			}); !errors.Is(err, translatorErr) {
				t.Fatalf("TryPublish translator error: got %v", err)
			}
			if available, err := ring.NewBarrier().WaitFor(context.Background(), 1); err != nil || available != 1 {
				t.Fatalf("failed TryPublish was not visible: available=%d err=%v", available, err)
			}
		})
	}
}

func TestBarrierStateAndDependencyCursor(t *testing.T) {
	ring := newTestRing(t, 8, SingleProducer, YieldingWait())
	first := NewSequence(3)
	second := NewSequence(5)
	barrier := ring.NewBarrier(first, second)
	if got := barrier.Cursor(); got != 3 {
		t.Fatalf("dependency cursor: got %d, want 3", got)
	}
	barrier.Alert()
	if !barrier.IsAlerted() {
		t.Fatal("barrier did not report alert")
	}
	barrier.ClearAlert()
	if barrier.IsAlerted() {
		t.Fatal("barrier remained alerted")
	}
}

func producerName(producer ProducerType) string {
	if producer == SingleProducer {
		return "single"
	}
	return "multi"
}
