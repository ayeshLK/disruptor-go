package disruptor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitStrategiesObservePublishAndAlert(t *testing.T) {
	strategies := map[string]func() WaitStrategy{
		"busy-spin": BusySpinWait,
		"yielding":  YieldingWait,
		"sleeping":  SleepingWait,
		"blocking":  BlockingWait,
	}
	for name, factory := range strategies {
		t.Run(name, func(t *testing.T) {
			ring := newTestRing(t, 8, SingleProducer, factory())
			barrier := ring.NewBarrier()
			result := make(chan error, 1)
			go func() {
				available, err := barrier.WaitFor(context.Background(), 0)
				if err == nil && available != 0 {
					err = errors.New("wrong available sequence")
				}
				result <- err
			}()
			time.Sleep(time.Millisecond)
			sequence, err := ring.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ring.PublishSequence(sequence)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			barrier.Alert()
			if _, err := barrier.WaitFor(context.Background(), 1); !errors.Is(err, ErrAlerted) {
				t.Fatalf("alert: got %v", err)
			}
		})
	}
}
