package disruptor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitStrategiesHonorCancelledContext(t *testing.T) {
	strategies := map[string]func() WaitStrategy{
		"busy-spin": BusySpinWait,
		"yielding":  YieldingWait,
		"sleeping": func() WaitStrategy {
			return SleepingWaitStrategy{Sleep: time.Hour}
		},
		"blocking": BlockingWait,
	}
	for name, factory := range strategies {
		t.Run(name, func(t *testing.T) {
			ring := newTestRing(t, 8, SingleProducer, factory())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := ring.NewBarrier().WaitFor(ctx, 0); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled wait: got %v", err)
			}
		})
	}
}
