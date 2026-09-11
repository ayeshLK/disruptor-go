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
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// WaitStrategy controls how consumers wait for a sequence to become visible.
// Implementations must be safe for concurrent use by multiple barriers.
type WaitStrategy interface {
	waitFor(context.Context, int64, sequenceReader, sequenceReader, waitState) (int64, error)
	signalAll()
}

type waitState struct {
	alerted  *atomic.Bool
	closed   *atomic.Bool
	closedCh <-chan struct{}
}

// BusySpinWaitStrategy continuously polls a dependency and consumes a CPU core.
type BusySpinWaitStrategy struct{}

// BusySpinWait returns a strategy intended for consumers on dedicated cores.
func BusySpinWait() WaitStrategy { return BusySpinWaitStrategy{} }

func (BusySpinWaitStrategy) waitFor(ctx context.Context, desired int64, _ sequenceReader, dependent sequenceReader, state waitState) (int64, error) {
	for spins := 0; ; spins++ {
		if available := dependent.Load(); available >= desired {
			return available, nil
		}
		if spins&63 == 0 {
			if err := checkWait(ctx, state); err != nil {
				return 0, err
			}
		}
	}
}

func (BusySpinWaitStrategy) signalAll() {}

// YieldingWaitStrategy yields to the scheduler between publication checks.
type YieldingWaitStrategy struct{}

// YieldingWait returns a scheduler-yielding wait strategy.
func YieldingWait() WaitStrategy { return YieldingWaitStrategy{} }

func (YieldingWaitStrategy) waitFor(ctx context.Context, desired int64, _ sequenceReader, dependent sequenceReader, state waitState) (int64, error) {
	for {
		if available := dependent.Load(); available >= desired {
			return available, nil
		}
		if err := checkWait(ctx, state); err != nil {
			return 0, err
		}
		runtime.Gosched()
	}
}

func (YieldingWaitStrategy) signalAll() {}

// SleepingWaitStrategy spins, yields, and then sleeps while awaiting publication.
type SleepingWaitStrategy struct {
	// SpinTries is the number of polling iterations before yielding.
	SpinTries int
	// YieldTries is the number of scheduler yields before sleeping.
	YieldTries int
	// Sleep is the duration of each sleep after spinning and yielding.
	Sleep time.Duration
}

// SleepingWait returns a balanced wait strategy with default retry counts.
func SleepingWait() WaitStrategy {
	return SleepingWaitStrategy{SpinTries: 100, YieldTries: 100, Sleep: time.Microsecond}
}

func (s SleepingWaitStrategy) waitFor(ctx context.Context, desired int64, _ sequenceReader, dependent sequenceReader, state waitState) (int64, error) {
	tries := 0
	for {
		if available := dependent.Load(); available >= desired {
			return available, nil
		}
		if err := checkWait(ctx, state); err != nil {
			return 0, err
		}
		switch {
		case tries < s.SpinTries:
		case tries < s.SpinTries+s.YieldTries:
			runtime.Gosched()
		default:
			timer := time.NewTimer(s.Sleep)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return 0, ctx.Err()
			case <-state.closedCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return 0, ErrClosed
			case <-timer.C:
			}
		}
		tries++
	}
}

func (SleepingWaitStrategy) signalAll() {}

// BlockingWaitStrategy sleeps consumers until a producer publishes. After the
// producer cursor passes the desired sequence, dependency waiting yields.
type BlockingWaitStrategy struct {
	mu sync.Mutex
	ch chan struct{}
}

// BlockingWait returns a strategy that sleeps until producers signal publication.
func BlockingWait() WaitStrategy {
	return &BlockingWaitStrategy{ch: make(chan struct{})}
}

func (s *BlockingWaitStrategy) waitFor(ctx context.Context, desired int64, cursor sequenceReader, dependent sequenceReader, state waitState) (int64, error) {
	for cursor.Load() < desired {
		if err := checkWait(ctx, state); err != nil {
			return 0, err
		}
		s.mu.Lock()
		ch := s.ch
		s.mu.Unlock()
		if cursor.Load() >= desired {
			break
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-state.closedCh:
			return 0, ErrClosed
		}
	}
	for {
		if available := dependent.Load(); available >= desired {
			return available, nil
		}
		if err := checkWait(ctx, state); err != nil {
			return 0, err
		}
		runtime.Gosched()
	}
}

func (s *BlockingWaitStrategy) signalAll() {
	s.mu.Lock()
	close(s.ch)
	s.ch = make(chan struct{})
	s.mu.Unlock()
}

func checkWait(ctx context.Context, state waitState) error {
	if state.alerted.Load() {
		return ErrAlerted
	}
	if state.closed.Load() {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
