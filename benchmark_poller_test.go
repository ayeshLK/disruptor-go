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
	"fmt"
	"testing"
)

func BenchmarkEventPoller(b *testing.B) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		for _, batch := range []int64{1, 16, 256} {
			name := fmt.Sprintf("%s/batch-%d", producerName(producer), batch)
			b.Run(name, func(b *testing.B) {
				ring, err := New(benchmarkRingSize, producer, func() *benchmarkEvent { return new(benchmarkEvent) }, BusySpinWait())
				if err != nil {
					b.Fatal(err)
				}
				poller, err := NewEventPoller(ring, ring.NewBarrier(), func(event *benchmarkEvent, sequence int64, _ bool) error {
					event.Value = sequence
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
				ring.AddGatingSequences(poller.Sequence())
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := ring.TryPublishN(batch, func(event *benchmarkEvent, sequence int64) error {
						event.Value = sequence
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					if state, err := poller.Poll(); err != nil || state != PollProcessing {
						b.Fatalf("poll: state=%d err=%v", state, err)
					}
				}
			})
		}
	}
}

func BenchmarkEventPollerIdle(b *testing.B) {
	ring, err := New(benchmarkRingSize, SingleProducer, func() *benchmarkEvent { return new(benchmarkEvent) }, BusySpinWait())
	if err != nil {
		b.Fatal(err)
	}
	poller, err := NewEventPoller(ring, ring.NewBarrier(), func(*benchmarkEvent, int64, bool) error { return nil })
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if state, err := poller.Poll(); err != nil || state != PollIdle {
			b.Fatalf("idle poll: state=%d err=%v", state, err)
		}
	}
}
