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

func BenchmarkTryClaimPublishMatrix(b *testing.B) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		for _, batch := range []int64{1, 16, 256} {
			name := fmt.Sprintf("%s/batch-%d", producerName(producer), batch)
			b.Run(name, func(b *testing.B) {
				ring, err := New(benchmarkRingSize, producer, func() *benchmarkEvent { return new(benchmarkEvent) }, BusySpinWait())
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for published := 0; published < b.N; {
					count := min(batch, int64(b.N-published))
					high, err := ring.TryNextN(count)
					if err != nil {
						b.Fatal(err)
					}
					low := high - count + 1
					for sequence := low; sequence <= high; sequence++ {
						ring.Get(sequence).Value = sequence
					}
					ring.PublishRange(low, high)
					published += int(count)
				}
			})
		}
	}
}

func BenchmarkMultiProducerPublicationGapScan(b *testing.B) {
	ring, err := New(benchmarkRingSize, MultiProducer, func() *benchmarkEvent { return new(benchmarkEvent) }, BusySpinWait())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		high, err := ring.TryNextN(2)
		if err != nil {
			b.Fatal(err)
		}
		low := high - 1
		ring.PublishSequence(high)
		if available := ring.sequencer.HighestPublished(low, high); available != low-1 {
			b.Fatalf("gap scan reached %d, want %d", available, low-1)
		}
		ring.PublishSequence(low)
		if available := ring.sequencer.HighestPublished(low, high); available != high {
			b.Fatalf("completed scan reached %d, want %d", available, high)
		}
	}
}
