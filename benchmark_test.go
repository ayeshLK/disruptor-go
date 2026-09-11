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
	"sync/atomic"
	"testing"
)

type benchmarkEvent struct{ Value int64 }

func BenchmarkRawPublish(b *testing.B) {
	ring, _ := New(65536, SingleProducer, func() *benchmarkEvent { return new(benchmarkEvent) }, BusySpinWait())
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sequence, _ := ring.Next(ctx)
		ring.Get(sequence).Value = int64(i)
		ring.PublishSequence(sequence)
	}
}

func BenchmarkSPSC(b *testing.B) {
	ring, _ := New(65536, SingleProducer, func() *benchmarkEvent { return new(benchmarkEvent) }, YieldingWait())
	processor, _ := NewBatchProcessor(ring, ring.NewBarrier(), func(_ *benchmarkEvent, _ int64, _ bool) error { return nil })
	ring.AddGatingSequences(processor.Sequence())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- processor.Run(ctx) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sequence, _ := ring.Next(ctx)
		ring.Get(sequence).Value = int64(i)
		ring.PublishSequence(sequence)
	}
	for processor.Sequence().Load() < int64(b.N-1) {
		runtime.Gosched()
	}
	b.StopTimer()
	processor.Halt()
	cancel()
	<-done
}

func BenchmarkBufferedChannelSPSC(b *testing.B) {
	channel := make(chan int64, 65536)
	var consumed atomic.Int64
	done := make(chan struct{})
	go func() {
		for value := range channel {
			consumed.Store(value)
		}
		close(done)
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		channel <- int64(i)
	}
	for consumed.Load() < int64(b.N-1) {
		runtime.Gosched()
	}
	b.StopTimer()
	close(channel)
	<-done
}
