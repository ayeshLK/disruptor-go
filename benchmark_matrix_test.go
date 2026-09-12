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
	"fmt"
	"runtime"
	"sync"
	"testing"
)

const benchmarkRingSize int64 = 65_536

func BenchmarkClaimPublishMatrix(b *testing.B) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		for _, batch := range []int64{1, 16, 256} {
			name := fmt.Sprintf("%s/batch-%d", producerName(producer), batch)
			b.Run(name, func(b *testing.B) {
				ring, err := New(benchmarkRingSize, producer, func() *benchmarkEvent { return new(benchmarkEvent) }, BusySpinWait())
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for published := 0; published < b.N; {
					count := min(batch, int64(b.N-published))
					high, err := ring.NextN(ctx, count)
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

func BenchmarkTopologyMatrix(b *testing.B) {
	for _, scenario := range []struct {
		name      string
		producers int
		topology  string
	}{
		{name: "SPSC", producers: 1, topology: "broadcast-1"},
		{name: "MPSC-2", producers: 2, topology: "broadcast-1"},
		{name: "MPSC-4", producers: 4, topology: "broadcast-1"},
		{name: "broadcast-2", producers: 1, topology: "broadcast-2"},
		{name: "pipeline-2", producers: 1, topology: "pipeline-2"},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkTopology(b, scenario.producers, scenario.topology, YieldingWait())
		})
	}
}

func BenchmarkConsumerWaitMatrix(b *testing.B) {
	for _, strategy := range []struct {
		name string
		new  func() WaitStrategy
	}{
		{name: "blocking", new: BlockingWait},
		{name: "sleeping", new: SleepingWait},
		{name: "yielding", new: YieldingWait},
		{name: "busy-spin", new: BusySpinWait},
	} {
		b.Run(strategy.name, func(b *testing.B) {
			benchmarkTopology(b, 1, "broadcast-1", strategy.new())
		})
	}
}

func benchmarkTopology(b *testing.B, producers int, topology string, wait WaitStrategy) {
	b.Helper()
	producerType := SingleProducer
	if producers > 1 {
		producerType = MultiProducer
	}
	ring, err := New(benchmarkRingSize, producerType, func() *benchmarkEvent { return new(benchmarkEvent) }, wait)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	processors, terminal, done := benchmarkProcessors(b, ring, topology, ctx)

	b.ReportAllocs()
	b.ResetTimer()
	var publishers sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		producerID := producer
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for value := producerID; value < b.N; value += producers {
				sequence, nextErr := ring.Next(ctx)
				if nextErr != nil {
					return
				}
				ring.Get(sequence).Value = int64(value)
				ring.PublishSequence(sequence)
			}
		}()
	}
	publishers.Wait()
	for _, sequence := range terminal {
		for sequence.Load() < int64(b.N-1) {
			runtime.Gosched()
		}
	}
	b.StopTimer()
	for _, processor := range processors {
		processor.Halt()
	}
	for _, result := range done {
		if err := <-result; err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkProcessors(b *testing.B, ring *RingBuffer[*benchmarkEvent], topology string, ctx context.Context) ([]*BatchProcessor[*benchmarkEvent], []*Sequence, []chan error) {
	b.Helper()
	count := 1
	pipeline := false
	switch topology {
	case "broadcast-1":
	case "broadcast-2":
		count = 2
	case "pipeline-2":
		count = 2
		pipeline = true
	default:
		b.Fatalf("unknown benchmark topology %q", topology)
	}
	processors := make([]*BatchProcessor[*benchmarkEvent], count)
	done := make([]chan error, count)
	var dependency *Sequence
	for i := range processors {
		barrier := ring.NewBarrier()
		if pipeline && dependency != nil {
			barrier = ring.NewBarrier(dependency)
		}
		processor, err := NewBatchProcessor(ring, barrier, func(event *benchmarkEvent, _ int64, _ bool) error {
			runtime.KeepAlive(event.Value)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		processors[i] = processor
		dependency = processor.Sequence()
		done[i] = make(chan error, 1)
		go func() { done[i] <- processor.Run(ctx) }()
	}
	if pipeline {
		ring.AddGatingSequences(processors[len(processors)-1].Sequence())
		return processors, []*Sequence{processors[len(processors)-1].Sequence()}, done
	}
	gates := make([]*Sequence, len(processors))
	for i, processor := range processors {
		gates[i] = processor.Sequence()
	}
	ring.AddGatingSequences(gates...)
	return processors, gates, done
}

type inlinePayload16 struct{ Payload [16]byte }
type inlinePayload256 struct{ Payload [256]byte }
type inlinePayload4096 struct{ Payload [4096]byte }
type referencedPayload4096 struct{ Payload []byte }

func BenchmarkPayloadSensitivitySPSC(b *testing.B) {
	benchmarkPayloadCases(b, SingleProducer)
}

func BenchmarkPayloadSensitivityMPSC(b *testing.B) {
	benchmarkPayloadCases(b, MultiProducer)
}

func benchmarkPayloadCases(b *testing.B, producer ProducerType) {
	b.Helper()
	b.Run("inline-16B", func(b *testing.B) {
		benchmarkPayload(b, producer, 16, func() *inlinePayload16 { return new(inlinePayload16) },
			func(event *inlinePayload16) []byte { return event.Payload[:] })
	})
	b.Run("inline-256B", func(b *testing.B) {
		benchmarkPayload(b, producer, 256, func() *inlinePayload256 { return new(inlinePayload256) },
			func(event *inlinePayload256) []byte { return event.Payload[:] })
	})
	b.Run("inline-4KiB", func(b *testing.B) {
		benchmarkPayload(b, producer, 4096, func() *inlinePayload4096 { return new(inlinePayload4096) },
			func(event *inlinePayload4096) []byte { return event.Payload[:] })
	})
	b.Run("referenced-4KiB", func(b *testing.B) {
		benchmarkPayload(b, producer, 4096, func() *referencedPayload4096 {
			return &referencedPayload4096{Payload: make([]byte, 4096)}
		}, func(event *referencedPayload4096) []byte { return event.Payload })
	})
}

func benchmarkPayload[T any](b *testing.B, producer ProducerType, payloadSize int, factory func() T, payload func(T) []byte) {
	b.Helper()
	const ringSize int64 = 1024
	ring, err := New(ringSize, producer, factory, YieldingWait())
	if err != nil {
		b.Fatal(err)
	}
	var checksum uint64
	processor, err := NewBatchProcessor(ring, ring.NewBarrier(), func(event T, _ int64, _ bool) error {
		var sum uint64
		for _, value := range payload(event) {
			sum += uint64(value)
		}
		checksum += sum
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	ring.AddGatingSequences(processor.Sequence())
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- processor.Run(ctx) }()

	b.ReportAllocs()
	b.ResetTimer()
	producerCount := 1
	if producer == MultiProducer {
		producerCount = 4
	}
	errorsCh := make(chan error, producerCount)
	var publishers sync.WaitGroup
	for producerID := 0; producerID < producerCount; producerID++ {
		producerID := producerID
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for i := producerID; i < b.N; i += producerCount {
				sequence, err := ring.Next(ctx)
				if err != nil {
					errorsCh <- err
					return
				}
				bytes := payload(ring.Get(sequence))
				for index := range bytes {
					bytes[index] = byte(sequence + int64(index))
				}
				ring.PublishSequence(sequence)
			}
		}()
	}
	publishers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		b.Fatal(err)
	}
	for processor.Sequence().Load() < int64(b.N-1) {
		runtime.Gosched()
	}
	b.StopTimer()
	processor.Halt()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	runtime.KeepAlive(checksum)
	b.ReportMetric(float64(b.N*payloadSize)/(1024*1024)/b.Elapsed().Seconds(), "payload-MiB/s")
}

func BenchmarkBufferedChannelMPSC(b *testing.B) {
	channel := make(chan int64, benchmarkRingSize)
	done := make(chan struct{})
	go func() {
		for value := range channel {
			runtime.KeepAlive(value)
		}
		close(done)
	}()
	b.ReportAllocs()
	b.ResetTimer()
	var producers sync.WaitGroup
	for producer := 0; producer < 4; producer++ {
		producerID := producer
		producers.Add(1)
		go func() {
			defer producers.Done()
			for value := producerID; value < b.N; value += 4 {
				channel <- int64(value)
			}
		}()
	}
	producers.Wait()
	close(channel)
	<-done
}
