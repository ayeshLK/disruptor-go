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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	disruptor "github.com/ayeshLK/disruptor-go"
)

type loadEvent struct {
	Value       int64
	PublishedAt int64
	Sampled     bool
}

type collector struct {
	mu      sync.Mutex
	latency []int64
}

func (c *collector) record(value int64) {
	c.mu.Lock()
	c.latency = append(c.latency, value)
	c.mu.Unlock()
}

type report struct {
	Events              int64   `json:"events"`
	Producers           int     `json:"producers"`
	Consumers           int     `json:"consumers"`
	RingSize            int64   `json:"ring_size"`
	BatchSize           int64   `json:"batch_size"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	PublishedPerSecond  float64 `json:"published_per_second"`
	DeliveriesPerSecond float64 `json:"deliveries_per_second"`
	LatencySamples      int     `json:"latency_samples"`
	LatencyP50NS        int64   `json:"latency_p50_ns,omitempty"`
	LatencyP95NS        int64   `json:"latency_p95_ns,omitempty"`
	LatencyP99NS        int64   `json:"latency_p99_ns,omitempty"`
	GOMAXPROCS          int     `json:"gomaxprocs"`
}

func main() {
	events := flag.Int64("events", 1_000_000, "total events to publish")
	producers := flag.Int("producers", 1, "publisher goroutines")
	consumers := flag.Int("consumers", 1, "broadcast consumer goroutines")
	ringSize := flag.Int64("ring-size", 65_536, "power-of-two ring size")
	batchSize := flag.Int64("batch-size", 256, "maximum consumer batch size")
	sampleEvery := flag.Int64("sample-every", 1024, "sample one latency per N publications; zero disables")
	timeout := flag.Duration("timeout", 30*time.Second, "overall timeout")
	flag.Parse()
	if *events < 1 || *producers < 1 || *consumers < 1 || *batchSize < 1 {
		fatal(fmt.Errorf("events, producers, consumers, and batch-size must be positive"))
	}

	producerType := disruptor.SingleProducer
	if *producers > 1 {
		producerType = disruptor.MultiProducer
	}
	ring, err := disruptor.New(*ringSize, producerType,
		func() *loadEvent { return new(loadEvent) }, disruptor.YieldingWait())
	if err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	latencies := new(collector)
	processors := make([]*disruptor.BatchProcessor[*loadEvent], *consumers)
	done := make([]chan error, *consumers)
	for i := range processors {
		processor, err := disruptor.NewBatchProcessor(ring, ring.NewBarrier(),
			func(event *loadEvent, _ int64, _ bool) error {
				if event.Sampled {
					latencies.record(time.Now().UnixNano() - event.PublishedAt)
				}
				return nil
			}, disruptor.WithMaxBatchSize(*batchSize))
		if err != nil {
			fatal(err)
		}
		processors[i] = processor
		ring.AddGatingSequences(processor.Sequence())
		done[i] = make(chan error, 1)
		go func(index int) { done[index] <- processors[index].Run(ctx) }(i)
	}

	started := time.Now()
	var publishers sync.WaitGroup
	publishErrors := make(chan error, *producers)
	for producer := 0; producer < *producers; producer++ {
		producerID := producer
		publishers.Go(func() {
			for value := int64(producerID); value < *events; value += int64(*producers) {
				sequence, err := ring.Next(ctx)
				if err != nil {
					publishErrors <- err
					return
				}
				event := ring.Get(sequence)
				event.Value = value
				event.Sampled = *sampleEvery > 0 && sequence%*sampleEvery == 0
				if event.Sampled {
					event.PublishedAt = time.Now().UnixNano()
				}
				ring.PublishSequence(sequence)
			}
		})
	}
	publishers.Wait()
	close(publishErrors)
	for err := range publishErrors {
		fatal(err)
	}
	for _, processor := range processors {
		for processor.Sequence().Load() < *events-1 {
			select {
			case <-ctx.Done():
				fatal(ctx.Err())
			default:
				runtime.Gosched()
			}
		}
	}
	elapsed := time.Since(started)
	for _, processor := range processors {
		processor.Halt()
	}
	for _, result := range done {
		if err := <-result; err != nil {
			fatal(err)
		}
	}

	latencies.mu.Lock()
	samples := append([]int64(nil), latencies.latency...)
	latencies.mu.Unlock()
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	result := report{
		Events: *events, Producers: *producers, Consumers: *consumers,
		RingSize: *ringSize, BatchSize: *batchSize, ElapsedSeconds: elapsed.Seconds(),
		PublishedPerSecond:  float64(*events) / elapsed.Seconds(),
		DeliveriesPerSecond: float64(*events*int64(*consumers)) / elapsed.Seconds(),
		LatencySamples:      len(samples), GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	if len(samples) > 0 {
		result.LatencyP50NS = percentile(samples, 0.50)
		result.LatencyP95NS = percentile(samples, 0.95)
		result.LatencyP99NS = percentile(samples, 0.99)
	}
	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(output))
}

func percentile(values []int64, quantile float64) int64 {
	index := int(float64(len(values)-1) * quantile)
	return values[index]
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
