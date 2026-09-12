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
	"errors"
	"flag"
	"fmt"
	"math"
	"os/exec"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	disruptor "github.com/ayeshLK/lib-disruptor"
)

const reportSchemaVersion = 1

type config struct {
	mode         string
	topology     string
	events       int64
	warmupEvents int64
	repetitions  int
	producers    int
	consumers    int
	ringSize     int64
	batchSize    int64
	producerWait string
	consumerWait string
	payloadSize  int
	sampleEvery  int64
	handlerDelay time.Duration
	timeout      time.Duration
	environment  string
}

type resultSet struct {
	SchemaVersion int         `json:"schema_version"`
	Runs          []runReport `json:"runs"`
}

type runReport struct {
	SchemaVersion          int     `json:"schema_version"`
	Run                    int     `json:"run"`
	StartedAt              string  `json:"started_at"`
	Commit                 string  `json:"commit,omitempty"`
	Dirty                  bool    `json:"dirty,omitempty"`
	GoVersion              string  `json:"go_version"`
	GOOS                   string  `json:"goos"`
	GOARCH                 string  `json:"goarch"`
	LogicalCPUs            int     `json:"logical_cpus"`
	GOMAXPROCS             int     `json:"gomaxprocs"`
	Environment            string  `json:"environment,omitempty"`
	Mode                   string  `json:"mode"`
	Topology               string  `json:"topology"`
	Events                 int64   `json:"events"`
	WarmupEvents           int64   `json:"warmup_events"`
	Producers              int     `json:"producers"`
	Consumers              int     `json:"consumers"`
	RingSize               int64   `json:"ring_size"`
	BatchSize              int64   `json:"batch_size"`
	ProducerWait           string  `json:"producer_wait"`
	ConsumerWait           string  `json:"consumer_wait"`
	PayloadSizeBytes       int     `json:"payload_size_bytes"`
	PayloadWorkingSetBytes int64   `json:"payload_working_set_bytes"`
	SampleEvery            int64   `json:"sample_every,omitempty"`
	HandlerDelayNS         int64   `json:"handler_delay_ns,omitempty"`
	PublishSeconds         float64 `json:"publish_seconds"`
	DrainSeconds           float64 `json:"drain_seconds"`
	EndToEndSeconds        float64 `json:"end_to_end_seconds"`
	PublishedPerSecond     float64 `json:"published_per_second"`
	EndToEndPerSecond      float64 `json:"end_to_end_events_per_second"`
	DeliveriesPerSecond    float64 `json:"deliveries_per_second"`
	PayloadMiBPerSecond    float64 `json:"payload_mib_per_second,omitempty"`
	AllocationBytes        uint64  `json:"allocation_bytes"`
	Allocations            uint64  `json:"allocations"`
	GCCount                uint32  `json:"gc_count"`
	LatencySamples         int     `json:"latency_samples"`
	LatencyP50NS           int64   `json:"latency_p50_ns,omitempty"`
	LatencyP95NS           int64   `json:"latency_p95_ns,omitempty"`
	LatencyP99NS           int64   `json:"latency_p99_ns,omitempty"`
	LatencyP999NS          int64   `json:"latency_p999_ns,omitempty"`
	LatencyMaxNS           int64   `json:"latency_max_ns,omitempty"`
	Checksum               uint64  `json:"checksum"`
}

type processorSet struct {
	processors []*disruptor.BatchProcessor[*loadEvent]
	terminal   []*disruptor.Sequence
	done       []chan error
	latencies  [][]int64
	checksums  []uint64
}

func runMain() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		fatal(err)
	}
	output := resultSet{SchemaVersion: reportSchemaVersion, Runs: make([]runReport, 0, cfg.repetitions)}
	for run := 1; run <= cfg.repetitions; run++ {
		result, err := runOnce(cfg, run)
		if err != nil {
			fatal(fmt.Errorf("run %d: %w", run, err))
		}
		output.Runs = append(output.Runs, result)
	}
	encoded, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(encoded))
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", "throughput", "measurement mode: throughput or latency")
	flag.StringVar(&cfg.topology, "topology", "broadcast", "consumer topology: broadcast or pipeline")
	flag.Int64Var(&cfg.events, "events", 1_000_000, "measured events to publish per repetition")
	flag.Int64Var(&cfg.warmupEvents, "warmup-events", 100_000, "unmeasured warmup events per repetition")
	flag.IntVar(&cfg.repetitions, "repetitions", 1, "number of fresh-ring repetitions")
	flag.IntVar(&cfg.producers, "producers", 1, "publisher goroutines")
	flag.IntVar(&cfg.consumers, "consumers", 1, "broadcast consumers or pipeline stages")
	flag.Int64Var(&cfg.ringSize, "ring-size", 65_536, "power-of-two ring size")
	flag.Int64Var(&cfg.batchSize, "batch-size", 256, "maximum consumer batch size")
	flag.StringVar(&cfg.producerWait, "producer-wait", "yielding", "producer capacity wait: yielding, blocking, or busy-spin")
	flag.StringVar(&cfg.consumerWait, "consumer-wait", "yielding", "consumer wait: blocking, sleeping, yielding, or busy-spin")
	flag.IntVar(&cfg.payloadSize, "payload-size", 0, "reused payload bytes touched by each producer and consumer")
	flag.Int64Var(&cfg.sampleEvery, "sample-every", 100, "latency mode: sample one event per N publications")
	flag.DurationVar(&cfg.handlerDelay, "handler-delay", 0, "artificial delay applied by each handler")
	flag.DurationVar(&cfg.timeout, "timeout", 30*time.Second, "timeout per repetition, including warmup")
	flag.StringVar(&cfg.environment, "environment", "", "free-form runner or environment label")
	flag.Parse()
	return cfg
}

func validateConfig(cfg config) error {
	if cfg.mode != "throughput" && cfg.mode != "latency" {
		return fmt.Errorf("mode must be throughput or latency")
	}
	if cfg.topology != "broadcast" && cfg.topology != "pipeline" {
		return fmt.Errorf("topology must be broadcast or pipeline")
	}
	if cfg.events < 1 || cfg.warmupEvents < 0 || cfg.repetitions < 1 || cfg.producers < 1 || cfg.consumers < 1 || cfg.batchSize < 1 {
		return fmt.Errorf("events, repetitions, producers, consumers, and batch-size must be positive; warmup-events must be non-negative")
	}
	if cfg.topology == "pipeline" && cfg.consumers < 2 {
		return fmt.Errorf("pipeline topology requires at least two consumer stages")
	}
	if cfg.payloadSize < 0 {
		return fmt.Errorf("payload-size must be non-negative")
	}
	if cfg.mode == "latency" && cfg.sampleEvery < 1 {
		return fmt.Errorf("sample-every must be positive in latency mode")
	}
	if cfg.handlerDelay < 0 || cfg.timeout <= 0 {
		return fmt.Errorf("handler-delay must be non-negative and timeout must be positive")
	}
	if _, err := parseProducerWait(cfg.producerWait); err != nil {
		return err
	}
	if _, err := parseConsumerWait(cfg.consumerWait); err != nil {
		return err
	}
	return nil
}

func runOnce(cfg config, run int) (runReport, error) {
	producerType := disruptor.SingleProducer
	if cfg.producers > 1 {
		producerType = disruptor.MultiProducer
	}
	producerWait, err := parseProducerWait(cfg.producerWait)
	if err != nil {
		return runReport{}, err
	}
	consumerWait, err := parseConsumerWait(cfg.consumerWait)
	if err != nil {
		return runReport{}, err
	}
	ring, err := disruptor.New(cfg.ringSize, producerType, func() *loadEvent {
		return &loadEvent{Payload: make([]byte, cfg.payloadSize)}
	}, consumerWait, disruptor.WithProducerWait(producerWait))
	if err != nil {
		return runReport{}, err
	}
	defer ring.Close()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	processors, err := startProcessors(cfg, ring, ctx)
	if err != nil {
		cancel()
		return runReport{}, err
	}
	stopped := false
	defer func() {
		if !stopped {
			cancel()
			_ = stopProcessors(processors)
		}
	}()

	if cfg.warmupEvents > 0 {
		if err := publishPhase(ctx, cfg, ring, 0, cfg.warmupEvents, false); err != nil {
			return runReport{}, err
		}
		if err := waitForSequences(ctx, processors.terminal, cfg.warmupEvents-1); err != nil {
			return runReport{}, err
		}
	}
	clear(processors.checksums)

	var memoryBefore runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	startedAt := time.Now()
	if err := publishPhase(ctx, cfg, ring, cfg.warmupEvents, cfg.events, cfg.mode == "latency"); err != nil {
		return runReport{}, err
	}
	publishedAt := time.Now()
	target := cfg.warmupEvents + cfg.events - 1
	if err := waitForSequences(ctx, processors.terminal, target); err != nil {
		return runReport{}, err
	}
	completedAt := time.Now()
	var memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryAfter)

	cancel()
	if err := stopProcessors(processors); err != nil && !errors.Is(err, context.Canceled) {
		return runReport{}, err
	}
	stopped = true

	latencies := combineLatencies(processors.latencies)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	publishDuration := publishedAt.Sub(startedAt)
	drainDuration := completedAt.Sub(publishedAt)
	endToEndDuration := completedAt.Sub(startedAt)
	deliveries := cfg.events * int64(cfg.consumers)
	commit, dirty := buildRevision()
	result := runReport{
		SchemaVersion: reportSchemaVersion, Run: run, StartedAt: startedAt.UTC().Format(time.RFC3339Nano),
		Commit: commit, Dirty: dirty, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), Environment: cfg.environment,
		Mode: cfg.mode, Topology: cfg.topology, Events: cfg.events, WarmupEvents: cfg.warmupEvents,
		Producers: cfg.producers, Consumers: cfg.consumers, RingSize: cfg.ringSize, BatchSize: cfg.batchSize,
		ProducerWait: cfg.producerWait, ConsumerWait: cfg.consumerWait, PayloadSizeBytes: cfg.payloadSize,
		PayloadWorkingSetBytes: cfg.ringSize * int64(cfg.payloadSize), HandlerDelayNS: cfg.handlerDelay.Nanoseconds(),
		PublishSeconds: publishDuration.Seconds(), DrainSeconds: drainDuration.Seconds(), EndToEndSeconds: endToEndDuration.Seconds(),
		PublishedPerSecond: perSecond(cfg.events, publishDuration), EndToEndPerSecond: perSecond(cfg.events, endToEndDuration),
		DeliveriesPerSecond: perSecond(deliveries, endToEndDuration), AllocationBytes: memoryAfter.TotalAlloc - memoryBefore.TotalAlloc,
		Allocations: memoryAfter.Mallocs - memoryBefore.Mallocs, GCCount: memoryAfter.NumGC - memoryBefore.NumGC,
		LatencySamples: len(latencies), Checksum: sumChecksums(processors.checksums),
	}
	if cfg.mode == "latency" {
		result.SampleEvery = cfg.sampleEvery
	}
	if cfg.payloadSize > 0 {
		result.PayloadMiBPerSecond = perSecond(cfg.events*int64(cfg.payloadSize), endToEndDuration) / (1024 * 1024)
	}
	if len(latencies) > 0 {
		result.LatencyP50NS = percentile(latencies, 0.50)
		result.LatencyP95NS = percentile(latencies, 0.95)
		result.LatencyP99NS = percentile(latencies, 0.99)
		result.LatencyP999NS = percentile(latencies, 0.999)
		result.LatencyMaxNS = latencies[len(latencies)-1]
	}
	return result, nil
}

func startProcessors(cfg config, ring *disruptor.RingBuffer[*loadEvent], ctx context.Context) (processorSet, error) {
	set := processorSet{
		processors: make([]*disruptor.BatchProcessor[*loadEvent], cfg.consumers),
		done:       make([]chan error, cfg.consumers),
		latencies:  make([][]int64, cfg.consumers),
		checksums:  make([]uint64, cfg.consumers),
	}
	pipeline := cfg.topology == "pipeline"
	var dependency *disruptor.Sequence
	for i := range set.processors {
		index := i
		barrier := ring.NewBarrier()
		if pipeline && dependency != nil {
			barrier = ring.NewBarrier(dependency)
		}
		recordLatency := cfg.mode == "latency" && (!pipeline || index == cfg.consumers-1)
		if recordLatency {
			samples := (cfg.events + cfg.sampleEvery - 1) / cfg.sampleEvery
			set.latencies[index] = make([]int64, 0, samples)
		}
		processor, err := disruptor.NewBatchProcessor(ring, barrier, func(event *loadEvent, _ int64, _ bool) error {
			for _, value := range event.Payload {
				set.checksums[index] += uint64(value)
			}
			if cfg.handlerDelay > 0 {
				time.Sleep(cfg.handlerDelay)
			}
			if recordLatency && event.Sampled {
				set.latencies[index] = append(set.latencies[index], time.Since(event.PublishedAt).Nanoseconds())
			}
			return nil
		}, disruptor.WithMaxBatchSize(cfg.batchSize))
		if err != nil {
			return processorSet{}, err
		}
		set.processors[index] = processor
		dependency = processor.Sequence()
		set.done[index] = make(chan error, 1)
		go func() { set.done[index] <- processor.Run(ctx) }()
	}
	if pipeline {
		set.terminal = []*disruptor.Sequence{set.processors[len(set.processors)-1].Sequence()}
	} else {
		set.terminal = make([]*disruptor.Sequence, len(set.processors))
		for i, processor := range set.processors {
			set.terminal[i] = processor.Sequence()
		}
	}
	ring.AddGatingSequences(set.terminal...)
	return set, nil
}

func publishPhase(ctx context.Context, cfg config, ring *disruptor.RingBuffer[*loadEvent], base, events int64, sample bool) error {
	errorsCh := make(chan error, cfg.producers)
	var publishers sync.WaitGroup
	for producer := 0; producer < cfg.producers; producer++ {
		producerID := int64(producer)
		publishers.Go(func() {
			for index := producerID; index < events; index += int64(cfg.producers) {
				sequence, err := ring.Next(ctx)
				if err != nil {
					errorsCh <- err
					return
				}
				event := ring.Get(sequence)
				event.Value = base + index
				event.Sampled = sample && index%cfg.sampleEvery == 0
				if event.Sampled {
					event.PublishedAt = time.Now()
				}
				for payloadIndex := range event.Payload {
					event.Payload[payloadIndex] = byte(sequence + int64(payloadIndex))
				}
				ring.PublishSequence(sequence)
			}
		})
	}
	publishers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		return err
	}
	return nil
}

func waitForSequences(ctx context.Context, sequences []*disruptor.Sequence, target int64) error {
	for _, sequence := range sequences {
		for sequence.Load() < target {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				runtime.Gosched()
			}
		}
	}
	return nil
}

func stopProcessors(set processorSet) error {
	for _, processor := range set.processors {
		if processor != nil {
			processor.Halt()
		}
	}
	var first error
	for i, result := range set.done {
		if result == nil {
			continue
		}
		if err := <-result; err != nil && first == nil {
			first = fmt.Errorf("processor %d: %w", i, err)
		}
	}
	return first
}

func parseProducerWait(value string) (disruptor.ProducerWaitMode, error) {
	switch value {
	case "yielding":
		return disruptor.ProducerWaitYielding, nil
	case "blocking":
		return disruptor.ProducerWaitBlocking, nil
	case "busy-spin":
		return disruptor.ProducerWaitBusySpin, nil
	default:
		return 0, fmt.Errorf("producer-wait must be yielding, blocking, or busy-spin")
	}
}

func parseConsumerWait(value string) (disruptor.WaitStrategy, error) {
	switch value {
	case "blocking":
		return disruptor.BlockingWait(), nil
	case "sleeping":
		return disruptor.SleepingWait(), nil
	case "yielding":
		return disruptor.YieldingWait(), nil
	case "busy-spin":
		return disruptor.BusySpinWait(), nil
	default:
		return nil, fmt.Errorf("consumer-wait must be blocking, sleeping, yielding, or busy-spin")
	}
}

func combineLatencies(groups [][]int64) []int64 {
	total := 0
	for _, values := range groups {
		total += len(values)
	}
	combined := make([]int64, 0, total)
	for _, values := range groups {
		combined = append(combined, values...)
	}
	return combined
}

func percentile(values []int64, quantile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func sumChecksums(values []uint64) uint64 {
	var sum uint64
	for _, value := range values {
		sum += value
	}
	return sum
}

func perSecond(count int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func buildRevision() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	var revision string
	var dirty bool
	if ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				dirty = setting.Value == "true"
			}
		}
	}
	if revision == "" {
		if output, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
			revision = strings.TrimSpace(string(output))
		}
		if output, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
			dirty = len(output) > 0
		}
	}
	return revision, dirty
}
