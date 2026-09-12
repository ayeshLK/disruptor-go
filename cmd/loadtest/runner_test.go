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
	"strings"
	"testing"
	"time"
)

func TestValidateConfig(t *testing.T) {
	valid := testConfig()
	if err := validateConfig(valid); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name   string
		change func(*config)
		want   string
	}{
		{name: "mode", change: func(c *config) { c.mode = "combined" }, want: "mode"},
		{name: "topology", change: func(c *config) { c.topology = "graph" }, want: "topology"},
		{name: "pipeline stages", change: func(c *config) { c.topology, c.consumers = "pipeline", 1 }, want: "two"},
		{name: "payload", change: func(c *config) { c.payloadSize = -1 }, want: "payload-size"},
		{name: "sampling", change: func(c *config) { c.sampleEvery = 0 }, want: "sample-every"},
		{name: "producer wait", change: func(c *config) { c.producerWait = "sleeping" }, want: "producer-wait"},
		{name: "consumer wait", change: func(c *config) { c.consumerWait = "unknown" }, want: "consumer-wait"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.change(&cfg)
			if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateConfig: got %v, want error containing %q", err, test.want)
			}
		})
	}
}

func TestPercentileUsesNearestRank(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, test := range []struct {
		quantile float64
		want     int64
	}{
		{quantile: 0, want: 1},
		{quantile: 0.50, want: 5},
		{quantile: 0.95, want: 10},
		{quantile: 1, want: 10},
	} {
		if got := percentile(values, test.quantile); got != test.want {
			t.Fatalf("percentile(%v): got %d, want %d", test.quantile, got, test.want)
		}
	}
	if got := percentile(nil, 0.99); got != 0 {
		t.Fatalf("empty percentile: got %d", got)
	}
}

func TestRunOnceThroughputBroadcast(t *testing.T) {
	cfg := testConfig()
	cfg.mode = "throughput"
	cfg.topology = "broadcast"
	cfg.producers = 2
	cfg.consumers = 2
	report, err := runOnce(cfg, 3)
	if err != nil {
		t.Fatal(err)
	}
	if report.Run != 3 || report.SchemaVersion != reportSchemaVersion {
		t.Fatalf("run identity: %+v", report)
	}
	if report.LatencySamples != 0 || report.SampleEvery != 0 {
		t.Fatalf("throughput mode recorded latency: %+v", report)
	}
	if report.PayloadWorkingSetBytes != cfg.ringSize*int64(cfg.payloadSize) {
		t.Fatalf("working set: got %d", report.PayloadWorkingSetBytes)
	}
	if report.Checksum == 0 {
		t.Fatalf("incomplete report: %+v", report)
	}
}

func TestRunOnceLatencyPipeline(t *testing.T) {
	cfg := testConfig()
	cfg.topology = "pipeline"
	cfg.consumers = 2
	report, err := runOnce(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	wantSamples := int((cfg.events + cfg.sampleEvery - 1) / cfg.sampleEvery)
	if report.LatencySamples != wantSamples {
		t.Fatalf("latency samples: got %d, want %d", report.LatencySamples, wantSamples)
	}
	if report.LatencyP999NS < report.LatencyP99NS || report.LatencyMaxNS < report.LatencyP999NS {
		t.Fatalf("invalid latency distribution: %+v", report)
	}
	if report.Topology != "pipeline" || report.Consumers != 2 {
		t.Fatalf("pipeline metadata: %+v", report)
	}
}

func testConfig() config {
	return config{
		mode: "latency", topology: "broadcast", events: 256, warmupEvents: 32,
		repetitions: 1, producers: 1, consumers: 1, ringSize: 64, batchSize: 16,
		producerWait: "yielding", consumerWait: "yielding", payloadSize: 16,
		sampleEvery: 8, timeout: 5 * time.Second,
	}
}
