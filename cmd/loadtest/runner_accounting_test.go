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

import "testing"

func TestRunOnceLatencyBroadcastCountsEachDelivery(t *testing.T) {
	cfg := testConfig()
	cfg.consumers = 2
	report, err := runOnce(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	perConsumer := (cfg.events + cfg.sampleEvery - 1) / cfg.sampleEvery
	want := int(perConsumer) * cfg.consumers
	if report.LatencySamples != want {
		t.Fatalf("latency samples: got %d, want %d", report.LatencySamples, want)
	}
}

func TestRunOnceSupportsWaitPolicyPairs(t *testing.T) {
	for _, pair := range []struct {
		producer string
		consumer string
	}{
		{producer: "blocking", consumer: "blocking"},
		{producer: "yielding", consumer: "sleeping"},
		{producer: "busy-spin", consumer: "busy-spin"},
	} {
		name := pair.producer + "/" + pair.consumer
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			cfg.mode = "throughput"
			cfg.producerWait = pair.producer
			cfg.consumerWait = pair.consumer
			if _, err := runOnce(cfg, 1); err != nil {
				t.Fatal(err)
			}
		})
	}
}
