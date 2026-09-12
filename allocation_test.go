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
	"testing"
)

func TestClaimPublishHotPathDoesNotAllocate(t *testing.T) {
	for _, producer := range []ProducerType{SingleProducer, MultiProducer} {
		t.Run(producerName(producer), func(t *testing.T) {
			ring, err := New(1024, producer, func() *testEvent { return new(testEvent) }, BusySpinWait())
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			allocations := testing.AllocsPerRun(1000, func() {
				sequence, err := ring.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				ring.Get(sequence).Value = sequence
				ring.PublishSequence(sequence)
			})
			if allocations != 0 {
				t.Fatalf("claim/publish allocations: got %v, want 0", allocations)
			}
		})
	}
}
