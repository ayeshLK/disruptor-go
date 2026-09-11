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
	"errors"
	"fmt"
	"log"

	disruptor "github.com/ayeshLK/lib-disruptor"
)

type OrderEvent struct{ OrderID int64 }

func main() {
	ring, err := disruptor.New(1024, disruptor.SingleProducer,
		func() *OrderEvent { return new(OrderEvent) }, disruptor.BlockingWait())
	if err != nil {
		log.Fatal(err)
	}
	processor, err := disruptor.NewBatchProcessor(ring, ring.NewBarrier(),
		func(event *OrderEvent, sequence int64, endOfBatch bool) error {
			fmt.Printf("sequence=%d order=%d endOfBatch=%t\n", sequence, event.OrderID, endOfBatch)
			return nil
		})
	if err != nil {
		log.Fatal(err)
	}
	ring.AddGatingSequences(processor.Sequence())
	done := make(chan error, 1)
	go func() { done <- processor.Run(context.Background()) }()
	if err := ring.Publish(context.Background(), func(event *OrderEvent, _ int64) error {
		event.OrderID = 42
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := ring.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	if err := <-done; !errors.Is(err, disruptor.ErrClosed) {
		log.Fatal(err)
	}
}
