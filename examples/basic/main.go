package main

import (
	"context"
	"fmt"
	"log"
	"runtime"

	disruptor "github.com/ayeshLK/disruptor-go"
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
	for processor.Sequence().Load() < 0 {
		runtime.Gosched()
	}
	processor.Halt()
	if err := <-done; err != nil {
		log.Fatal(err)
	}
}
