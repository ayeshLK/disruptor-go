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
	"runtime"
	"testing"
)

func BenchmarkBufferedChannelBroadcast2(b *testing.B) {
	channels := [2]chan int64{
		make(chan int64, benchmarkRingSize),
		make(chan int64, benchmarkRingSize),
	}
	done := make(chan struct{}, len(channels))
	for _, channel := range channels {
		go func() {
			for value := range channel {
				runtime.KeepAlive(value)
			}
			done <- struct{}{}
		}()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, channel := range channels {
			channel <- int64(i)
		}
	}
	for _, channel := range channels {
		close(channel)
	}
	for range channels {
		<-done
	}
}

func BenchmarkBufferedChannelPipeline2(b *testing.B) {
	first := make(chan int64, benchmarkRingSize)
	second := make(chan int64, benchmarkRingSize)
	done := make(chan struct{})
	go func() {
		for value := range first {
			second <- value
		}
		close(second)
	}()
	go func() {
		for value := range second {
			runtime.KeepAlive(value)
		}
		close(done)
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		first <- int64(i)
	}
	close(first)
	<-done
}
