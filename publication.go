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

import "sync/atomic"

type publicationSlot struct {
	marker atomic.Int64
	state  atomic.Uint32
}

const (
	publicationUnresolved uint32 = iota
	publicationPublished
	publicationDiscarded
	publicationPreparing
)

func preparePublication(marker *atomic.Int64, state *atomic.Uint32, value int64) {
	state.Store(publicationPreparing)
	marker.Store(value)
	state.Store(publicationUnresolved)
}

func resolvePublication(marker *atomic.Int64, state *atomic.Uint32, value int64, resolution uint32) bool {
	if state.Load() != publicationUnresolved || marker.Load() != value {
		return false
	}
	return state.CompareAndSwap(publicationUnresolved, resolution)
}

func publicationState(marker *atomic.Int64, state *atomic.Uint32, value int64) uint32 {
	resolved := state.Load()
	if resolved == publicationUnresolved || resolved == publicationPreparing || marker.Load() != value {
		return publicationUnresolved
	}
	return resolved
}
