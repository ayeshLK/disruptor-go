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

// Package disruptor provides an in-process, bounded event exchange based on a
// preallocated ring buffer and monotonically increasing sequences.
//
// Producers claim a sequence, mutate its preallocated event, and publish the
// sequence. Consumers wait through a SequenceBarrier, process available events
// in batches, and advance their own Sequence. The slowest registered gating
// sequence prevents producers from overwriting unread entries.
//
// The repository's production guide covers topology assembly, ring sizing,
// ownership, lifecycle, and operational validation.
package disruptor
