// Package disruptor provides an in-process, bounded event exchange based on a
// preallocated ring buffer and monotonically increasing sequences.
//
// Producers claim a sequence, mutate its preallocated event, and publish the
// sequence. Consumers wait through a SequenceBarrier, process available events
// in batches, and advance their own Sequence. The slowest registered gating
// sequence prevents producers from overwriting unread entries.
package disruptor
