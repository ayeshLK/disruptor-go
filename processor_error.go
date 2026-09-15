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

import "fmt"

// HandlerError reports an error returned by an event handler.
type HandlerError struct {
	// Sequence is the first sequence in the failed batch.
	Sequence int64
	// Err is the error returned by the handler.
	Err error
}

// Error returns a sequence-aware description of the handler failure.
func (e *HandlerError) Error() string {
	return fmt.Sprintf("disruptor: handler failed at sequence %d: %v", e.Sequence, e.Err)
}

// Unwrap returns the error returned by the handler.
func (e *HandlerError) Unwrap() error { return e.Err }

// HandlerPanicError reports a panic recovered from an event handler.
type HandlerPanicError struct {
	// Sequence is the first sequence in the failed batch.
	Sequence int64
	// Value is the value recovered from the handler panic.
	Value any
}

// Error returns a sequence-aware description of the recovered panic.
func (e *HandlerPanicError) Error() string {
	return fmt.Sprintf("disruptor: handler panicked at sequence %d: %v", e.Sequence, e.Value)
}

// Unwrap returns the panic value when that value implements error.
func (e *HandlerPanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
