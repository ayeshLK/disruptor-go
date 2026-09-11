package disruptor

import "errors"

var (
	// ErrInvalidBufferSize indicates that a ring size is not a positive power of two.
	ErrInvalidBufferSize = errors.New("disruptor: buffer size must be a positive power of two")
	// ErrInvalidProducerType indicates that the selected producer mode is unknown.
	ErrInvalidProducerType = errors.New("disruptor: invalid producer type")
	// ErrInvalidClaimSize indicates that a batch claim is outside the ring bounds.
	ErrInvalidClaimSize = errors.New("disruptor: claim size must be between one and the buffer size")
	// ErrInsufficientCapacity indicates that a non-blocking claim would overtake a gate.
	ErrInsufficientCapacity = errors.New("disruptor: insufficient capacity")
	// ErrNilFactory indicates that New received no event factory.
	ErrNilFactory = errors.New("disruptor: event factory must not be nil")
	// ErrNilTranslator indicates that Publish or TryPublish received no translator.
	ErrNilTranslator = errors.New("disruptor: event translator must not be nil")
	// ErrNilHandler indicates that NewBatchProcessor received no event handler.
	ErrNilHandler = errors.New("disruptor: event handler must not be nil")
	// ErrInvalidBatchSize indicates that a processor batch limit is not positive.
	ErrInvalidBatchSize = errors.New("disruptor: batch size must be positive")
	// ErrAlerted indicates that a barrier alert interrupted a wait.
	ErrAlerted = errors.New("disruptor: barrier alerted")
	// ErrClosed indicates that the ring was closed before an operation completed.
	ErrClosed = errors.New("disruptor: ring buffer closed")
	// ErrAlreadyRunning indicates that Run was called on an active processor.
	ErrAlreadyRunning = errors.New("disruptor: processor already running")
)
