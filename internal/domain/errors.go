package domain

import "errors"

var (
	ErrInvalidInput     = errors.New("invalid input")
	ErrStaleEvent       = errors.New("stale event")
	ErrLockTimeout      = errors.New("lock timeout")
	ErrCorruptState     = errors.New("corrupt state")
	ErrUnsupportedEvent = errors.New("unsupported event")
)
