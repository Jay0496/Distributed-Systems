package storage

import "errors"

// Sentinel errors returned by the storage package. Callers should test them
// with errors.Is so wrapped I/O failures remain distinguishable from missing
// records and version conflicts.
var (
	// ErrNotFound means the requested key, stripe, fragment, or config row does
	// not exist.
	ErrNotFound = errors.New("storage: not found")

	// ErrConflict means a metadata mutation conflicts with the stored version.
	ErrConflict = errors.New("storage: conflict")

	// ErrClosed means the store has already been closed.
	ErrClosed = errors.New("storage: closed")
)
