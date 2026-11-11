package server

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"distributed-kv-store/internal/redundancy"
)

const (
	// maxKeyBytes bounds a key so the metadata record stays small and the Raft
	// metadata size guard can never be approached by key length alone.
	maxKeyBytes = 512

	// writeQuorum is the number of durably stored shards or copies required for
	// a write to succeed. It matches the three-node Raft quorum.
	writeQuorum = 3
)

// validateKey enforces the key rules: non-empty, at most maxKeyBytes, valid
// UTF-8, and free of control characters.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key must not be empty")
	}
	if len(key) > maxKeyBytes {
		return fmt.Errorf("key is %d bytes, limit is %d", len(key), maxKeyBytes)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("key must be valid UTF-8")
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return fmt.Errorf("key must not contain control characters")
		}
	}
	return nil
}

// validateValueSize rejects an empty or oversized value. The configured limit
// is MAX_VALUE_SIZE (default 8 MiB).
func validateValueSize(value []byte, maxValueSize int) error {
	if len(value) == 0 {
		return fmt.Errorf("value must not be empty")
	}
	if maxValueSize > 0 && len(value) > maxValueSize {
		return fmt.Errorf("value is %d bytes, limit is MAX_VALUE_SIZE %d", len(value), maxValueSize)
	}
	return nil
}

// validateMode rejects a mode that is neither empty (resolve from cluster
// config) nor one of the concrete modes.
func validateMode(mode string) error {
	if mode == "" {
		return nil
	}
	parsed, err := redundancy.ParseMode(mode)
	if err != nil {
		return err
	}
	if !parsed.Valid() {
		return fmt.Errorf("invalid redundancy mode %q", mode)
	}
	return nil
}

// validatePut checks every client-supplied field before any encoding or
// forwarding. It deliberately does not resolve the empty mode: that requires
// the leader's committed cluster configuration.
func validatePut(key string, value []byte, mode string, expectedVersion int64, maxValueSize int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateValueSize(value, maxValueSize); err != nil {
		return err
	}
	if err := validateMode(mode); err != nil {
		return err
	}
	if expectedVersion < 0 {
		return fmt.Errorf("expected_version must not be negative, got %d", expectedVersion)
	}
	return nil
}
