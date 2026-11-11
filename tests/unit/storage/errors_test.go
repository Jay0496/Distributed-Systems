package storage_test

import (
	"errors"
	"fmt"
	"testing"

	"distributed-kv-store/internal/storage"
)

func TestSentinelErrorsSupportErrorsIs(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"not found", fmt.Errorf("wrapped: %w", storage.ErrNotFound), storage.ErrNotFound},
		{"conflict", fmt.Errorf("wrapped: %w", storage.ErrConflict), storage.ErrConflict},
		{"closed", fmt.Errorf("wrapped: %w", storage.ErrClosed), storage.ErrClosed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.want) {
				t.Fatalf("errors.Is(%v, %v) = false", tc.err, tc.want)
			}
		})
	}
}

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{storage.ErrNotFound, storage.ErrConflict, storage.ErrClosed}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Fatalf("%v unexpectedly matches %v", a, b)
			}
		}
	}
}
