package support

import (
	"fmt"
	"math/rand"
	"testing"
)

// DefaultSeed is the fixed seed used by randomized test payloads. A test that
// needs a different seed should call LogSeed so a failure can be reproduced.
const DefaultSeed int64 = 20261112

// NewRand returns a deterministic random source for a seed.
func NewRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

// RandomValue returns a deterministic payload of size bytes for seed.
func RandomValue(seed int64, size int) []byte {
	if size < 0 {
		size = 0
	}
	rng := NewRand(seed)
	value := make([]byte, size)
	_, _ = rng.Read(value)
	return value
}

// Key returns the deterministic key for a prefix and index.
func Key(prefix string, index int) string {
	return fmt.Sprintf("%s-%04d", prefix, index)
}

// Keys returns count deterministic keys ordered by index.
func Keys(prefix string, count int) []string {
	keys := make([]string, count)
	for i := range keys {
		keys[i] = Key(prefix, i)
	}
	return keys
}

// LogSeed records the seed in the test log so a randomized failure is
// reproducible with the same fixtures.
func LogSeed(t testing.TB, seed int64) {
	t.Helper()
	t.Logf("deterministic test seed: %d", seed)
}
