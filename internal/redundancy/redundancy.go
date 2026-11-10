// Package redundancy is the seam where the Reed-Solomon and replication engines
// plug into the write path. It currently contains only a temporary no-op
// implementation and redundancy-mode validation.
package redundancy

import "fmt"

// RedundancyEngine is the seam where the Reed-Solomon / replication engine
// plugs into the write path.
type RedundancyEngine interface {
	// Encode splits value into data and parity fragments according to mode.
	Encode(key string, value []byte, mode string) ([][]byte, error)
}

// NoopRedundancy is a temporary pass-through implementation so the consensus
// layer can be developed and tested independently. It is replaced by the real
// engine in the redundancy task (task 03).
type NoopRedundancy struct{}

// NewNoopRedundancy returns the temporary pass-through redundancy engine.
// It is replaced in task 03.
func NewNoopRedundancy() RedundancyEngine {
	return NoopRedundancy{}
}

// Encode returns the value unchanged as a single fragment.
func (NoopRedundancy) Encode(key string, value []byte, mode string) ([][]byte, error) {
	return [][]byte{value}, nil
}

// ValidateMode rejects unknown redundancy modes.
func ValidateMode(mode string) error {
	switch mode {
	case "", "replication", "rs32":
		return nil
	default:
		return fmt.Errorf("invalid redundancy mode %q (expected \"replication\" or \"rs32\")", mode)
	}
}
