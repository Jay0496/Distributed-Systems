package main

import "fmt"

// RedundancyEngine is the seam where the Reed-Solomon / replication engine
// plugs into the write path. The default implementation is a pass-through so
// the consensus layer can be developed and tested independently.
type RedundancyEngine interface {
	// Encode splits value into data and parity fragments according to mode.
	Encode(key string, value []byte, mode string) ([][]byte, error)
}

type noopRedundancy struct{}

func (noopRedundancy) Encode(key string, value []byte, mode string) ([][]byte, error) {
	return [][]byte{value}, nil
}

// validateMode rejects unknown redundancy modes.
func validateMode(mode string) error {
	switch mode {
	case "", "replication", "rs32":
		return nil
	default:
		return fmt.Errorf("invalid redundancy mode %q (expected \"replication\" or \"rs32\")", mode)
	}
}
