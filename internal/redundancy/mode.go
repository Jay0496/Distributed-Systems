package redundancy

import "fmt"

// Mode identifies how a value is made redundant across the cluster.
type Mode string

const (
	// ModeRS32 uses Reed-Solomon RS(3,2): three data shards and two parity
	// shards. The five shards are indexed 0..4 globally.
	ModeRS32 Mode = "rs32"
	// ModeReplication stores five full copies of the value, indexed 0..4.
	ModeReplication Mode = "replication"
)

// ParseMode validates a raw mode string. The empty string selects the default
// mode, replication, which preserves the pre-redundancy API contract.
func ParseMode(raw string) (Mode, error) {
	switch raw {
	case "":
		return ModeReplication, nil
	case string(ModeRS32):
		return ModeRS32, nil
	case string(ModeReplication):
		return ModeReplication, nil
	default:
		return "", fmt.Errorf("invalid redundancy mode %q (expected %q or %q)", raw, ModeReplication, ModeRS32)
	}
}

// Valid reports whether the mode is one of the supported concrete modes. The
// empty string is not a concrete mode; use ParseMode to resolve defaults.
func (m Mode) Valid() bool {
	return m == ModeRS32 || m == ModeReplication
}

// String returns the wire representation of the mode.
func (m Mode) String() string {
	return string(m)
}
