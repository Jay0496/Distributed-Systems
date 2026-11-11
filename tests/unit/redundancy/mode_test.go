package redundancy_test

import (
	"strings"
	"testing"

	"distributed-kv-store/internal/redundancy"
)

func TestParseMode(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    redundancy.Mode
		wantErr bool
	}{
		{name: "empty selects replication", raw: "", want: redundancy.ModeReplication},
		{name: "replication", raw: "replication", want: redundancy.ModeReplication},
		{name: "rs32", raw: "rs32", want: redundancy.ModeRS32},
		{name: "unknown", raw: "triple", wantErr: true},
		{name: "wrong case", raw: "RS32", wantErr: true},
		{name: "whitespace", raw: " rs32", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := redundancy.ParseMode(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseMode(%q) = %q, want error", tc.raw, got)
				}
				if !strings.Contains(err.Error(), "invalid redundancy mode") {
					t.Fatalf("ParseMode(%q) error = %v, want a mode explanation", tc.raw, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseMode(%q) returned error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("ParseMode(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestModeValid(t *testing.T) {
	cases := []struct {
		mode redundancy.Mode
		want bool
	}{
		{redundancy.ModeRS32, true},
		{redundancy.ModeReplication, true},
		{"", false},
		{"triple", false},
		{"RS32", false},
	}

	for _, tc := range cases {
		if got := tc.mode.Valid(); got != tc.want {
			t.Errorf("Mode(%q).Valid() = %v, want %v", tc.mode, got, tc.want)
		}
	}
}

func TestModeString(t *testing.T) {
	if got := redundancy.ModeRS32.String(); got != "rs32" {
		t.Fatalf("ModeRS32.String() = %q, want %q", got, "rs32")
	}
	if got := redundancy.ModeReplication.String(); got != "replication" {
		t.Fatalf("ModeReplication.String() = %q, want %q", got, "replication")
	}
}
