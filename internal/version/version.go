// Package version exposes the release version compiled into every binary.
package version

// Version is overridden at build time with
// -ldflags "-X distributed-kv-store/internal/version.Version=v1.0.0".
var Version = "dev"

// Value returns the compiled version or "dev" for an unstamped build.
func Value() string {
	if Version == "" {
		return "dev"
	}
	return Version
}
