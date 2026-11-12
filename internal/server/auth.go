package server

import (
	"context"
	"errors"
)

// Authorizer is the authorization seam for server RPCs. Task 08 wires the
// allow-all implementation only while AUTH_TOKEN is empty; task 10 supplies the
// bearer-token implementation and enforces it in the handlers.
type Authorizer interface {
	Authorize(ctx context.Context, method string) error
}

// allowAllAuthorizer is used only when authentication is disabled. It is never
// installed when AUTH_TOKEN is set, so auth cannot silently degrade.
type allowAllAuthorizer struct{}

// Authorize permits every method while auth is disabled.
func (allowAllAuthorizer) Authorize(context.Context, string) error { return nil }

// SetAuthorizer installs the authorizer used by the server. It is intended to
// be called during process wiring before the gRPC server starts serving.
func (s *Server) SetAuthorizer(authorizer Authorizer) {
	s.authorizer = authorizer
}

// CheckAuth refuses to start an authenticated server without an authorizer.
// The process wiring calls it before serving; a nil authorizer with a non-empty
// AUTH_TOKEN is a configuration error, not a silent no-op.
func (s *Server) CheckAuth() error {
	if s.cfg.AuthToken != "" && s.authorizer == nil {
		return errors.New("AUTH_TOKEN is set but no authorizer is configured")
	}
	return nil
}
