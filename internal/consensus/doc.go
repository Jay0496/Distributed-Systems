// Package consensus implements the metadata Raft group: the Raft node wrapper,
// the finite state machine, and the commands applied through the log.
//
// The transport is injected by the caller, so the same package backs both the
// real TCP deployment and in-memory tests.
package consensus
