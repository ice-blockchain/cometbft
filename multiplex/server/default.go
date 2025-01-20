package server

import (
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

type (
	DefaultServer struct{}
)

var (
	_ Server = (*DefaultServer)(nil)
)

// Close implements io.Closer
func (DefaultServer) Close() error {
	return nil
}

// GetAcceptor returns the injected [client.Acceptor] implementation.
func (DefaultServer) GetAcceptor() client.Acceptor {
	return &client.DefaultAcceptor{}
}

// MustStart must start a replication backend or return an error.
func (DefaultServer) MustStart() {}
