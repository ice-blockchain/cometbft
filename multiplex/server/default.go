package server

import (
	"context"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

type (
	DefaultServer struct{ service.BaseService }
)

var (
	_ Server = (*DefaultServer)(nil)
)

// Close implements io.Closer
func (DefaultServer) OnStop() error {
	return nil
}

// GetAcceptor returns the injected [client.Acceptor] implementation.
func (DefaultServer) GetAcceptor() client.Acceptor {
	return &client.DefaultAcceptor{}
}

// MustStart must start a replication backend or return an error.
func (DefaultServer) OnStart(ctx context.Context) {}
