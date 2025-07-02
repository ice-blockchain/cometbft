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

// GetAcceptor returns the injected [client.Acceptor] implementation.
func (DefaultServer) GetAcceptor() client.Acceptor {
	return &client.DefaultAcceptor{}
}

// OnStart implements [service.BaseService]
func (DefaultServer) OnStart(ctx context.Context) error {
	return nil
}

// OnStop implements [service.BaseService]
func (DefaultServer) OnStop() {}

// OnReset implements [service.BaseService]
func (DefaultServer) OnReset(_ context.Context) error {
	return nil
}
