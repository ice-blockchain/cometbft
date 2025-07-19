package server

import (
	"context"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
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

// GetLogger returns the injected [cmtlog.Logger] instance.
func (DefaultServer) GetLogger() cmtlog.Logger {
	return cmtlog.NewNopLogger()
}

// GetRuntimeRegistry returns the injected node runtime manager.
func (DefaultServer) GetRuntimeRegistry() runtime.Manager {
	return nil
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
