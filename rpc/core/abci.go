package core

import (
	"context"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	"github.com/ice-blockchain/cometbft/libs/bytes"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/proxy"
	ctypes "github.com/ice-blockchain/cometbft/rpc/core/types"
	rpctypes "github.com/ice-blockchain/cometbft/rpc/jsonrpc/types"
)

// ABCIQuery queries the application for some information.
// More: https://docs.cometbft.com/main/rpc/#/ABCI/abci_query
func (env *Environment) ABCIQuery(
	_ *rpctypes.Context,
	path string,
	data bytes.HexBytes,
	height int64,
	prove bool,
) (*ctypes.ResultABCIQuery, error) {
	// Inject the ChainID for access in ABCI
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, env.GenDoc.ChainID)

	resQuery, err := env.ProxyAppQuery.Query(ctx, &abci.QueryRequest{
		Path:   path,
		Data:   data,
		Height: height,
		Prove:  prove,
	})
	if err != nil {
		return nil, err
	}

	return &ctypes.ResultABCIQuery{Response: *resQuery}, nil
}

// ABCIInfo gets some info about the application.
// More: https://docs.cometbft.com/main/rpc/#/ABCI/abci_info
func (env *Environment) ABCIInfo(_ *rpctypes.Context) (*ctypes.ResultABCIInfo, error) {
	// Inject the ChainID for access in ABCI
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, env.GenDoc.ChainID)

	resInfo, err := env.ProxyAppQuery.Info(ctx, proxy.InfoRequest)
	if err != nil {
		return nil, err
	}

	return &ctypes.ResultABCIInfo{Response: *resInfo}, nil
}
