package client

import (
	"context"

	"github.com/ice-blockchain/cometbft/config"
)

// InjectSyncConfig defines a callback that returns a map with state-sync
// config objects [config.StateSyncConfig] mapped by ChainID. Individual
// state-sync configuration objects are passed through a extensionFn which
// may execute custom business logic to retrieve the state-sync config.
//
// We provide an example [DefaultSyncConfigExtension] implementation for the
// extensionFn parameter which only copies the default state-sync config.
//
// This method is called by [multiplex.NewChainRegistry] and *takes precedence*
// over the configuration passed in the [config.MultiplexConfig] object.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultSyncConfigExtension].
func InjectSyncConfig(
	conf *config.MultiplexConfig,
	extensionFn SyncConfigExtensionFn,
) map[string]*config.StateSyncConfig {
	// Allocate the return object, defaults are taken from conf.
	nextSyncConfig := map[string]*config.StateSyncConfig{}

	// Note that we do not provide any ordering here, add if necessary
	for userAddress, chainIds := range conf.UserChains {
		for _, chainID := range chainIds {
			// Injects UserAddress and ChainID to the context in case it is
			// necessary inside the [SyncConfigExtensionFn] extension.
			chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
			chainContext = context.WithValue(chainContext, KeyChainID, chainID)

			// We still use the [config.MultiplexConfig] if available.
			baseSyncConf := config.DefaultStateSyncConfig()
			if confChainSync, ok := conf.SyncConfig[chainID]; ok {
				baseSyncConf = confChainSync
			}

			// CALLBACK: You may add custom per-user-chain source code here.
			//
			// e.g.: Retrieving the sync config from a custom remote server
			// by implementing a custom SyncConfigExtensionFn, an example is
			// available with [DefaultSyncConfigExtension].
			chainSyncConf := extensionFn(chainContext, baseSyncConf)
			nextSyncConfig[chainID] = chainSyncConf
		}
	}

	return nextSyncConfig
}

// InjectChainSeeds defines a callback that returns a map with seed nodes
// config strings - i.e. comma-separated nodes - mapped by ChainID.
// Individual seed nodes configuration strings are passed through a
// extensionFn which may execute custom business logic to retrieve the
// network's seed nodes config.
//
// We provide an example [DefaultSeedConfigExtension] implementation for the
// extensionFn parameter which only copies the default seed nodes config.
//
// This method is called by [multiplex.NewChainRegistry] and *takes precedence*
// over the configuration passed in the [config.MultiplexConfig] object.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultSeedConfigExtension].
func InjectChainSeeds(
	conf *config.MultiplexConfig,
	extensionFn SeedConfigExtensionFn,
) map[string]string {
	// Allocate the return object, defaults are taken from conf.
	nextChainSeeds := map[string]string{}

	// Note that we do not provide any ordering here, add if necessary
	for userAddress, chainIds := range conf.UserChains {
		for _, chainID := range chainIds {
			// Injects UserAddress and ChainID to the context in case it is
			// necessary inside the [SeedConfigExtensionFn] extension.
			chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
			chainContext = context.WithValue(chainContext, KeyChainID, chainID)

			// We still use the [config.MultiplexConfig] if available.
			baseSeeds := ""
			if confChainSeeds, ok := conf.ChainSeeds[chainID]; ok {
				baseSeeds = confChainSeeds
			}

			// CALLBACK: You may add custom per-user-chain source code here.
			//
			// e.g.: Retrieving the seed nodes from a custom remote server
			// by implementing a custom SeedConfigExtensionFn, an example is
			// available with [DefaultSeedConfigExtension].
			chainSeedsConf := extensionFn(chainContext, baseSeeds)
			nextChainSeeds[chainID] = chainSeedsConf
		}
	}

	return nextChainSeeds
}
