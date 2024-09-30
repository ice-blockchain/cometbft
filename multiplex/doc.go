/*
Package multiplex defines structures to run parallel consensus instances
using separate storage layers and databases per user scope hash.

# Structures

  - [UserConfig]: defines state replication configuration for a CometBFT node.
  - [DataReplicationConfig]: determines the network mode (singular or plural).
  - [ScopeRegistry]: wraps searchable scope hashes by user and by user+scope.
  - [ScopeHashProvider]: cached scope hash registry (pair user address/scope).
  - [GenesisDocSet]: initial conditions for parallel CometBFT blockchain nodes.
  - [Reactor]: The multiplex reactor takes care of configuring nodes listeners.

# Design decisions

A multiplex configuration object is created using [UserConfig] and contains
the details of the parallel networks being run on a CometBFT node.

Following naming conventions are used to standardize the source code:

- Naming convention `MultiplexAbc` describes a map[string]*Abc
- Naming convention for `MultiplexAbcProvider()` which creates and initializes a MultiplexAbc
- Naming convention for `GetScopedAbc(string)` which tries to find a *Abc pointer by scope

A [ScopeRegistry] instance can be used to retrieve scope hashes by user address
without having to re-create all SHA256 hashes every time. A [ScopeHashProvider]
implementation is available with [DefaultScopeHashProvider].

To start parallel CometBFT nodes that replicate several different blockchain
networks, a [GenesisDocSet] instance must be initialized. It is expected that
the `genesis.json` file contains a list of supported blockchain networks rather
than just one network genesis configuration.

The [Reactor] implementation takes care of configuring node instances for the
correct replicated blockchain networks. The reactor starts multiple listeners
in parallel and sends messages on a channel to report about successful launch.

The multiplex reactor is responsible for starting the following node listeners:

- the ABCI server through [proxy.AppConns] ;
- the event bus for block events [types.EventBus] ;
- the transaction- and block indexers [txindex.IndexerService] ;
- the priv validator (signer) instance [types.PrivValidator] ;

When a set of node listeners is ready, the multiplex reactor sends a message on
its channel `listenersStartedCh` which contains the user scope hash of the chain
that is being replicated. After this happened, the node is able to start syncing
state and blocks, as well as starting indexers, pruners, mempool, and others.
*/
package multiplex
