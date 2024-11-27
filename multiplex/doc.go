/*
The [multiplex] package provides with an implementation of [CometBFT] that
allows for running concurrent consensus instances, on many different chains
in parallel.

# Implementation

A [ChainRegistry] interface is used for the initial configuration of seed
nodes, for connecting to existing chains, and for custom state-sync, which
is currently disabled.
Importantly, when multiplex is enabled, we expect the `genesis.json` file
to contain a [GenesisDocSet] JSON.

The `snapsapp` package implements a multi-network ABCI application
that enables consensus events mapping for the client implementation with
extension interfaces for several stages of a consensus instance:

- `CheckTx`: Define auditing/reporting units for transactions ;
- `PrepareProposal`: Define pre-processing units for transactions data ;
- `ProcessProposal`: Define post-processing units for transactions data ;
- `FinalizeBlock`: Define processing units for blocks data ;
- `Commit`: Define auditing/reporting units for committed blocks.

The `client` package implements a *default* client integration for multiplex
features that may be used to *inject custom configuration* and to mutate
or otherwise use state machines as they are replicated on the networks.

## Source code conventions

We define some simple conventions in the `multiplex` package that must be
followed to improve readability of the source code and to provide a more
standardized implementation.

- Tests are colocated in a go package `multiplex_test` which imports `multiplex`.
- Tests are written in files with a suffix of `_test.go`, e.g. `chain_id_test.go`.
- Naming convention `ChainAbc` for structures with a ChainID, e.g. `ChainDB`.
- Naming convention `MultiplexAbc` for ChainID mappings, e.g. `MultiplexDB`.`
- Naming convention `NewMultiplexAbc()` must return `(MultiplexAbc, error)`.
- Naming convention `NewChainAbc()` must return `(ChainAbc, error)`.
- Naming convention for imports with `mx` prefix for multiplex features.
- Naming convention for imports with `cmt` prefix for cometbft features.

# Configuration

A `config.MultiplexConfig` structure defines the multiplex state replication
configuration for a CometBFT node connecting to one or many networks.

The `MultiplexConfig#UserChains` option is necessary for creating new
replicated chains, while `MultiplexConfig#ChainSeeds` is necessary for
connecting to existing ones.

Individual fields documentation can be found in [config.MultiplexConfig].

## Options helpers:

- [WithStrategy]
- [WithSyncConfig]
- [WithChainSeeds]
- [WithUserChains]
- [WithP2PStartPort]
- [WithRPCStartPort]

## NewConfigOverwrite

To begin with, [NewConfigOverwrite] updates a node configuration in-place to
overwrite the services listen addresses such that there is one P2P- and one RPC
port per replicated chain. Following ports overwrite apply:

- P2P: legacy `26656`, multiplex `30001`...`3000x` with x the index of nodes
- RPC: legacy `26657`, multiplex `40001`...`4000x` with x the index of nodes

The *index of a node* generally represents the index of the ChainID as per
the [ChainRegistry#GetChains] return value, e.g. given ChainIDs ['A','B','C'],
the index of a node for the chain 'B', is 1 and the index for the chain 'A',
is 0.

This method also overwrites the `P2P.Seeds` configuration option such that
each replicated chain *uses its own seed nodes*, and the `WAL` file is changed
so that each replicated chain *writes to a separate WAL-file*.

Also, state-sync is forcefully **disabled** because the preferred method of
synchronization with individual replicated chains is to use **block-sync**.

## ReplicationStrategy

The [ReplicationStrategy] exports a string interface that determines the type
of replication being executed on this node. This strategy is notably used to
determine the type of node and if it should enable multiplex features.

We currently support two replication strategies:

- `"Network"`: The instance shall synchronize with replicated chains.
- `"Disable"`: The instance shall run as a legacy node, without multiplex.

The replication strategy of a node shall determine whether the node does
synchronize with replicated chains or not. Nodes that are not configured to
synchronize with replicated chains may only be used to synchronize with legacy
cometbft blockchain networks which are not compatible with nodes multiplexes.

## GenesisDocSet

The [GenesisDocSet] consists of a slice of `types.GenesisDoc` objects which
define the initial conditions for a CometBFT node multiplex, in particular
their validator set, consensus parameters and ChainID.

We added an interface `node.IChecksummedGenesisDoc` based on the legacy
interface to enable compatibility with legacy nodes that use only a singular
`types.GenesisDoc` instance to connect to only one network without multiplex.

Importantly, when multiplex is enabled, we expect the `genesis.json` file
to contain a [GenesisDocSet] JSON with one or many replicated chains.

# Client interface

## Extensions

We define the rules for *configuration extensions*, *consensus extensions* and
also for *data extensions*, which may be used to overwrite configuration objects
and to process- or mutate data using a *custom client business logic*,
e.g. which involves calls to remote servers, or which stores data in a separate
database, etc.

### Interfaces

  - [SyncConfigExtensionFn]: Provides custom state-sync configuration values.
  - [SeedConfigExtensionFn]: Provides custom seed nodes configuration values.
  - [ValidatorUpdateExtensionFn]: Provides custom auditing/reporting units for validator updates.
  - [ConsensusUpdateExtensionFn]: Provides custom auditing/reporting units for consensus parameter updates.
  - [CheckTxExtensionFn]: Provides custom auditing/reporting units for transactions.
  - [PrepareProposalExtensionFn]: Provides custom pre-processing units for transactions data.
  - [ProcessProposalExtensionFn]: Provides custom post-processing units for transactions data.
  - [FinalizeBlockExtensionFn]: Provides custom processing units for blocks data.
  - [CommitExtensionFn]: Provides custom auditing/reporting units for committed blocks.
  - [CheckMutationResultExtensionFn]: Provides custom auditing units for state mutation results.

We provide several example implementations that basically just *deep-copy* the
input. Obviously, if you are developing a custom extension, you would do more
than just deep-copy input objects.

An example for [SyncConfigExtensionFn] is: [DefaultSyncConfigExtension]
An example for [SeedConfigExtensionFn] is: [DefaultSeedConfigExtension]
An example for [ValidatorUpdateExtensionFn] is: [DefaultValidatorUpdateExtension]
An example for [ConsensusUpdateExtensionFn] is: [DefaultConsensusUpdateExtension]
An example for [CheckTxExtensionFn] is: [DefaultCheckTxExtension]
An example for [PrepareProposalExtensionFn] is: [DefaultPrepareProposalExtension]
An example for [ProcessProposalExtensionFn] is: [DefaultProcessProposalExtension]
An example for [FinalizeBlockExtensionFn] is: [DefaultFinalizeBlockExtension]
An example for [CommitExtensionFn] is: [DefaultCommitExtension]
An example for [CheckMutationResultExtensionFn] is: [DefaultCheckMutationResultExtension]

## ChainRegistry

A [ChainRegistry] interface is used for the initial configuration of seed
nodes, for connecting to existing chains.

This structure defines a registry pattern contract which should be searchable
by ChainID and by user address.

Note that the **ChainID slice is ordered in ascending alphabetical order**.

IMPORTANT: This structure requires the ChainID field to contain a user address
of 20 bytes in hexadecimal format and a fingerprint of 8 bytes.
e.g.: `mx-chain-FF080888BE0F48DE88927C3F49215B96548273AB-3E547E3280313019`

The [ChainRegistry] interface defines a contract for the methods:

- [ChainRegistry#HasChain]: True when the ChainID is known by the peer.
- [ChainRegistry#GetChains]: Returns an *ordered slice* of ChainID values.
- [ChainRegistry#GetSeeds]: Returns a comma-separated list of seed nodes.
- [ChainRegistry#GetStateSyncConfig]: Returns the custom state-sync config.
- [ChainRegistry#GetAddress]: Finds a user address for a ChainID.
- [ChainRegistry#FindChain]: Searches for a ChainID in the registry.

Note that we provide an internal implementation of the [ChainRegistry]
interface with `singletonChainRegistry` which is the implementation used
under-the-hood by [NewChainRegistry].

## Reactor

The [Reactor] implementation takes care of configuring node instances for the
correct replicated blockchain networks. The reactor starts multiple listeners
in parallel and sends messages on a channel to report about successful launch.

When a set of node listeners is ready, the multiplex reactor sends a message on
its channel `chainReadyCh` which contains a ChainID of the chain that is
being replicated. After this happened, the node is able to start syncing state
and/or blocks, as well as starting indexers, mempool, and other services.

This structure is responsible for handling incoming messages on one or more
`Channel` instances whereby the following contract applies:

- The `OnStart()` must be called to setup replicated chain node listeners.
- The `p2p.Switch` calls `GetChannels()` when a new reactor is added to it.
- When a new peer joins our node, `InitPeer()` and `AddPeer()` are called.
- When a peer is stopped, obviously, `RemovePeer()` is called.
- When receiving messages on channels of a reactor, `Receive()` is called.

## proxy.ChainConns

The `proxy.ChainConns` is a breaking upgrade to `proxy.AppConns` which passes a
ChainID to connection methods such that the right connections are used for the
different replicated chains.

Note that only one shared ABCI client is used by all replicated chains.
On the other hand, we create x connections with the client, one per
replicated chain.

Return types of methods defined by this interface are compatible with
`proxy.AppConns` to prevent breaking the ABCI integration.

# SnapsApp

SnapsApp defines a multi-network ABCI application that enables consensus
events mapping for the client implementation.

This application creates snapshots of full state machines, without filtering
any of the included properties: ChainID, ConsensusParams, Validators, etc.

Read-write mutexes are created to track initial heights on concurrent threads,
as well as for the currently working height in the process of finalizing and
committing blocks. Extensions can be implemented to hook into the processes
of creating blocks proposal, processing them, and/or to audit the data added
with a committed block.

Note that *only one instance* of the SnapsApp application must be created
for node multiplexes.

# Protobuf

The `multiplex` package enables Protobuf messages for different purposes,
e.g. for transporting Snapshots metadata or Nodes information.

We provide an *extension* to the `api/` package using Protobuf `.proto` files
in a custom folder `multiplex/proto/`. The package name `cometbft.multiplex.v1`
is used to extend the currently available [CometBFT] API Protobuf generation.

## MultiNetworkNodeInfo

This Protobuf definition consists in defining a `p2p.NodeInfo` implementation
that is compatible with node multiplex which are connected to multiple
replicated chains.

Notable methods implementation include, but are not limited to:

- `GetChains()`: Get the list of ChainID from replicated chains of a node.
- `GetListenAddrs()`: Get the P2P listen addresses per replicated chains.
- `GetRPCAddresses()`: Get the RPC listen addresses per replicated chains.

Note that the `MultiNetworkNodeInfo` Protobuf message is *compatible* with
the legacy `cometbft.p2p.v1.NodeInfo`.

### Generating from Protobuf definition

	protoc -I=$GOPATH/src \
			-I=$GOPATH/pkg/mod/github.com/cosmos/gogoproto\@v1.6.0/
			-I=proto/ \
			-I=multiplex/proto/ \
			--gogofaster_out=api/ \
			multiplex/proto/cometbft/multiplex/v1/types.proto

	mv api/github.com/ice-blockchain/cometbft/* api/cometbft/
	rm -rf api/github.com

# Testing

Multiple unit test suites are provided with the `multiplex` package. You can
run one of these full unit test suites with the following commands:

	# running the full unit test suites
	go test github.com/ice-blockchain/cometbft/multiplex -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/client -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/snapsapp -test.v

Alternatively, you can also run individual unit tests or unit test suites
using one of the following commands:

	# running individual unit test suites
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexGenesis.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexDB.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexFS.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexExtendedChainID.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexChainState.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexReactor.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexP2P.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/client -run TestMultiplexClient.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/snapsapp -run TestABCI.* -test.v

# Linter

	# running the pre-commit step(s)
	`make lint`

The preferred linter is `golangci-lint` as included in the `pre-commit` steps
which can also be run manually with the following command:

	# running the linter manually
	golangci-lint run -c .golangci.yml --fix --disable revive,iface,recvcheck

# Runtime

A more comprehensive *node setup guide* should be provided in a separate
document. This section merely lists the *commands* that have been modified
or added as part of this implementation.

	# configuring a nodes multiplex (requires users.json)
	go run ./cmd/cometbft/main.go init --home /tmp/cometbftmx --multiplex

	# starting the nodes multiplex (requires genesis.json)
	go run ./cmd/cometbft/main.go multiplex --home /tmp/cometbftmx

# References

This implementation is based on [CometBFT] `v1.x` branch, which is still under
active development. Therefore, it is utterly important to keep track of updates
committed to the upstream branch as listed here: [cometbft-v1x].

## Links

- Source code for `multiplex`: [multiplex]
- Source code for `snapsapp`: [snapsapp]
- Source code for `client`: [client]
- Technical definition: [multiplex-notion]

## Other resources

- CometBFT v1.x Release Branch: [CometBFT]
- CometBFT v1.x Commits Log: [cometbft-v1x]

[multiplex]: https://github.com/ice-blockchain/cometbft/tree/multiplex/
[multiplex-notion]: https://www.notion.so/leftclick/Nodes-Multiplex-10d0a77b88c88050ac8bf75c012d1b00
[snapsapp]: https://github.com/ice-blockchain/cometbft/tree/multiplex/multiplex/snapsapp/
[client]: https://github.com/ice-blockchain/cometbft/tree/multiplex/multiplex/client/
[CometBFT]: https://github.com/ice-blockchain/cometbft/tree/v1.x/README.md
[cometbft-v1x]: https://github.com/ice-blockchain/cometbft/commits/v1.x/
*/
package multiplex
