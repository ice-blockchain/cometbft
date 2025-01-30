# CometBFT Multiplex
The [multiplex] package provides with an implementation of [CometBFT] that
allows for running concurrent and on-demand consensus instances, using many
different chains that run in parallel.

## Packages

The `multiplex` package implements a multiplex node backend compatible with
[CometBFT] networks, which is capable of running many separate networks
in parallel.

The `snapsapp` package implements a multi-network ABCI application
that enables consensus events mapping for the client implementation with
extension interfaces for several stages of a consensus instance.

The `client` package implements a *default* client integration for multiplex
features that may be used to *inject custom configuration* and to mutate
or otherwise use state machines as they are replicated on the networks.

The `server` package defines a server contract for the multiplex library and
may be used to interface with a multiplex node backend, e.g. to communicate
about the replication of new chains, or to dispatch events to the multiplex
CometBFT nodes that are actively running on a relay.

## RelayInfo API

A node multiplex backend serves two public APIs which may be used to request
relay information such as the Relay ID (CometBFT Node ID) or the networks
that are supported.

- P2P Server: Port `30001` - i.e. always uses `DiscoveryPort`
- RPC Server: Port `30000` - i.e. always uses `DiscoveryPort-1`

## CometBFT API

A node multiplex backend also serves two CometBFT APIs which may be used to
interact with underlying replication chains, e.g. to communicate with a node
about transactions.

- P2P Server: Port `30002` - i.e. always uses `DiscoveryPort+1`
- RPC Server: Port `30003` - i.e. always uses `DiscoveryPort+2`

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

## Configuration

A `config.MultiplexConfig` structure defines the multiplex state replication
configuration for a CometBFT node connecting to one or many networks.

Individual fields documentation can be found in `config.MultiplexConfig`.

### Options helpers:

- `multiplex.WithStrategy`
- `multiplex.WithChainSeeds`
- `multiplex.WithUserChains`
- `multiplex.WithDiscoveryPort`

### NewConfigOverwrite

To begin with, `NewConfigOverwrite` updates a node configuration in-place to
overwrite the services listen addresses such that there is one P2P- and one RPC
port per running node instance - i.e. to communicate with a node multiplex, you
must always use the same port and must not use different ports per network.

This method also overwrites the `P2P.Seeds` configuration option such that
each replicated chain *uses its own seed nodes*, if necessary, and the `WAL`
file is changed so that each replicated chain *writes to a separate WAL-file*.

Also, state-sync is forcefully **disabled** because the method used for
synchronization with individual replicated chains is **block-sync**.

### ReplicationStrategy

The `ReplicationStrategy` exports a string interface that determines the type
of replication being executed on this node. This strategy is notably used to
determine the type of node and if it should enable multiplex features.

We currently support two replication strategies:

- `"Network"`: The instance shall synchronize with replicated chains.
- `"Disable"`: The instance shall run as a legacy node, without multiplex.

The replication strategy of a node shall determine whether the node does
synchronize with replicated chains or not. Nodes that are not configured to
synchronize with replicated chains may only be used to synchronize with legacy
cometbft blockchain networks which are not compatible with nodes multiplexes.

### GenesisDocSet

The `multiplex.GenesisDocSet` consists of a slice of `types.GenesisDoc` objects which
define the initial conditions for a CometBFT node multiplex, in particular
their validator set, consensus parameters and ChainID.

We added an interface `node.IChecksummedGenesisDoc` based on the legacy
interface to enable compatibility with legacy nodes that use only a singular
`types.GenesisDoc` instance to connect to only one network without multiplex.

Importantly, when multiplex is enabled, we expect the `genesis.json` file
to contain a `multiplex.GenesisDocSet` JSON with one or many replicated chains.

# Implementation

The [multiplex] package provides with an implementation of [CometBFT] that
allows for running concurrent and on-demand consensus instances, using many
different chains that run in parallel.

## ChainRegistry

A `multiplex.ChainRegistry` interface is used for the initial configuration of seed
nodes and known networks.

This structure defines a registry pattern contract which should be searchable
by ChainID and by user address.

Note that the **ChainID slice is ordered in ascending alphabetical order**.

IMPORTANT: This structure requires the ChainID field to contain a user address
of 20 bytes in hexadecimal format and a fingerprint of 8 bytes.
e.g.: `mx-chain-FF080888BE0F48DE88927C3F49215B96548273AB-3E547E3280313019`

The `ChainRegistry` interface defines a contract for the methods:

- `ChainRegistry#HasChain`: True when the ChainID is known by the peer.
- `ChainRegistry#GetChains`: Returns an *ordered slice* of ChainID values.
- `ChainRegistry#GetSeeds`: Returns a comma-separated list of seed nodes.
- `ChainRegistry#GetStateSyncConfig`: Returns the custom state-sync config.
- `ChainRegistry#GetAddress`: Finds a user address for a ChainID.
- `ChainRegistry#FindChain`: Searches for a ChainID in the registry.

Note that we provide an internal implementation of the `ChainRegistry`
interface with `singletonChainRegistry` which is the implementation used
under-the-hood by `NewChainRegistry`.

## Reactor

The `multiplex.Reactor` implementation takes care of configuring node instances for the
correct replicated blockchain networks. The reactor starts multiple listeners
in parallel and sends messages on a channel to report about successful launch.

When a set of node listeners is ready, the multiplex reactor sends a message on
its channel `chainReadyCh` which contains a ChainID of the chain that is
being replicated. After this happened, the node is able to start syncing state
and/or blocks, as well as starting indexers, mempool, and other services.

This reactor is responsible for handling messages on `client.ReplicationChannel`
which is notably used to instruct a multiplex to replicate a new chain.

## proxy.ChainConns

The `proxy.ChainConns` is a breaking upgrade to `proxy.AppConns` which passes a
ChainID to connection methods such that the right connections are used for the
different replicated chains.

Note that only one shared ABCI client is used by all replicated chains.
On the other hand, we create x connections with the client, one per
replicated chain.

Return types of methods defined by this interface are compatible with
`proxy.AppConns` to prevent breaking the ABCI integration.

## SnapsApp

SnapsApp defines a multi-network ABCI application that enables consensus
events mapping for the client implementation.

Read-write mutexes are created to track initial heights on concurrent threads,
as well as for the currently working height in the process of finalizing and
committing blocks. Extensions can be implemented to hook into the processes
of creating blocks proposal, processing them, and/or to audit the data added
with a committed block.

Note that *only one instance* of the SnapsApp application must be created
for node multiplexes.

Also, note that given a non-nil [client.Acceptor] instance on the app, transaction
batches will be forwarded to the acceptor's `AcceptTx()` method. We do this
to ensure that blocks replay and block-sync always persist all batches.

# Client

A `client.Client` instance may be used to spawn on-demand consensus instances
using a list of active relays running one or many cometbft network.
Transactions that are broadcast will be first broadcast to other relays,
then verified and/or persisted, before they are committed to a network.

## Interfaces

  - `client.Transaction`: A transaction consists of an object with Data and Fingerprint.
  - `client.Acceptor`: Defines the contract for client-side transactions verification.
  - `client.Client`: Defines the contract for multiplex client implementations.

## BroadcastTx

1. It is expected that any transaction batch forwarded to `client.Client#BroadcastTx`
must have been previously accepted locally using `Acceptor#AcceptBroadcastTx`.

2. Transaction batches may concern one or more than one network, which must all
exist by the time the transaction gets full acceptance of the relays.

3. A broadcast operation is built around a list of predefined relays- of which
a majority must be healthy-, a user address and a transactions batch.

4. A complete consensus instance must succeed before a transaction batch may
be committed, such that all relays effectively agree to persist the batch.

# Server

A `server.Server` instance may be used to instruct the multiplex about the replication
of new chains, and to dispatch events to the multiplex CometBFT nodes that
are actively running on a relay.

## Interfaces

  - `server.Server`: Describes a server implementation with Close() and MustStart().
  - `server.Backend`: Describes a multiplex node backend and implements Server.
  - `server.Jobs`: Describes a suite of extensible background routines for a backend.
  - `server.RelayAddress`: Defines a wrapper for relay communication addresses.

# MultiplexBackend

A `multiplex.MultiplexBackend` instance may be used to initialize one or many node
runtimes and defines a multiplex backend adapter implementation which
satisfies the `server.Server` interface.

MultiplexBackend implements the `server.Backend` interface for a multiplex.
This implementation makes use of an internal `Reactor` instance to read
local networks heights and uses instances of `p2p.Switch` to communicate
with relays about chain replications and transaction broadcasts.

Additionally, an internal `client.Acceptor` instance may be used to further
extend the broadcast process, e.g. to call RollbackTx.

## Methods

  - `NewServer()`: Creates a MultiplexBackend instance.
  - `MustStart()`: Spawns one or many node runtimes, i.e. one per network.
  - `Close()`: Closes the adapter and stops all node runtimes.

## Protobuf

The `multiplex` package enables Protobuf messages for different purposes,
e.g. for transporting Snapshots metadata or Nodes information.

We provide an *extension* to the `api/` package using Protobuf `.proto` files
in a custom folder `multiplex/proto/`. The package name `cometbft.multiplex.v1`
is used to extend the currently available [CometBFT] API Protobuf generation.

### MultiNetworkNodeInfo

This Protobuf definition consists in defining a `p2p.NodeInfo` implementation
that is compatible with node multiplex which are connected to multiple
replicated chains.

Notable methods implementation include, but are not limited to:

- `GetNetworks()`: Get the list of ChainID from replicated chains of a node.

Note that the `MultiNetworkNodeInfo` Protobuf message is *compatible* with
the legacy `cometbft.p2p.v1.NodeInfo`.

#### Generating from Protobuf definition

```bash
	protoc -I=$GOPATH/src \
			-I=$GOPATH/pkg/mod/github.com/cosmos/gogoproto\@v1.6.0/
			-I=proto/ \
			-I=multiplex/proto/ \
			--gogofaster_out=api/ \
			multiplex/proto/cometbft/multiplex/v1/types.proto

	mv api/github.com/ice-blockchain/cometbft/* api/cometbft/
	rm -rf api/github.com
```

## Testing

Multiple unit test suites are provided with the `multiplex` package. You can
run one of these full unit test suites with the following commands:

```bash
	# running the full unit test suites
	go test github.com/ice-blockchain/cometbft/multiplex -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/client -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/server -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/snapsapp -test.v
```

Alternatively, you can also run individual unit tests or unit test suites
using one of the following commands:

```bash
	# running individual unit test suites
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexGenesis.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexDB.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexFS.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexExtendedChainID.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexChainState.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexReactor.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex -run TestMultiplexP2P.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/client -run TestMultiplexClient.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/server -run TestMultiplexServer.* -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/snapsapp -run TestABCI.* -test.v
```

## Linter

```bash
	# running the pre-commit step(s)
	`make lint`
```

The preferred linter is `golangci-lint` as included in the `pre-commit` steps
which can also be run manually with the following command:

```bash
	# running the linter manually
	golangci-lint run -c .golangci.yml --fix --disable revive,iface,recvcheck
```

## Runtime

A more comprehensive *node setup guide* should be provided in a separate
document. This section merely lists the *commands* that have been modified
or added as part of this implementation.

```bash
	# configuring a nodes multiplex (requires users.json)
	go run ./cmd/cometbft/main.go init --home /tmp/cometbftmx --multiplex

	# starting the nodes multiplex (requires genesis.json)
	go run ./cmd/cometbft/main.go multiplex --home /tmp/cometbftmx
```

## References

This implementation is based on [CometBFT] `v1.x` branch, which is still under
active development. Therefore, it is utterly important to keep track of updates
committed to the upstream branch as listed here: [cometbft-v1x].

## Links

- Source code for `multiplex`: [multiplex]
- Source code for `snapsapp`: [snapsapp]
- Source code for `client`: [client]
- Source code for `server`: [server]
- Technical definition: [multiplex-notion]

## Other resources

- CometBFT v1.x Release Branch: [CometBFT]
- CometBFT v1.x Commits Log: [cometbft-v1x]

[multiplex]: https://github.com/ice-blockchain/cometbft/tree/multiplex/
[multiplex-notion]: https://www.notion.so/leftclick/Nodes-Multiplex-10d0a77b88c88050ac8bf75c012d1b00
[snapsapp]: https://github.com/ice-blockchain/cometbft/tree/multiplex/multiplex/snapsapp/
[client]: https://github.com/ice-blockchain/cometbft/tree/multiplex/multiplex/client/
[server]: https://github.com/ice-blockchain/cometbft/tree/multiplex/multiplex/server/
[CometBFT]: https://github.com/ice-blockchain/cometbft/tree/v1.x/README.md
[cometbft-v1x]: https://github.com/ice-blockchain/cometbft/commits/v1.x/
