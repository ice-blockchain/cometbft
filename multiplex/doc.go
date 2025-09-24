/*
The [multiplex] package provides with an implementation of [CometBFT] that
allows for running concurrent and on-demand consensus instances, using many
different chains that run in parallel.

# Packages

The `multiplex` package implements a multiplex node backend compatible with
[CometBFT] networks, which is capable of running many separate networks
in parallel.

The `snapsapp` package implements a multi-network ABCI application
that enables consensus events mapping for the client implementation with
extension interfaces for several stages of a consensus instance.

The `client` package implements a default client integration and defines
the rules for further implementation of multiplex clients. A full-featured
integration is provided with `MultiplexClient` to satisfy this contract.

The `runtime` package defines a `Registry` of network runtimes and may be
used to start/stop and idle multiplex nodes, e.g. to start running the
consensus services for a given chain which implements CometBFT. Importantly,
the functionality of on-demand chain runtimes is split amongst several
services, including: a `BroadcastPool` to manage the acceptance of transactions
by peers, a `ReplicationPool` to manage the replication of individual chains
and lastly, also a `ConsensusPool` which is responsible for running services
for consensus, which are required per each replication chain runtime.

# RelayInfo API

A node multiplex backend serves two public APIs which may be used to request
relay information such as the Relay ID (CometBFT Node ID) or the networks
that are supported.

- P2P Server: Port `30001` - i.e. always uses `DiscoveryPort`
- RPC Server: Port `30000` - i.e. always uses `DiscoveryPort-1`

# CometBFT API

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
- Naming convention `NewChainAbc()` must return `(*ChainAbc, error)`.
- Naming convention for imports with `mx` prefix for multiplex features.
- Naming convention for imports with `cmt` prefix for cometbft features.

# Configuration

A [config.MultiplexConfig] structure defines the multiplex state replication
configuration for a CometBFT node connecting to one or many networks.

## GenesisDocSet

The [helpers.GenesisDocSet] consists of a slice of `types.GenesisDoc` objects which
define the initial conditions for a CometBFT node multiplex, in particular
their validator set, consensus parameters and ChainID.

We added an interface `node.IChecksummedGenesisDoc` based on the legacy
interface to enable compatibility with legacy nodes that use only a singular
`types.GenesisDoc` instance to connect to only one network without multiplex.

Importantly, when multiplex is enabled, we expect the `genesis.json` file
to contain a [helpers.GenesisDocSet] JSON with one or many replicated chains.

# Implementation

The [multiplex] package provides with an implementation of [CometBFT] that
allows for running concurrent and on-demand consensus instances, using many
different chains that run in parallel.

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

A [client.Client] instance may be used to spawn on-demand consensus instances
using a list of active relays running one or many cometbft network.
Transactions that are broadcast will be first broadcast to other relays,
then verified and/or persisted, before they are committed to a network.

## Interfaces

  - [client.Transaction]: A transaction consists of an object with Data and Fingerprint.
  - [client.Acceptor]: Defines the contract for client-side transactions verification.
  - [client.Client]: Defines the contract for multiplex client implementations.

## BroadcastTx

1. It is expected that any transaction batch forwarded to [client.Client#BroadcastTx]
must have been previously accepted locally using [Acceptor#AcceptBroadcastTx].

2. Transaction batches may concern one or more than one network, which must all
exist by the time the transaction gets full acceptance of the relays.

3. A broadcast operation is built around a list of predefined relays- of which
a majority must be healthy-, a user address and a transactions batch.

4. A complete consensus instance must succeed before a transaction batch may
be committed, such that all relays effectively agree to persist the batch.

# Server

A [types.Server] instance may be used to instruct the multiplex about the replication
of new chains, and to dispatch events to the multiplex CometBFT nodes that
are actively running on a relay.

## Interfaces

  - [types.Server]: Describes a server implementation with Close() and MustStart().
  - [types.Backend]: Describes a multiplex node backend and implements Server.
  - [types.Jobs]: Describes a suite of extensible background routines for a backend.
  - [helpers.RelayAddress]: Defines a wrapper for relay communication addresses.

# MultiplexBackend

A [multiplex.MultiplexBackend] instance may be used to initialize one or many node
runtimes and defines a multiplex backend adapter implementation which
satisfies the [types.Server] interface.

MultiplexBackend implements the [types.Backend] interface for a multiplex.
This implementation makes use of an internal [multiplex.Reactor] instance to read
local networks heights and uses instances of [p2p.Switch] to communicate
with relays about chain replications and transaction broadcasts.

Additionally, an internal [client.Acceptor] instance may be used to further
extend the broadcast process, e.g. to call RollbackTx.

## Methods

  - [multiplex.NewServer]: Creates a MultiplexBackend instance.
  - [multiplex.MultiplexBackend#OnStart]: Spawns node runtimes on-demand.
  - [multiplex.MultiplexBackend#OnStop]: Closes the adapter and stops any node runtimes.

# Protobuf

The `multiplex` package enables Protobuf messages for different purposes,
e.g. for transporting Snapshots metadata or Nodes information.

We provide an *extension* to the `api/` package using Protobuf `.proto` files
in a custom folder `multiplex/proto/`. The package name `cometbft.multiplex.v1`
is used to extend the currently available [CometBFT] API Protobuf generation.

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
	go test github.com/ice-blockchain/cometbft/multiplex/runtime -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/snapsapp -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/p2p -test.v
	go test github.com/ice-blockchain/cometbft/multiplex/replay -test.v

# Linter

	# running the pre-commit step(s)
	`make lint`

The preferred linter is `golangci-lint` as included in the `pre-commit` steps
which can also be run manually with the following command:

	# running the linter manually
	golangci-lint run -c .golangci.yml --fix --disable revive,iface,recvcheck

# Monitoring

A *prometheus exporter* is run as a HTTP server that delivers metrics. To visualize
this data, you will need a `grafana` installation and a prometheus types.

- Edit the `prometheus.yml` of your server and update its `scrape_config` so
that it collects metrics from the built-in prometheus exporter:

```yaml
scrape_configs:
  - job_name: "prometheus"
    metrics_path: "/"
    static_configs:
  - targets: ["localhost:30004"]

```

- Run the *prometheus server*, by default it runs at `http://localhost:9090`.
- Run the grafana server and access it using `http://localhost:3000`.
- Add a *data source* in grafana server and connect it to the *prometheus server*.

# References

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
*/
package multiplex
