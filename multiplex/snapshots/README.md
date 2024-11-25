# Snapshots

The `snapshots` package implements automatic support for CometBFT state sync
bootstrapping of nodes. State sync allows a new node joining a network to
simply fetch a recent snapshot of the application state instead of fetching
and applying all historical blocks. This can reduce the time needed to join the
network by several orders of magnitude (e.g. weeks to minutes).

This document describes the `multiplex/snapshots` implementation of the ABCI
state sync interface, for more information on CometBFT state sync in general see:

* [CometBFT State Sync for Developers](https://medium.com/cometbft/cometbft-core-state-sync-for-developers-70a96ba3ee35)
* [ABCI State Sync](https://docs.cometbft.com/v1.0/explanation/core/state-sync)
* [ABCI State Sync Methods](https://docs.cometbft.com/v1.0/spec/abci/abci++_basic_concepts#state-sync-methods)
* [Cosmos-SDK State Sync Snapshotting](https://github.com/cosmos/cosmos-sdk/blob/release/v0.50.x/store/snapshots/README.md)

## StateSnapshotter

The [Manager] implementation uses only *one* snapshotter: [StateSnapshotter]
which shall provide added methods `GetStateMachine()` and `AppHash()`.

This snapshotter executes upon **Commit()** of a block - i.e. after finalizing
blocks - in cosmos-sdk, this type of snapshotter is called a commitment
snapshotter because it is used to store block header information used in Commit.

The [ChunkWriter] and [ChunkReader] implementations are unchanged from the
original `cosmos-sdk` snapshots manager implementation.

The [Store] implementation is also unchanged from the original `cosmos-sdk`
snapshots manager implementation and serves as a storage layer using the
filesystem to store snapshots metadata and snapshot chunks.

The [WriterCloser], [StreamReader] and [StreamWriter] imeplementations are
as well, unchanged from the original `cosmos-sdk` snapshots manager
implementation. These are used to enable *streaming* snapshots data.

### Interface

The [Manager] implementation makes abstraction of the underlying *snapshotter*
instance being used by adding the [StateSnapshotter] interface.

An external implementation for this interface must be implemented. Following
implementation contract applies:

- [StateSnapshotter#Snapshot]: Should take a snapshot.
- [StateSnapshotter#Restore]: Should restore a snapshot.
- [StateSnapshotter#GetStateMachine]: Should return a copy of the [sm.State].
- [StateSnapshotter#AppHash]: Should return the `AppHash` from loaded state.

We shall provide this implementation with `ChainHistoryStore` in `multiplex`.

## Testing

You can test the chunks stream, snapshots manager and snapshotting features
using the following unit test suite:

```bash
go test github.com/ice-blockchain/cometbft/multiplex/snapshots -test.v -count=1
```

Note that this test suite is apart from the `snapshots` package and implemented
in a `snapshots_test` package instead.
