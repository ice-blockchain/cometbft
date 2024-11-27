/*
The `snapsapp` package implements a multi-network ABCI application
that enables consensus events mapping for the client implementation.

# Application

The [SnapsApp] structure is configured via the following instances:

- A `Reactor` contains an implementation to retrieve networks and state stores.

When the SnapsApp is created, the replicated chains configuration is read from
the reactor interface and are validated upon ABCI requests at several stages
of an individual consensus instance.

The state store structure, that is retrieved from the reactor, using
the [GetStateStore] method, is expected to satisfy the [state.Store] interface.

The SnapsApp application enables consensus events mapping for the client
implementation by running specific hooks at different stages of the consensus
instance, including: `CheckTx`, `PrepareProposal` and `Commit`, amongst others.

# ABCI

The most prominent methods implemented with the [SnapsApp] ABCI application
include, but are not limited to:

- [SnapsApp#InitChain]: InitChain initializes the application's state.
- [SnapsApp#Info]: Info returns information about the application.
- [SnapsApp#CheckTx]: CheckTx must validate individual transaction data.
- [SnapsApp#Commit]: Commit must persist relevant application data.

We also provide implementations for all other *required* methods, including
for `PrepareProposal`, `ProcessProposal`, `FinalizeBlock` and `Commit`.

# Testing

You can test the ABCI methods using the following unit test suite:

```bash
go test github.com/ice-blockchain/cometbft/multiplex/snapsapp -test.v -count=1
```

Note that this test suite is apart from the `snapsapp` package and implemented
in a `snapsapp_test` package instead.
*/
package snapsapp
