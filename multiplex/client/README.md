# Multiplex Client

The `client` package implements an example of a client for the multiplex
library. Extensions can be implemented in this client or using the interfaces.

This source code publishes a `multiplex/client` golang package.

## Extensions

We define the rules for *configuration extensions* and *data extensions*,
which may be used to get custom configuration objects and to process- or mutate
data using a custom client business logic, e.g. which involve calls to remote
servers, or which stores data in a separate database, etc.

## Interfaces

| Interface | Description |
| --- | --- |
| `SyncConfigExtensionFn` | Provides custom state-sync configuration values. |
| `SeedConfigExtensionFn` | Provides custom seed nodes configuration values. |
| `ValidatorUpdateExtensionFn` | Provides custom auditing/reporting for validator updates. |
| `ConsensusUpdateExtensionFn` | Provides custom auditing/reporting for consensus updates. |
| `SnapshotMutationExtensionFn` | Provides custom processing units for snapshots data. |
| `CheckMutationResultExtensionFn` | Provides custom auditing units for snapshot mutation results. |
| `SnapshotRestoreExtensionFn` | Provides custom restoration units for snapshots data. |
| `CheckTxExtensionFn` | Provides custom auditing/reporting for individual transactions. |
| `PrepareProposalExtensionFn` | Provides custom pre-processing units for transactions data. |
| `ProcessProposalExtensionFn` | Provides custom post-processing units for transactions data. |
| `FinalizeBlockExtensionFn` | Provides custom processing units for blocks data. |
| `CommitExtensionFn` | Provides custom auditing/reporting for commited blocks. |

We provide several example implementations that basically just *deep-copy* the
input. Obviously, if you are developing a custom extension, you would do more
than just deep-copy input objects.

An example for `SyncConfigExtensionFn` is: `DefaultSyncConfigExtension`
An example for `SeedConfigExtensionFn` is: `DefaultSeedConfigExtension`
An example for `ValidatorUpdateExtensionFn` is: `DefaultValidatorUpdateExtension`
An example for `ConsensusUpdateExtensionFn` is: `DefaultConsensusUpdateExtension`
An example for `SnapshotMutationExtensionFn` is: `DefaultSnapshotMutationExtension`
An example for `CheckMutationResultExtensionFn` is: `DefaultCheckMutationResultExtension`
An example for `SnapshotRestoreExtensionFn` is: `DefaultSnapshotRestoreExtension`
An example for `CheckTxExtensionFn` is: `DefaultCheckTxExtension`
An example for `PrepareProposalExtensionFn` is: `DefaultPrepareProposalExtension`
An example for `ProcessProposalExtensionFn` is: `DefaultProcessProposalExtension`
An example for `FinalizeBlockExtensionFn` is: `DefaultFinalizeBlockExtension`
An example for `CommitExtensionFn` is: `DefaultCommitExtension`

## Custom extensions

You may implement other extensions and use them by modifying the source code
at `multiplex/client.go`. This file is present only for this purpose, thus if
the extension you are developing should *become the default*, you may as well
just overwrite the `Default..Extension` method with your custom business logic.

## Testing

You can test the `client` package using the following unit test suite:

```bash
go test github.com/ice-blockchain/cometbft/multiplex/client -test.v -count=1
```

Note that this test suite is apart from the `client` package and implemented
in a `client_test` package instead.
