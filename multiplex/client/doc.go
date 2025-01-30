/*
This package provides a client contract for the multiplex library.

An [Acceptor] instance is injected in a [Client] to perform pre-committing
verification and/or persistence of transactions data. If the acceptor method
returns an error, the transactions data must be discarded entirely.

A [Client] instance may be used to spawn on-demand consensus instances using
a list of active relays running one or many cometbft network. Transactions
that are broadcast will be first broadcast to other relays, then verified,
before they are committed to a network.

# Interfaces

  - [Transaction]: A transaction consists of an object with Data and Fingerprint.
  - [Acceptor]: Defines the contract for client-side transactions verification.
  - [Client]: Defines the contract for multiplex client implementations.

# BroadcastTx

1. It is expected that any transaction batch forwarded to [Client#BroadcastTx]
must have been previously accepted locally using [Acceptor#AcceptBroadcastTx].

2. Transaction batches may concern one or more than one network, which must all
exist by the time the transaction gets full acceptance of the relays.

3. A broadcast operation is built around a list of predefined relays- of which
a majority must be healthy-, a user address and a transactions batch.

4. A complete consensus instance must succeed before a transaction batch may
be committed, such that all relays effectively agree to persist the batch.

# Testing

You can test the client package using the following unit test suite:

```bash
go test github.com/ice-blockchain/cometbft/multiplex/client -test.v -count=1
```

Note that this test suite is apart from the `client` package and implemented
in a `client_test` package instead.
*/
package client
