/*
This package provides a server contract for the multiplex library.

An [Acceptor] instance is injected in a [Server] to perform pre-committing
verification and/or persistence of transactions data. If the acceptor method
returns an error, the transactions data must be discarded entirely.

A [Server] instance may be used to instruct the multiplex about the replication
of new chains, and to dispatch events to the multiplex CometBFT nodes that
are actively running on a relay.

# Interfaces

  - [Server]: Describes a server implementation with Close() and MustStart().
  - [Backend]: Describes a multiplex node backend and implements Server.
  - [Jobs]: Describes a suite of extensible background routines for a backend.
  - [RelayAddress]: Defines a wrapper for relay communication addresses.

# Testing

You can test the server package using the following unit test suite:

```bash
go test github.com/ice-blockchain/cometbft/multiplex/server -test.v -count=1
```

Note that this test suite is apart from the `server` package and implemented
in a `server_test` package instead.
*/
package server
