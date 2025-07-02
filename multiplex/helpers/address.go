package server_test

import (
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
)

func MakeAddress() crypto.Address {
	return ed25519.GenPrivKey().PubKey().Address()
}
