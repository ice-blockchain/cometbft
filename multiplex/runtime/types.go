package runtime

import (
	"github.com/ice-blockchain/cometbft/libs/service"
)

// RuntimeManager defines the contract for chain runtime managers.
type RuntimeManager interface {
	service.Service

	NumRuntimes() uint64
	NumSleeping() uint64

	ActiveRuntimes() map[string]uint64
	SleepingRuntimes() []string

	OnActivate(chainID string) error
	OnComplete(chainID string) error

	OnIdle(chainID string) error
	HasIdler() bool
}
