package multiplex

import (
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
)

// ScopedSwitch embeds a [p2p.Switch] pointer and adds a scope hash
type ScopedSwitch struct {
	ScopeHash string
	*p2p.Switch
}

// ----------------------------------------------------------------------------
// Builders

// NewScopedSwitch creates a new [p2p.Switch] with the given config and scope hash.
func NewScopedSwitch(
	cfg *config.P2PConfig,
	transport p2p.Transport,
	userScopeHash string,
	options ...p2p.SwitchOption,
) *ScopedSwitch {
	sw := p2p.NewSwitch(cfg, transport, options...)
	ssw := &ScopedSwitch{
		ScopeHash: userScopeHash,
		Switch:    sw,
	}

	return ssw
}
