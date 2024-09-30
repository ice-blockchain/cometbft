package multiplex

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"

	mxp2p "github.com/cometbft/cometbft/api/cometbft/multiplex/v1"
	tmp2p "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	bc "github.com/cometbft/cometbft/internal/blocksync"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	cmtstrings "github.com/cometbft/cometbft/internal/strings"
	cmtbytes "github.com/cometbft/cometbft/libs/bytes"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/statesync"
)

// ScopedProtocolVersion contains a scope hash and protocol versions for the software.
type ScopedProtocolVersion struct {
	ScopeHash string `json:"scope_hash"`
	P2P       uint64 `json:"p2p"`
	Block     uint64 `json:"block"`
	App       uint64 `json:"app"`
}

// ScopedChainInfo contains a scope hash and a ChainID
type ScopedChainInfo struct {
	ScopeHash string `json:"scope_hash"`
	ChainID   string `json:"chain_id"`
}

// ScopedListenAddr contains a scope hash and a listen address
type ScopedListenAddr struct {
	ScopeHash  string `json:"scope_hash"`
	ListenAddr string `json:"listen_addr"`
}

// Assert MultiNetworkNodeInfo satisfies NodeInfo.
var _ p2p.NodeInfo = MultiNetworkNodeInfo{}

// MultiNetworkNodeInfo is a multiplex node information exchanged
// between two peers during the CometBFT P2P handshake.
type MultiNetworkNodeInfo struct {
	// Replication configuration
	Scopes           []string                `json:"scopes"` // contains ScopeHash
	ProtocolVersions []ScopedProtocolVersion `json:"protocol_versions"`
	Networks         []ScopedChainInfo       `json:"networks"`     // contains chainID
	ListenAddrs      []ScopedListenAddr      `json:"listen_addrs"` // accepting incoming
	RPCAddresses     []ScopedListenAddr      `json:"rpc_addrs"`    // accepting RPC

	// Authenticate
	// TODO: replace with NetAddress
	DefaultNodeID p2p.ID `json:"id"`          // authenticated identifier
	ListenAddr    string `json:"listen_addr"` // default accepting incoming (first network)

	// Check compatibility.
	// Channels are HexBytes so easier to read as JSON
	Version  string            `json:"version"`  // major.minor.revision
	Channels cmtbytes.HexBytes `json:"channels"` // channels this node knows about

	// ASCIIText fields
	Moniker string                   `json:"moniker"` // arbitrary moniker
	Other   p2p.DefaultNodeInfoOther `json:"other"`   // other application specific data
}

// ID returns the node's peer ID.
func (info MultiNetworkNodeInfo) ID() p2p.ID {
	return info.DefaultNodeID
}

// GetChannels returns the node's channels.
func (info MultiNetworkNodeInfo) GetChannels() cmtbytes.HexBytes {
	return info.Channels
}

// GetReplNodeInfo returns a [p2p.NodeInfo] instance by chain ID and scope hash.
func (info MultiNetworkNodeInfo) GetReplNodeInfo(chainID string, scopeHash string) p2p.DefaultNodeInfo {
	versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ScopedProtocolVersion) bool {
		return v.ScopeHash == scopeHash
	})

	networkPos := slices.IndexFunc(info.Networks, func(n ScopedChainInfo) bool {
		return n.ScopeHash == scopeHash
	})

	laddrPos := slices.IndexFunc(info.ListenAddrs, func(a ScopedListenAddr) bool {
		return a.ScopeHash == scopeHash
	})

	// Not being able to find a protocol version or network should never happen
	if versionPos < 0 || networkPos < 0 || laddrPos < 0 {
		panic(fmt.Sprintf("could not determine protocol version and network with scope hash %s ; Got %d versionPos and %d networkPos", scopeHash, versionPos, networkPos))
	}

	protocolVersion := info.ProtocolVersions[versionPos]
	laddr := info.ListenAddrs[laddrPos]

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(
			protocolVersion.P2P,
			protocolVersion.Block,
			protocolVersion.App,
		),
		DefaultNodeID: info.DefaultNodeID,
		Network:       chainID,
		Version:       info.Version,
		Channels: []byte{
			bc.BlocksyncChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			mempl.MempoolChannel,
			evidence.EvidenceChannel,
			statesync.SnapshotChannel, statesync.ChunkChannel,
		},
		Moniker: info.Moniker,
		Other:   info.Other,
	}

	nodeInfo.ListenAddr = laddr.ListenAddr
	err := nodeInfo.Validate()
	if err != nil {
		panic(fmt.Errorf("could not validate p2p node info: %w", err))
	}

	return nodeInfo
}

// Validate checks the self-reported MultiNetworkNodeInfo is safe.
// It returns an error if there are too many Channels, if there are
// any duplicate Channels, if the ListenAddr is malformed, or if the
// ListenAddr is a host name that can not be resolved to some IP.
func (info MultiNetworkNodeInfo) Validate() error {
	// ID is already validated.

	// Validate all P2P listen addresses
	for _, laddr := range info.ListenAddrs {
		_, err := p2p.NewNetAddressString(p2p.IDAddressString(info.ID(), laddr.ListenAddr))
		if err != nil {
			return err
		}
	}

	// Network is validated in CompatibleWith.

	// Validate Version
	if len(info.Version) > 0 &&
		(!cmtstrings.IsASCIIText(info.Version) || cmtstrings.ASCIITrim(info.Version) == "") {
		return fmt.Errorf("info.Version must be valid ASCII text without tabs, but got %v", info.Version)
	}

	// Validate Channels - ensure max and check for duplicates.
	if len(info.Channels) > p2p.MaxNumChannels() {
		return fmt.Errorf("info.Channels is too long (%v). Max is %v", len(info.Channels), p2p.MaxNumChannels())
	}
	channels := make(map[byte]struct{})
	for _, ch := range info.Channels {
		_, ok := channels[ch]
		if ok {
			return fmt.Errorf("info.Channels contains duplicate channel id %v", ch)
		}
		channels[ch] = struct{}{}
	}

	// Validate Moniker.
	if !cmtstrings.IsASCIIText(info.Moniker) || cmtstrings.ASCIITrim(info.Moniker) == "" {
		return fmt.Errorf("info.Moniker must be valid non-empty ASCII text without tabs, but got %v", info.Moniker)
	}

	// Validate Other.
	other := info.Other
	txIndex := other.TxIndex
	switch txIndex {
	case "", "on", "off":
	default:
		return fmt.Errorf("info.Other.TxIndex should be either 'on', 'off', or empty string, got '%v'", txIndex)
	}

	// Validate RPC addresses
	for _, scopedRPCAddr := range info.RPCAddresses {
		rpcAddr := scopedRPCAddr.ListenAddr

		// TODO: Should we be more strict about address formats?
		if len(rpcAddr) > 0 && (!cmtstrings.IsASCIIText(rpcAddr) || cmtstrings.ASCIITrim(rpcAddr) == "") {
			return fmt.Errorf("info.Other.RPCAddress=%v must be valid ASCII text without tabs", rpcAddr)
		}
	}

	return nil
}

// CompatibleWith checks if two DefaultNodeInfo are compatible with each other.
// CONTRACT: two nodes are compatible if the Block version and network match
// and they have at least one channel in common.
func (info MultiNetworkNodeInfo) CompatibleWith(otherInfo p2p.NodeInfo) error {
	other, ok := otherInfo.(MultiNetworkNodeInfo)
	if !ok {
		return fmt.Errorf("wrong NodeInfo type. Expected DefaultNodeInfo, got %v", reflect.TypeOf(otherInfo))
	}

	haveCommonReplicatedChain := false
	for _, otherProtocolVersion := range other.ProtocolVersions {
		otherScopeHash := otherProtocolVersion.ScopeHash
		versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ScopedProtocolVersion) bool {
			return v.ScopeHash == otherScopeHash
		})

		// Not having *all* the same replicated chains is allowed
		if versionPos < 0 {
			continue
		}

		localProtocolVersion := info.ProtocolVersions[versionPos]
		if localProtocolVersion.Block != otherProtocolVersion.Block {
			// nodes must be share a block version
			return fmt.Errorf("peer is on a different Block version for scope hash %s. Got %v, expected %v",
				localProtocolVersion.ScopeHash, otherProtocolVersion.Block, localProtocolVersion.Block)
		}

		// Make sure we also have the network information such as ChainID
		networkPos := slices.IndexFunc(info.Networks, func(v ScopedChainInfo) bool {
			return v.ScopeHash == otherScopeHash
		})

		// Not having *all* the same replicated chains is allowed
		if networkPos < 0 {
			continue
		}

		haveCommonReplicatedChain = true
	}

	// nodes must share at least one replicated chain
	if !haveCommonReplicatedChain {
		return errors.New("peer does not have at least one replicated chain in common")
	}

	// if we have no channels, we're just testing
	if len(info.Channels) == 0 {
		return nil
	}

	// for each of our channels, check if they have it
	found := false
OUTER_LOOP:
	for _, ch1 := range info.Channels {
		for _, ch2 := range other.Channels {
			if ch1 == ch2 {
				found = true
				break OUTER_LOOP // only need one
			}
		}
	}
	if !found {
		return fmt.Errorf("peer has no common channels. Our channels: %v ; Peer channels: %v", info.Channels, other.Channels)
	}
	return nil
}

// NetAddress returns a NetAddress derived from the MultiNetworkNodeInfo -
// it includes the authenticated peer ID and the self-reported
// ListenAddr. Note that the ListenAddr is not authenticated and
// may not match that address actually dialed if its an outbound peer.
func (info MultiNetworkNodeInfo) NetAddress() (*p2p.NetAddress, error) {
	idAddr := p2p.IDAddressString(info.ID(), info.ListenAddr)
	return p2p.NewNetAddressString(idAddr)
}

func (info MultiNetworkNodeInfo) HasChannel(chID byte) bool {
	return bytes.Contains(info.Channels, []byte{chID})
}

func (info MultiNetworkNodeInfo) ToProto() *mxp2p.MultiNetworkNodeInfo {

	numReplicatedChains := len(info.Scopes)
	numVersions := len(info.ProtocolVersions)
	numNetworks := len(info.Networks)

	// Mismatch in sizes should never happen here
	if numReplicatedChains != numVersions || numReplicatedChains != numNetworks {
		panic(fmt.Sprintf("number of replicated chains inconsistent with protocol versions and networks, Got %d versions, %d networks and %d scopes", numVersions, numNetworks, numReplicatedChains))
	}

	dni := new(mxp2p.MultiNetworkNodeInfo)
	dni.Scopes = make([]string, numReplicatedChains)
	dni.ProtocolVersions = make([]*mxp2p.ScopedProtocolVersion, numReplicatedChains)
	dni.Networks = make([]*mxp2p.ScopedChainInfo, numReplicatedChains)
	dni.ListenAddrs = make([]*mxp2p.ScopedListenAddr, numReplicatedChains)
	dni.RPCAddresses = make([]*mxp2p.ScopedListenAddr, numReplicatedChains)

	for i, userScopeHash := range info.Scopes {

		versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ScopedProtocolVersion) bool {
			return v.ScopeHash == userScopeHash
		})

		networkPos := slices.IndexFunc(info.Networks, func(n ScopedChainInfo) bool {
			return n.ScopeHash == userScopeHash
		})

		laddrPos := slices.IndexFunc(info.ListenAddrs, func(a ScopedListenAddr) bool {
			return a.ScopeHash == userScopeHash
		})

		rpcAddrPos := slices.IndexFunc(info.RPCAddresses, func(a ScopedListenAddr) bool {
			return a.ScopeHash == userScopeHash
		})

		// Not being able to find a protocol version or network should never happen
		if versionPos < 0 || networkPos < 0 || laddrPos < 0 {
			panic(fmt.Sprintf("could not determine protocol version and network with scope hash %s ; Got %d versionPos and %d networkPos", userScopeHash, versionPos, networkPos))
		}

		protocolVersion := info.ProtocolVersions[versionPos]
		network := info.Networks[networkPos]
		laddr := info.ListenAddrs[laddrPos]
		rpcAddr := info.RPCAddresses[rpcAddrPos]

		dni.Scopes[i] = userScopeHash

		dni.ProtocolVersions[i] = &mxp2p.ScopedProtocolVersion{
			ScopeHash: userScopeHash,
			ProtocolVersion: &tmp2p.ProtocolVersion{
				P2P:   protocolVersion.P2P,
				Block: protocolVersion.Block,
				App:   protocolVersion.App,
			},
		}

		dni.Networks[i] = &mxp2p.ScopedChainInfo{
			ScopeHash: userScopeHash,
			ChainID:   network.ChainID,
		}

		dni.ListenAddrs[i] = &mxp2p.ScopedListenAddr{
			ScopeHash:  userScopeHash,
			ListenAddr: laddr.ListenAddr,
		}

		dni.RPCAddresses[i] = &mxp2p.ScopedListenAddr{
			ScopeHash:  userScopeHash,
			ListenAddr: rpcAddr.ListenAddr,
		}
	}

	dni.DefaultNodeID = string(info.DefaultNodeID)
	dni.ListenAddr = info.ListenAddr
	dni.Version = info.Version
	dni.Channels = info.Channels
	dni.Moniker = info.Moniker
	dni.Other = tmp2p.DefaultNodeInfoOther{
		TxIndex:    info.Other.TxIndex,
		RPCAddress: info.Other.RPCAddress,
	}

	return dni
}

func MultiNetworkNodeInfoFromToProto(pb *mxp2p.MultiNetworkNodeInfo) (MultiNetworkNodeInfo, error) {
	if pb == nil {
		return MultiNetworkNodeInfo{}, errors.New("nil node info")
	}

	scopes := make([]string, len(pb.Scopes))
	protocolVersions := make([]ScopedProtocolVersion, len(pb.ProtocolVersions))
	networks := make([]ScopedChainInfo, len(pb.Networks))
	listenAddrs := make([]ScopedListenAddr, len(pb.ListenAddrs))
	rpcAddresses := make([]ScopedListenAddr, len(pb.RPCAddresses))

	for i, scope := range pb.Scopes {
		scopes[i] = scope
	}

	for i, pv := range pb.ProtocolVersions {
		protocolVersions[i] = ScopedProtocolVersion{
			ScopeHash: pv.ScopeHash,
			P2P:       pv.ProtocolVersion.P2P,
			Block:     pv.ProtocolVersion.Block,
			App:       pv.ProtocolVersion.App,
		}
	}

	for i, n := range pb.Networks {
		networks[i] = ScopedChainInfo{
			ScopeHash: n.ScopeHash,
			ChainID:   n.ChainID,
		}
	}

	for i, laddr := range pb.ListenAddrs {
		listenAddrs[i] = ScopedListenAddr{
			ScopeHash:  laddr.ScopeHash,
			ListenAddr: laddr.ListenAddr,
		}
	}

	for i, raddr := range pb.RPCAddresses {
		rpcAddresses[i] = ScopedListenAddr{
			ScopeHash:  raddr.ScopeHash,
			ListenAddr: raddr.ListenAddr,
		}
	}

	dni := MultiNetworkNodeInfo{
		Scopes:           scopes,
		ProtocolVersions: protocolVersions,
		Networks:         networks,
		ListenAddrs:      listenAddrs,
		RPCAddresses:     rpcAddresses,
		DefaultNodeID:    p2p.ID(pb.DefaultNodeID),
		ListenAddr:       pb.ListenAddr,
		Version:          pb.Version,
		Channels:         pb.Channels,
		Moniker:          pb.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    pb.Other.TxIndex,
			RPCAddress: pb.Other.RPCAddress,
		},
	}

	return dni, nil
}
