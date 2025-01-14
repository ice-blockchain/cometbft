package multiplex

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	"github.com/ice-blockchain/cometbft/config"
	bc "github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtstrings "github.com/ice-blockchain/cometbft/internal/strings"
	cmtbytes "github.com/ice-blockchain/cometbft/libs/bytes"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	"github.com/ice-blockchain/cometbft/statesync"
	"github.com/ice-blockchain/cometbft/version"
)

// DefaultProtocolVersion populates the Block and P2P versions using
// the global values, but not the App.
var DefaultProtocolVersion = p2p.NewProtocolVersion(
	version.P2PProtocol,
	version.BlockProtocol,
	0,
)

// ChainProtocolVersion contains a ChainID and protocol versions for the software.
type ChainProtocolVersion struct {
	ChainID string `json:"chain_id"`
	P2P     uint64 `json:"p2p"`
	Block   uint64 `json:"block"`
	App     uint64 `json:"app"`
}

// NewChainProtocolVersion creates a [ChainProtocolVersion] from
// a ChainID and a legacy [p2p.ProtocolVersion].
func NewChainProtocolVersion(chainID string, ver p2p.ProtocolVersion) ChainProtocolVersion {
	return ChainProtocolVersion{
		ChainID: chainID,
		P2P:     ver.P2P,
		Block:   ver.Block,
		App:     ver.App,
	}
}

// ChainListenAddr contains a ChainID and a listen address.
type ChainListenAddr struct {
	ChainID    string `json:"chain_id"`
	ListenAddr string `json:"listen_addr"`
}

// NewChainListenAddr wraps a listen address for a ChainID.
func NewChainListenAddr(chainID string, laddr string) ChainListenAddr {
	return ChainListenAddr{
		ChainID:    chainID,
		ListenAddr: laddr,
	}
}

// MultiNetworkNodeInfo is a multiplex node information exchanged
// between two peers during the CometBFT P2P handshake.
type MultiNetworkNodeInfo struct {
	// Replication configuration
	Networks         []string               `json:"networks"` // contains ChainIDs
	ProtocolVersions []ChainProtocolVersion `json:"protocol_versions"`
	ListenAddrs      []ChainListenAddr      `json:"listen_addrs"` // accepting incoming
	RPCAddresses     []ChainListenAddr      `json:"rpc_addrs"`    // accepting RPC

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

// Assert MultiNetworkNodeInfo satisfies NodeInfo.
var _ p2p.NodeInfo = MultiNetworkNodeInfo{}

// NewMultiNetworkNodeInfo creates a new instance around a nodeCfg, a nodeKey
// and a listenAddr.
// Note that the liveness of listenAddr is not validated here.
func NewMultiNetworkNodeInfo(
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
	listenAddr *p2p.NetAddress,
) *MultiNetworkNodeInfo {
	return &MultiNetworkNodeInfo{
		Networks:         []string{},
		ProtocolVersions: []ChainProtocolVersion{},
		ListenAddrs:      []ChainListenAddr{},
		RPCAddresses:     []ChainListenAddr{},
		DefaultNodeID:    nodeKey.ID(),
		Version:          nodeCfg.Version,
		Moniker:          nodeCfg.Moniker,
		ListenAddr:       listenAddr.DialString(),
		Channels:         []byte{ReplicationChannel},
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "off",
			RPCAddress: "",
		},
	}
}

// ID returns the node's peer ID.
func (info MultiNetworkNodeInfo) ID() p2p.ID {
	return info.DefaultNodeID
}

// GetChannels returns the node's channels.
func (info MultiNetworkNodeInfo) GetChannels() cmtbytes.HexBytes {
	return info.Channels
}

// GetNodeInfo returns a [p2p.NodeInfo] instance by chain ID.
func (info MultiNetworkNodeInfo) GetNodeInfo(chainID string) p2p.DefaultNodeInfo {
	versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ChainProtocolVersion) bool {
		return v.ChainID == chainID
	})

	networkPos := slices.IndexFunc(info.Networks, func(n string) bool {
		return n == chainID
	})

	laddrPos := slices.IndexFunc(info.ListenAddrs, func(a ChainListenAddr) bool {
		return a.ChainID == chainID
	})

	// Not finding a protocol version, network or listen address should never happen
	if versionPos < 0 || networkPos < 0 || laddrPos < 0 {
		panic(fmt.Sprintf("could not determine version and listen address for ChainID %s", chainID)) //nolint:perfsprint
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
			pex.PexChannel,
			ReplicationChannel,
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
	if len(info.ListenAddr) > 0 && info.ListenAddr != p2p.EmptyNetAddress {
		if _, err := p2p.NewNetAddressString(p2p.IDAddressString(info.ID(), info.ListenAddr)); err != nil {
			return err
		}
		for _, laddr := range info.ListenAddrs {
			_, err := p2p.NewNetAddressString(p2p.IDAddressString(info.ID(), laddr.ListenAddr))
			if err != nil {
				return err
			}
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
	rpcAddr := info.Other.RPCAddress
	if len(rpcAddr) > 0 && (!cmtstrings.IsASCIIText(rpcAddr) || cmtstrings.ASCIITrim(rpcAddr) == "") {
		return fmt.Errorf("info.Other.RPCAddress=%v must be valid ASCII text without tabs", rpcAddr)
	}
	for _, chainRPC := range info.RPCAddresses {
		rpcAddr = chainRPC.ListenAddr

		// TODO: Should we be more strict about address formats?
		if len(rpcAddr) > 0 && (!cmtstrings.IsASCIIText(rpcAddr) || cmtstrings.ASCIITrim(rpcAddr) == "") {
			return fmt.Errorf("info.Other.RPCAddress=%v must be valid ASCII text without tabs", rpcAddr)
		}
	}

	return nil
}

// CompatibleWith checks if two DefaultNodeInfo are compatible with each other.
//
// This implementation of CompatibleWith verifies that at least one of the
// replicated chains is compatible with the other peer's replicated chains.
//
// CONTRACT: two nodes are compatible if the Block version and network match
// and they have at least one channel in common.
func (info MultiNetworkNodeInfo) CompatibleWith(otherInfo p2p.NodeInfo) error {
	other, ok := otherInfo.(MultiNetworkNodeInfo)
	if !ok {
		return fmt.Errorf("wrong NodeInfo type. Expected DefaultNodeInfo, got %v", reflect.TypeOf(otherInfo))
	}

	haveCommonReplicatedChain := false
	for _, otherProtocolVersion := range other.ProtocolVersions {
		otherChainID := otherProtocolVersion.ChainID
		versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ChainProtocolVersion) bool {
			return v.ChainID == otherChainID
		})

		// Not having *all* the same replicated chains is allowed
		if versionPos < 0 {
			continue
		}

		localProtocolVersion := info.ProtocolVersions[versionPos]

		// Block versions must be the same on both nodes
		if localProtocolVersion.Block != otherProtocolVersion.Block {
			// nodes must share a block version
			return fmt.Errorf("peer is on a different Block version for ChainID %s. Got %v, expected %v",
				localProtocolVersion.ChainID, otherProtocolVersion.Block, localProtocolVersion.Block)
		}

		// Make sure we also have the network information such as ChainID
		networkPos := slices.IndexFunc(info.Networks, func(v string) bool {
			return v == otherChainID
		})

		// Not having *all* the same replicated chains is allowed
		if networkPos < 0 {
			continue
		}

		haveCommonReplicatedChain = true
	}

	// nodes must share at least one replicated chain
	if !haveCommonReplicatedChain && len(info.ProtocolVersions) > 0 {
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
	numReplicatedChains := len(info.Networks)
	numVersions := len(info.ProtocolVersions)

	// Mismatch in sizes should never happen here
	if numReplicatedChains != numVersions {
		panic(fmt.Sprintf("found inconsistent number of replicated chains, got %d networks and %d versions",
			numReplicatedChains, numVersions))
	}

	dni := new(mxp2p.MultiNetworkNodeInfo)
	dni.Networks = make([]string, numReplicatedChains)
	dni.ProtocolVersions = make([]*mxp2p.ChainProtocolVersion, numReplicatedChains)
	dni.ListenAddrs = make([]*mxp2p.ChainListenAddr, numReplicatedChains)
	dni.RPCAddresses = make([]*mxp2p.ChainListenAddr, numReplicatedChains)

	for i, userChainID := range info.Networks {
		versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ChainProtocolVersion) bool {
			return v.ChainID == userChainID
		})

		networkPos := slices.IndexFunc(info.Networks, func(n string) bool {
			return n == userChainID
		})

		laddrPos := slices.IndexFunc(info.ListenAddrs, func(a ChainListenAddr) bool {
			return a.ChainID == userChainID
		})

		rpcAddrPos := slices.IndexFunc(info.RPCAddresses, func(a ChainListenAddr) bool {
			return a.ChainID == userChainID
		})

		// Not being able to find a protocol version or network should never happen
		if versionPos < 0 || networkPos < 0 || laddrPos < 0 {
			panic(fmt.Sprintf("could not determine version and listen address for ChainID %s", userChainID)) //nolint:perfsprint
		}

		protocolVersion := info.ProtocolVersions[versionPos]
		laddr := info.ListenAddrs[laddrPos]
		rpcAddr := info.RPCAddresses[rpcAddrPos]

		dni.Networks[i] = userChainID

		dni.ProtocolVersions[i] = &mxp2p.ChainProtocolVersion{
			ChainID: userChainID,
			ProtocolVersion: &tmp2p.ProtocolVersion{
				P2P:   protocolVersion.P2P,
				Block: protocolVersion.Block,
				App:   protocolVersion.App,
			},
		}

		dni.ListenAddrs[i] = &mxp2p.ChainListenAddr{
			ChainID:    userChainID,
			ListenAddr: laddr.ListenAddr,
		}

		dni.RPCAddresses[i] = &mxp2p.ChainListenAddr{
			ChainID:    userChainID,
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

func MultiNetworkNodeInfoFromProto(pb *mxp2p.MultiNetworkNodeInfo) (MultiNetworkNodeInfo, error) {
	if pb == nil {
		return MultiNetworkNodeInfo{}, errors.New("nil node info")
	}

	networks := make([]string, len(pb.Networks))
	protocolVersions := make([]ChainProtocolVersion, len(pb.ProtocolVersions))
	listenAddrs := make([]ChainListenAddr, len(pb.ListenAddrs))
	rpcAddresses := make([]ChainListenAddr, len(pb.RPCAddresses))

	copy(networks, pb.Networks)

	for i, pv := range pb.ProtocolVersions {
		protocolVersions[i] = ChainProtocolVersion{
			ChainID: pv.ChainID,
			P2P:     pv.ProtocolVersion.P2P,
			Block:   pv.ProtocolVersion.Block,
			App:     pv.ProtocolVersion.App,
		}
	}

	for i, laddr := range pb.ListenAddrs {
		listenAddrs[i] = ChainListenAddr{
			ChainID:    laddr.ChainID,
			ListenAddr: laddr.ListenAddr,
		}
	}

	for i, raddr := range pb.RPCAddresses {
		rpcAddresses[i] = ChainListenAddr{
			ChainID:    raddr.ChainID,
			ListenAddr: raddr.ListenAddr,
		}
	}

	dni := MultiNetworkNodeInfo{
		Networks:         networks,
		ProtocolVersions: protocolVersions,
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

// WithListenAddress defines an option helper to customize the ListenAddr
// of a MultiNetworkNodeInfo instance.
func WithListenAddress(
	listenAddr *p2p.NetAddress,
) func(*MultiNetworkNodeInfo) {
	return func(nodeInfo *MultiNetworkNodeInfo) {
		nodeInfo.ListenAddr = listenAddr.DialString()
	}
}
