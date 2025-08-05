package p2p

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	"github.com/ice-blockchain/cometbft/config"
	cmtstrings "github.com/ice-blockchain/cometbft/internal/strings"
	cmtbytes "github.com/ice-blockchain/cometbft/libs/bytes"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/version"
)

const (
	// NOTE(midas): We removed the Networks listing from exported fields in
	// [MultiNetworkNodeInfo], so we don't need to allow for bigger messages.
	maxNodeInfoSize = 512 // 512 bytes

	// NOTE(midas): Multiplex implementation adds 3 channels.
	maxNumChannels = 16 // currently using 12 for CometBFT
)

// DefaultProtocolVersion populates the Block and P2P versions using
// the global values, but not the App.
var DefaultProtocolVersion = cmtp2p.NewProtocolVersion(
	version.P2PProtocol,
	version.BlockProtocol,
	0,
)

// MultiNetworkNodeInfo is a multiplex node information exchanged
// between two peers during the CometBFT P2P handshake.
type MultiNetworkNodeInfo struct {
	mtx      sync.Mutex
	chainIds map[string]bool

	// Authenticate
	DefaultNodeID cmtp2p.ID `json:"id"`          // authenticated identifier
	ListenAddr    string    `json:"listen_addr"` // default accepting incoming (first network)

	// Check compatibility.
	// Channels are HexBytes so easier to read as JSON
	Version         string                 `json:"version"` // major.minor.revision
	ProtocolVersion cmtp2p.ProtocolVersion `json:"protocol_version"`
	Channels        cmtbytes.HexBytes      `json:"channels"` // channels this node knows about

	// ASCIIText fields
	Moniker string                      `json:"moniker"` // arbitrary moniker
	Other   cmtp2p.DefaultNodeInfoOther `json:"other"`   // other application specific data
}

// Assert MultiNetworkNodeInfo satisfies NodeInfo.
var _ cmtp2p.NodeInfo = (*MultiNetworkNodeInfo)(nil)

// NewMultiNetworkNodeInfoWithConfig creates a new instance around a nodeCfg, a nodeKey
// and a listenAddr.
// Note that the liveness of listenAddr is not validated here.
func NewMultiNetworkNodeInfoWithConfig(
	nodeCfg *config.Config,
	nodeKey *cmtp2p.NodeKey,
	listenAddr *cmtp2p.NetAddress,
	channels []byte,
) *MultiNetworkNodeInfo {
	chainIds := map[string]bool{}
	for _, userChains := range nodeCfg.UserChains {
		for _, chainID := range userChains {
			chainIds[chainID] = true
		}
	}

	return &MultiNetworkNodeInfo{
		chainIds:        chainIds,
		DefaultNodeID:   nodeKey.ID(),
		Version:         nodeCfg.Version,
		ProtocolVersion: DefaultProtocolVersion,
		Moniker:         nodeCfg.Moniker,
		ListenAddr:      listenAddr.DialString(),
		Channels:        channels,
		Other: cmtp2p.DefaultNodeInfoOther{
			TxIndex:    "on",
			RPCAddress: nodeCfg.RPC.ListenAddress,
		},
	}
}

func NewMultiNetworkNodeInfo() *MultiNetworkNodeInfo {
	return &MultiNetworkNodeInfo{
		chainIds: map[string]bool{},
	}
}

// NodeInfoWithListenAddress defines an option helper to customize the ListenAddr
// of a MultiNetworkNodeInfo instance.
func NodeInfoWithListenAddress(
	listenAddr *cmtp2p.NetAddress,
) func(*MultiNetworkNodeInfo) {
	return func(nodeInfo *MultiNetworkNodeInfo) {
		nodeInfo.ListenAddr = listenAddr.DialString()
	}
}

// ----------------------------------------------------------------------------
// MultiNetworkNodeInfo implements cmtp2p.NodeInfo

// ID returns the node's peer ID.
func (info *MultiNetworkNodeInfo) ID() cmtp2p.ID {
	info.mtx.Lock()
	defer info.mtx.Unlock()

	return info.DefaultNodeID
}

// GetChannels returns the node's channels.
func (info *MultiNetworkNodeInfo) GetChannels() cmtbytes.HexBytes {
	info.mtx.Lock()
	defer info.mtx.Unlock()

	return info.Channels
}

// SetNetworks sets the node's channels.
func (info *MultiNetworkNodeInfo) AddNetworks(networks []string) {
	info.mtx.Lock()
	defer info.mtx.Unlock()

	for _, chainID := range networks {
		if _, ok := info.chainIds[chainID]; !ok {
			info.chainIds[chainID] = true
		}
	}
}

// GetNodeInfo returns a [cmtp2p.NodeInfo] instance by chainID.
func (info *MultiNetworkNodeInfo) GetNodeInfo(chainID string) (cmtp2p.DefaultNodeInfo, error) {
	info.mtx.Lock()
	defer info.mtx.Unlock()

	nodeInfo := cmtp2p.DefaultNodeInfo{
		ProtocolVersion: cmtp2p.NewProtocolVersion(
			info.ProtocolVersion.P2P,
			info.ProtocolVersion.Block,
			info.ProtocolVersion.App,
		),
		DefaultNodeID: info.DefaultNodeID,
		Network:       chainID,
		Version:       info.Version,
		Channels:      GetChannelIds(GetRuntimeChannels()),
		Moniker:       info.Moniker,
		Other:         info.Other,
	}

	nodeInfo.ListenAddr = info.ListenAddr
	err := nodeInfo.Validate()
	if err != nil {
		return cmtp2p.DefaultNodeInfo{}, fmt.Errorf(
			"could not validate p2p node info: %w", err)
	}

	return nodeInfo, nil
}

// Validate checks the self-reported MultiNetworkNodeInfo is safe.
// It returns an error if there are too many Channels, if there are
// any duplicate Channels, if the ListenAddr is malformed, or if the
// ListenAddr is a host name that can not be resolved to some IP.
func (info *MultiNetworkNodeInfo) Validate() error {
	// ID is already validated.

	info.mtx.Lock()
	defer info.mtx.Unlock()

	// Validate P2P listen address
	if len(info.ListenAddr) > 0 && info.ListenAddr != cmtp2p.EmptyNetAddress {
		if _, err := cmtp2p.NewNetAddressString(cmtp2p.IDAddressString(info.DefaultNodeID, info.ListenAddr)); err != nil {
			return err
		}
	}

	// Validate Version
	if len(info.Version) > 0 &&
		(!cmtstrings.IsASCIIText(info.Version) || cmtstrings.ASCIITrim(info.Version) == "") {
		return fmt.Errorf("info.Version must be valid ASCII text without tabs, but got %v", info.Version)
	}

	// Validate Channels - ensure max and check for duplicates.
	if len(info.Channels) > cmtp2p.MaxNumChannels() {
		return fmt.Errorf("info.Channels is too long (%v). Max is %v", len(info.Channels), cmtp2p.MaxNumChannels())
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

	return nil
}

// CompatibleWith checks if two DefaultNodeInfo are compatible with each other.
//
// This implementation of CompatibleWith verifies that the Block versions match.
//
// CONTRACT: two nodes are compatible if the Block version match and they
// have at least one channel in common.
func (info *MultiNetworkNodeInfo) CompatibleWith(otherInfo cmtp2p.NodeInfo) error {
	other, ok := otherInfo.(*MultiNetworkNodeInfo)
	if !ok {
		return fmt.Errorf(
			"wrong NodeInfo type. Expected MultiNetworkNodeInfo, got %v", reflect.TypeOf(otherInfo))
	}

	info.mtx.Lock()
	defer info.mtx.Unlock()

	// Block versions for one ChainID must be the same on both nodes
	if other.ProtocolVersion.Block != info.ProtocolVersion.Block {
		// nodes must share a block version
		return fmt.Errorf("peer is on a different Block version. Got %v, expected %v",
			other.ProtocolVersion.Block, info.ProtocolVersion.Block)
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
func (info *MultiNetworkNodeInfo) NetAddress() (*cmtp2p.NetAddress, error) {
	idAddr := cmtp2p.IDAddressString(info.ID(), info.ListenAddr)
	return cmtp2p.NewNetAddressString(idAddr)
}

func (info *MultiNetworkNodeInfo) HasChannel(chID byte) bool {
	info.mtx.Lock()
	defer info.mtx.Unlock()

	return bytes.Contains(info.Channels, []byte{chID})
}

func (info *MultiNetworkNodeInfo) ToProto() (*mxp2p.MultiNetworkNodeInfo, error) {
	info.mtx.Lock()
	defer info.mtx.Unlock()

	dni := new(mxp2p.MultiNetworkNodeInfo)
	dni.ProtocolVersion = &tmp2p.ProtocolVersion{
		P2P:   info.ProtocolVersion.P2P,
		Block: info.ProtocolVersion.Block,
		App:   info.ProtocolVersion.App,
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

	return dni, nil
}

func MultiNetworkNodeInfoFromProto(pb *mxp2p.MultiNetworkNodeInfo) (*MultiNetworkNodeInfo, error) {
	if pb == nil {
		return nil, errors.New("nil node info")
	}

	dni := &MultiNetworkNodeInfo{
		DefaultNodeID: cmtp2p.ID(pb.DefaultNodeID),
		ListenAddr:    pb.ListenAddr,
		Version:       pb.Version,
		ProtocolVersion: cmtp2p.NewProtocolVersion(
			pb.ProtocolVersion.P2P,
			pb.ProtocolVersion.Block,
			pb.ProtocolVersion.App,
		),
		Channels: pb.Channels,
		Moniker:  pb.Moniker,
		Other: cmtp2p.DefaultNodeInfoOther{
			TxIndex:    pb.Other.TxIndex,
			RPCAddress: pb.Other.RPCAddress,
		},
	}

	return dni, nil
}
