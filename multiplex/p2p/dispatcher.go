package p2p

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/cosmos/gogoproto/proto"

	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	cmtconn "github.com/ice-blockchain/cometbft/p2p/conn"
	cmttypes "github.com/ice-blockchain/cometbft/types"

	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// packetDispatcher defines a runtime composer.
type packetDispatcher struct {
	mtx *sync.Mutex

	resourceMgr types.ResourceManager

	multiplexReactor    cmtp2p.Reactor
	reactorsByChIds     map[byte]string
	reactorsServiceKeys map[string]string
	channelsIndex       map[byte]*cmtp2p.Channel

	initialized uint32 // atomic

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ cmtp2p.Dispatcher = (*packetDispatcher)(nil)
var _ cmtp2p.ChannelProvider = (*packetDispatcher)(nil)

type DispatcherOption func(*packetDispatcher)

// NewDispatcher creates a new database service.
func NewDispatcher(
	ctx context.Context,
	nodeInfo *MultiNetworkNodeInfo,
	resourceMgr types.ResourceManager,
	logger cmtlog.Logger,
	options ...DispatcherOption,
) cmtp2p.Dispatcher {
	router := &packetDispatcher{
		mtx: new(sync.Mutex),

		resourceMgr:     resourceMgr,
		reactorsByChIds: map[byte]string{},
		reactorsServiceKeys: map[string]string{
			"BLOCKSYNC": types.ServiceKeyBlockSyncReactor,
			"CONSENSUS": types.ServiceKeyConsensusReactor,
			"EVIDENCE":  types.ServiceKeyEvidenceReactor,
			"MEMPOOL":   types.ServiceKeyMempoolReactor,
			"PEX":       types.ServiceKeyAddressesReactor,
		},
		channelsIndex: map[byte]*cmtp2p.Channel{},

		// Options
		logger: logger,
	}

	atomic.StoreUint32(&router.initialized, 0)

	// Use option helpers
	router.SetOptions(options...)

	for reactor, channelIds := range GetRuntimeChannels() {
		for _, chID := range channelIds {
			router.reactorsByChIds[chID] = reactor
		}
	}

	return router
}

// DispatcherWithLogger injects a custom logger instance.
func DispatcherWithLogger(logger cmtlog.Logger) DispatcherOption {
	return func(router *packetDispatcher) {
		router.logger = logger
	}
}

// ----------------------------------------------------------------------------
// cmtp2p.Dispatcher API implementation

// Target returns the target reactor to process packet.
func (router *packetDispatcher) Target(packet tmp2p.PacketMsg) cmtp2p.Reactor {
	router.mtx.Lock()
	// Uses a static list of reactors names
	name := router.reactorsByChIds[byte(packet.ChannelID)]
	router.mtx.Unlock()

	if name == "MULTIPLEX" {
		return router.GetMultiplexReactor()
	}

	// Populated in NewDispatcher().
	skey := router.reactorsServiceKeys[name]

	if !router.resourceMgr.Has(packet.ChainID, skey) {
		return nil
	}

	// Get the service instance from resources.
	target := router.resourceMgr.Get(
		packet.ChainID,
		skey,
	).(cmtp2p.Reactor)
	return target
}

// Dispatch forwards the packet to the target reactor.
func (router *packetDispatcher) Dispatch(
	sourcePeer *cmtp2p.PeerImpl,
	packet tmp2p.PacketMsg,
) (err error) {
	// Get the packet's target reactor.
	target := router.Target(packet)
	if target == nil {
		router.logger.Error("failed to find target for message",
			"msg", packet.Data)
		err = fmt.Errorf("[CAUTION] ignoring message; too early, retry later")
		return
	}

	// Find the correct proto message type.
	chDescs := target.GetChannels()

	var msgType proto.Message
	for _, chDesc := range chDescs {
		if chDesc.ID != byte(packet.ChannelID) {
			continue
		}

		msgType = chDesc.MessageType
		break
	}

	// Unmarshal the packet data and unwrap before dispatching.
	msg := proto.Clone(msgType)
	if err = proto.Unmarshal(packet.Data, msg); err != nil {
		err = fmt.Errorf("failed to unmarshal message: %v into type: %s",
			err, reflect.TypeOf(msgType),
		)
		return
	}
	if w, ok := msg.(cmttypes.Unwrapper); ok {
		if msg, err = w.Unwrap(); err != nil {
			err = fmt.Errorf("failed to unwrap message: %w", err)
			return
		}
	}

	// TODO(midas): remove debug logs.
	router.logger.Debug("packetDispatcher#Dispatch; dispatching...",
		"target", target,
		"msg", msg,
	)

	target.Receive(cmtp2p.Envelope{
		ChainID:   packet.ChainID,
		ChannelID: byte(packet.ChannelID),
		Src:       sourcePeer,
		Message:   msg,
	})
	return
}

// Reactors returns reactors for chainID by name.
func (router *packetDispatcher) Reactors(chainID string) map[string]cmtp2p.Reactor {
	router.mtx.Lock()
	defer router.mtx.Unlock()

	reactors := make(map[string]cmtp2p.Reactor, len(router.reactorsServiceKeys))
	for name, serviceKey := range router.reactorsServiceKeys {
		reactors[name] = router.resourceMgr.Get(
			chainID,
			serviceKey,
		).(cmtp2p.Reactor)
	}

	return reactors
}

// Reactor returns a reactor for chainID by name.
func (router *packetDispatcher) Reactor(chainID string, name string) cmtp2p.Reactor {
	reactors := router.Reactors(chainID)

	if r, ok := reactors[name]; ok {
		return r
	}

	return nil
}

// SetMultiplexReactor sets the multiplex reactor.
func (router *packetDispatcher) SetMultiplexReactor(mxR cmtp2p.Reactor) {
	router.mtx.Lock()
	defer router.mtx.Unlock()

	router.multiplexReactor = mxR
}

// GetMultiplexReactor returns the multiplex reactor.
func (router *packetDispatcher) GetMultiplexReactor() cmtp2p.Reactor {
	router.mtx.Lock()
	defer router.mtx.Unlock()

	return router.multiplexReactor
}

// ----------------------------------------------------------------------------
// cmtp2p.ChannelProvider API implementation

// InitChannels initializes the channels index for this dispatcher.
func (router *packetDispatcher) InitChannels() {
	if atomic.CompareAndSwapUint32(&router.initialized, 0, 1) {
		mxR := router.GetMultiplexReactor()
		chDescs := GetChannelDescriptors(mxR)
		router.logger.Info("InitChannels", "len", len(chDescs))

		router.mtx.Lock()
		defer router.mtx.Unlock()

		router.channelsIndex = make(map[byte]*cmtp2p.Channel, len(chDescs))
		for chID, chDesc := range chDescs {
			router.channelsIndex[chID] = cmtconn.NewChannel(chDesc)
		}
	}
}

// GetChannels returns a slice of Channel instances.
func (router *packetDispatcher) GetChannels() (channels []*cmtp2p.Channel) {
	router.mtx.Lock()
	channelsIdx := router.channelsIndex
	router.mtx.Unlock()

	channels = make([]*cmtp2p.Channel, len(channelsIdx))
	for _, channel := range channelsIdx {
		channels = append(channels, channel)
	}
	return // channels
}

// GetChannel returns a Channel by ID.
func (router *packetDispatcher) GetChannel(chID byte) *cmtp2p.Channel {
	router.mtx.Lock()
	defer router.mtx.Unlock()

	if len(router.channelsIndex) == 0 {
		return nil
	}

	if _, ok := router.channelsIndex[chID]; !ok {
		return nil
	}

	return router.channelsIndex[chID]
}

// GetDescriptor returns a ChannelDescriptor by ID.
func (router *packetDispatcher) GetDescriptor(chID byte) *cmtp2p.ChannelDescriptor {
	router.mtx.Lock()
	defer router.mtx.Unlock()

	if len(router.channelsIndex) == 0 {
		return nil
	}

	if _, ok := router.channelsIndex[chID]; !ok {
		return nil
	}

	return router.channelsIndex[chID].Desc()
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (router *packetDispatcher) SetOptions(options ...DispatcherOption) {
	for _, option := range options {
		option(router)
	}
}

// Logger returns the logger instance.
func (router *packetDispatcher) Logger() cmtlog.Logger {
	return router.logger
}
