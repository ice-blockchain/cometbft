package conn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmos/gogoproto/proto"

	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	"github.com/ice-blockchain/cometbft/config"
	flow "github.com/ice-blockchain/cometbft/internal/flowrate"
	cmtrand "github.com/ice-blockchain/cometbft/internal/rand"
	"github.com/ice-blockchain/cometbft/internal/timer"
	"github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/protoio"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
)

const (
	defaultMaxPacketMsgPayloadSize = 1 * 1024 * 1024 // 1 MiB

	numBatchPacketMsgs = 10
	minReadBufferSize  = 1024
	minWriteBufferSize = 65536
	updateStats        = 2 * time.Second

	// some of these defaults are written in the user config
	// flushThrottle, sendRate, recvRate
	// TODO: remove values present in config.
	defaultFlushThrottle = 10 * time.Millisecond

	defaultSendQueueCapacity   = 100 // allows up to 100 queued send operations
	defaultRecvBufferCapacity  = 4096
	defaultRecvMessageCapacity = 22020096      // 21MB
	defaultSendRate            = int64(512000) // 500KB/s
	defaultRecvRate            = int64(512000) // 500KB/s
	defaultSendTimeout         = 10 * time.Second
	defaultPingInterval        = 60 * time.Second
	defaultPongTimeout         = 45 * time.Second

	SharedChannelsNamespace = "_shared_channels"
	TestChannel             = byte(0x90)
)

type (
	receiveCbFunc func(chainID string, chID byte, msgBytes []byte)
	errorCbFunc   func(any)
)

/*
Each peer has one `MConnection` (multiplex connection) instance.

__multiplex__ *noun* a system or signal involving simultaneous transmission of
several messages along a single channel of communication.

Each `MConnection` handles message transmission on multiple abstract communication
`Channel`s.  Each channel has a globally unique byte id.
The byte id and the relative priorities of each `Channel` are configured upon
initialization of the connection.

There are two methods for sending messages:

	func (m MConnection) Send(chID byte, msgBytes []byte) bool {}
	func (m MConnection) TrySend(chID byte, msgBytes []byte}) bool {}

`Send(chID, msgBytes)` is a blocking call that waits until `msg` is
successfully queued for the channel with the given id byte `chID`, or until the
request times out.  The message `msg` is serialized using Protobuf.

`TrySend(chID, msgBytes)` is a nonblocking call that returns false if the
channel's queue is full.

Inbound message bytes are handled with an onReceive callback function.
*/
type MConnection struct {
	service.BaseService

	connPeerId string

	// conn contains a SecretConnection.
	conn            net.Conn
	bufConnReader   *bufio.Reader
	bufConnWriter   *bufio.Writer
	channelProvider ChannelProvider

	// Callbacks
	onReceive receiveCbFunc
	onError   errorCbFunc

	// Monitors send and recv rates.
	sendMonitor *flow.Monitor
	recvMonitor *flow.Monitor

	// Internal channels
	send chan struct{}
	pong chan struct{}
	// channelsMtx   cmtsync.Mutex
	errored uint32
	config  MConnConfig

	// Closing quitSendRoutine will cause the sendRoutine to eventually quit.
	// doneSendRoutine is closed when the sendRoutine actually quits.
	quitSendRoutine chan struct{}
	doneSendRoutine chan struct{}

	// Closing quitRecvRouting will cause the recvRouting to eventually quit.
	quitRecvRoutine chan struct{}

	// used to ensure FlushStop and OnStop
	// are safe to call concurrently.
	stopMtx cmtsync.Mutex

	flushTimer *timer.ThrottleTimer // flush writes as necessary but throttled.
	pingTimer  *time.Ticker         // send pings periodically

	// close conn if pong is not received in pongTimeout
	pongTimer     *time.Timer
	pongTimeoutCh chan bool // true - timeout, false - peer sent pong

	created time.Time // time of creation

	_maxPacketMsgSize int

	numOpenChannels uint32 // atomic
	startedRoutines uint32 // atomic
	stoppedRoutines uint32 // atomic
}

// MConnConfig is a MConnection configuration.
type MConnConfig struct {
	SendRate int64 `mapstructure:"send_rate"`
	RecvRate int64 `mapstructure:"recv_rate"`

	// Maximum payload size
	MaxPacketMsgPayloadSize int `mapstructure:"max_packet_msg_payload_size"`

	// Interval to flush writes (throttled)
	FlushThrottle time.Duration `mapstructure:"flush_throttle"`

	// Interval to send pings
	PingInterval time.Duration `mapstructure:"ping_interval"`

	// Maximum wait time for pongs
	PongTimeout time.Duration `mapstructure:"pong_timeout"`

	// Fuzz connection
	TestFuzz       bool                   `mapstructure:"test_fuzz"`
	TestFuzzConfig *config.FuzzConnConfig `mapstructure:"test_fuzz_config"`
}

// DefaultMConnConfig returns the default config.
func DefaultMConnConfig() MConnConfig {
	return MConnConfig{
		SendRate:                defaultSendRate,
		RecvRate:                defaultRecvRate,
		MaxPacketMsgPayloadSize: defaultMaxPacketMsgPayloadSize,
		FlushThrottle:           defaultFlushThrottle,
		PingInterval:            defaultPingInterval,
		PongTimeout:             defaultPongTimeout,
	}
}

// NewMConnection wraps net.Conn and creates multiplex connection.
func NewMConnection(
	ctx context.Context,
	peerId string,
	conn net.Conn,
	channelProvider ChannelProvider,
	onReceive receiveCbFunc,
	onError errorCbFunc,
) *MConnection {
	return NewMConnectionWithConfig(
		ctx,
		peerId,
		conn,
		channelProvider,
		onReceive,
		onError,
		DefaultMConnConfig())
}

// NewMConnectionWithConfig wraps net.Conn and creates multiplex connection with a config.
func NewMConnectionWithConfig(
	ctx context.Context,
	peerId string,
	conn net.Conn,
	channelProvider ChannelProvider,
	onReceive receiveCbFunc,
	onError errorCbFunc,
	config MConnConfig,
) *MConnection {
	if config.PongTimeout >= config.PingInterval {
		panic("pongTimeout must be less than pingInterval (otherwise, next ping will reset pong timer)")
	}

	mconn := &MConnection{
		connPeerId:      peerId,
		conn:            conn,
		bufConnReader:   bufio.NewReaderSize(conn, minReadBufferSize),
		bufConnWriter:   bufio.NewWriterSize(conn, minWriteBufferSize),
		channelProvider: channelProvider,
		send:            make(chan struct{}, 1),
		pong:            make(chan struct{}, 1),
		onReceive:       onReceive,
		onError:         onError,
		config:          config,
		created:         time.Now(),
	}

	atomic.StoreUint32(&mconn.startedRoutines, 0)
	atomic.StoreUint32(&mconn.stoppedRoutines, 0)
	atomic.StoreUint32(&mconn.numOpenChannels, 0)

	mconn.pingTimer = time.NewTicker(mconn.config.PingInterval)
	mconn.pongTimeoutCh = make(chan bool, 1)
	mconn.quitSendRoutine = make(chan struct{})
	mconn.doneSendRoutine = make(chan struct{})
	mconn.quitRecvRoutine = make(chan struct{})

	mconn.BaseService = *service.NewBaseService(ctx, nil, "MConnection", mconn)

	// maxPacketMsgSize() is a bit heavy, so call just once,
	// it uses oneChainID to create a correctly-sized PacketMsg.
	templateChainID := RandomStringOfSize(66)
	mconn._maxPacketMsgSize = mconn.maxPacketMsgSize(templateChainID)

	return mconn
}

func (c *MConnection) SetLogger(l log.Logger) {
	c.BaseService.SetLogger(l)
}

func (c *MConnection) SocketAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *MConnection) NumOpenChannels() uint32 {
	return atomic.LoadUint32(&c.numOpenChannels)
}

func (c *MConnection) PeerID() string {
	return c.connPeerId
}

// OnStart implements BaseService.
func (c *MConnection) OnStart(ctx context.Context) error {
	return c.startServices(ctx)
}

// HasStartedRoutines returns true or false depending on the routines' state.
func (c *MConnection) HasStartedRoutines() bool {
	return atomic.LoadUint32(&c.startedRoutines) == 1 && atomic.LoadUint32(&c.stoppedRoutines) == 0
}

func (c *MConnection) startServices(ctx context.Context) error {
	if atomic.CompareAndSwapUint32(&c.startedRoutines, 0, 1) {
		if atomic.LoadUint32(&c.stoppedRoutines) == 1 {
			c.Logger.Error(fmt.Sprintf("Not starting %v routines -- already stopped", c.Name()),
				"impl", c)
			// revert flag
			atomic.StoreUint32(&c.startedRoutines, 0)
			return service.ErrAlreadyStopped
		}

		c.Logger.Info("routines start",
			"msg", log.NewLazySprintf("Starting %v routines", c.Name()),
			"impl", c.String())

		// Re-initialize all channels in case this conn is reset.
		c.send = make(chan struct{}, 1)
		c.pong = make(chan struct{}, 1)

		c.pingTimer = time.NewTicker(c.config.PingInterval)
		c.pongTimeoutCh = make(chan bool, 1)
		c.quitSendRoutine = make(chan struct{})
		c.doneSendRoutine = make(chan struct{})
		c.quitRecvRoutine = make(chan struct{})

		c.flushTimer = timer.NewThrottleTimer("flush", c.config.FlushThrottle)
		// c.chStatsTimer = time.NewTicker(updateStats)

		c.sendMonitor = flow.New(0, 0)
		c.recvMonitor = flow.New(0, 0)
		go c.sendRoutine()
		go c.recvRoutine()

		return nil // continue BaseService.Start()
	}

	c.Logger.Debug("routines start",
		"msg", log.NewLazySprintf("Not starting %v routines -- already started", c.Name()),
		"impl", c.String())
	return nil // continue BaseService.Start()
}

// stopServices stops the BaseService and timers and closes the quitSendRoutine.
// if the quitSendRoutine was already closed, it returns true, otherwise it returns false.
// It uses the stopMtx to ensure only one of FlushStop and OnStop can do this at a time.
func (c *MConnection) stopServices() (alreadyStopped bool) {
	if atomic.CompareAndSwapUint32(&c.stoppedRoutines, 0, 1) {
		if atomic.LoadUint32(&c.startedRoutines) == 0 {
			c.Logger.Error(fmt.Sprintf("Not stopping %v routines -- has not been started yet", c.Name()),
				"impl", c)
			// revert flag
			atomic.StoreUint32(&c.stoppedRoutines, 0)
			return false
		} else {
			// permit restarts
			atomic.StoreUint32(&c.startedRoutines, 0)
		}

		c.Logger.Info("routines stop",
			"msg", log.NewLazySprintf("Stopping %v routines", c.Name()),
			"impl", c.String())

		// IMPORTANT:
		// Stopping service, flush timer, ping timer, stats, send/recv routines

		c.stopMtx.Lock()
		c.BaseService.OnStop()
		c.flushTimer.Stop()
		c.pingTimer.Stop()
		// c.chStatsTimer.Stop()
		c.recvMonitor.Stop()
		c.sendMonitor.Stop()

		// inform the routines that we are shutting down
		close(c.quitSendRoutine)
		close(c.quitRecvRoutine)
		defer c.stopMtx.Unlock()

		return true
	}

	c.Logger.Debug("routines stop",
		"msg", log.NewLazySprintf("Stopping %v routines (already stopped)", c.Name()),
		"impl", c.String())
	return false
}

// FlushStop replicates the logic of OnStop.
// It additionally ensures that all successful
// .Send() calls will get flushed before closing
// the connection.
func (c *MConnection) FlushStop() {
	if !c.stopServices() {
		return
	}

	// this block is unique to FlushStop
	{
		// wait until the sendRoutine exits
		// so we dont race on calling sendSomePacketMsgs
		<-c.doneSendRoutine

		// Send and flush all pending msgs.
		// Since sendRoutine has exited, we can call this
		// safely
		w := protoio.NewDelimitedWriter(c.bufConnWriter)
		eof := c.sendSomePacketMsgs(w)
		for !eof {
			eof = c.sendSomePacketMsgs(w)
		}
		c.flush()

		// Now we can close the connection
	}

	c.conn.Close()
	// c.recvMonitor.Stop()
	// c.sendMonitor.Stop()

	// We can't close pong safely here because
	// recvRoutine may write to it after we've stopped.
	// Though it doesn't need to get closed at all,
	// we close it @ recvRoutine.

	// c.Stop()
}

// OnStop implements BaseService.
func (c *MConnection) OnStop() {
	if !c.stopServices() {
		return
	}

	c.conn.Close()
	// c.recvMonitor.Stop()
	// c.sendMonitor.Stop()

	// We can't close pong safely here because
	// recvRoutine may write to it after we've stopped.
	// Though it doesn't need to get closed at all,
	// we close it @ recvRoutine.
}

func (c *MConnection) OnReset(ctx context.Context) error {
	c.Logger.Debug("MConnection reset")

	atomic.StoreUint32(&c.startedRoutines, 0)
	atomic.StoreUint32(&c.stoppedRoutines, 0)
	atomic.StoreUint32(&c.numOpenChannels, 0)

	return nil
}

func (c *MConnection) String() string {
	if c.conn == nil {
		return fmt.Sprintf("nil-MConn")
	}
	return fmt.Sprintf("MConn{%v@%v}", c.connPeerId, c.conn.RemoteAddr())
}

func (c *MConnection) flush() {
	c.Logger.Debug("MConnection#Flush", "conn", c)
	err := c.bufConnWriter.Flush()
	if err != nil {
		c.Logger.Debug("MConnection flush failed", "err", err)
	}
}

// Catch panics, usually caused by remote disconnects.
func (c *MConnection) _recover() {
	if r := recover(); r != nil {
		c.Logger.Error("MConnection panicked", "err", r, "stack", string(debug.Stack()))
		c.stopForError(fmt.Errorf("recovered from panic: %v", r))
	}
}

func (c *MConnection) stopForError(r any) {
	if err := c.Stop(); err != nil {
		c.Logger.Error("Error stopping connection", "err", err)
	}
	if atomic.CompareAndSwapUint32(&c.errored, 0, 1) {
		if c.onError != nil {
			c.onError(r)
		}
	}
}

// Queues a message to be sent to channel.
func (c *MConnection) Send(chainID string, chID byte, msgBytes []byte) bool {
	if !c.IsRunning() || len(msgBytes) == 0 {
		return false
	}

	c.Logger.Debug("MConnection#Send",
		"chainID", chainID,
		"channel", chID,
		"mconn", c,
		"msgBytes", log.NewLazySprintf("%X", msgBytes))

	var channel *Channel
	channel = c.channelProvider.GetChannel(c, chID)
	if channel == nil {
		return false
	}

	// TODO(midas): remove debug logs.
	c.Logger.Debug("Channel#sendBytes",
		"chainID", chainID,
		"channel", chID,
		"mconn", c,
		"msgBytes", log.NewLazySprintf("%X", msgBytes),
	)

	// Send message to channel.
	success := channel.sendBytes(chainID, msgBytes)
	if success {
		// Wake up sendRoutine if necessary
		select {
		case c.send <- struct{}{}:
		default:
		}
	} else {
		c.Logger.Debug("Send failed", "chainID", chainID, "channel", chID, "conn", c, "msgBytes", log.NewLazySprintf("%X", msgBytes))
	}
	return success
}

// Queues a message to be sent to channel.
// Nonblocking, returns true if successful.
func (c *MConnection) TrySend(chainID string, chID byte, msgBytes []byte) bool {
	if !c.IsRunning() || len(msgBytes) == 0 {
		return false
	}

	c.Logger.Debug("MConnection#TrySend",
		"chainId", chainID,
		"channel", chID,
		"conn", c,
		"msgBytes", log.NewLazySprintf("%X", msgBytes))

	var channel *Channel
	channel = c.channelProvider.GetChannel(c, chID)
	if channel == nil {
		return false
	}

	//c.Logger.Debug("Channel", "msgBytes", log.NewLazySprintf("%X", msgBytes), "ch", channel)

	ok := channel.trySendBytes(chainID, msgBytes)
	if ok {
		// Wake up sendRoutine if necessary
		select {
		case c.send <- struct{}{}:
		default:
		}
	}

	return ok
}

// CanSend returns true if you can send more data onto the chID, false
// otherwise. Use only as a heuristic.
func (c *MConnection) CanSend(chainID string, chID byte) bool {
	if !c.IsRunning() {
		return false
	}

	var channel *Channel
	channel = c.channelProvider.GetChannel(c, chID)
	if channel == nil {
		return false
	}

	return channel.canSendForChainID(chainID)
}

// sendRoutine polls for packets to send from channels.
func (c *MConnection) sendRoutine() {
	defer c._recover()

	protoWriter := protoio.NewDelimitedWriter(c.bufConnWriter)

FOR_LOOP:
	for c.Context().Err() == nil {
		var _n int
		var err error
	SELECTION:
		select {
		case <-c.flushTimer.Ch:
			// NOTE: flushTimer.Set() must be called every time
			// something is written to .bufConnWriter.
			c.flush()
		case <-c.pingTimer.C:
			c.Logger.Debug("Send Ping")
			_n, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPing{}))
			if err != nil {
				c.Logger.Error("Failed to send PacketPing", "err", err)
				break SELECTION
			}
			c.sendMonitor.Update(_n)
			c.Logger.Debug("Starting pong timer", "dur", c.config.PongTimeout)
			c.pongTimer = time.AfterFunc(c.config.PongTimeout, func() {
				select {
				case c.pongTimeoutCh <- true:
				default:
				}
			})
			c.flush()
		case timeout := <-c.pongTimeoutCh:
			if timeout {
				c.Logger.Debug("Pong timeout")
				err = errors.New("pong timeout")
			} else {
				c.stopPongTimer()
			}
		case <-c.pong:
			c.Logger.Debug("Send Pong")
			_n, err = protoWriter.WriteMsg(mustWrapPacket(&tmp2p.PacketPong{}))
			if err != nil {
				c.Logger.Error("Failed to send PacketPong", "err", err)
				break SELECTION
			}
			c.sendMonitor.Update(_n)
			c.flush()
		case <-c.quitSendRoutine:
			break FOR_LOOP
		case <-c.send:
			// Send some PacketMsgs
			// This also calls `c.flushTimer.Set()` if anything is written.
			eof := c.sendSomePacketMsgs(protoWriter)
			if !eof {
				// Keep sendRoutine awake.
				select {
				case c.send <- struct{}{}:
				default:
				}
			}
		}

		if !c.IsRunning() {
			break FOR_LOOP
		}
		if err != nil {
			c.Logger.Error("Connection failed @ sendRoutine", "conn", c, "err", err)
			c.stopForError(err)
			break FOR_LOOP
		}
	}

	// Cleanup
	c.stopPongTimer()
	close(c.doneSendRoutine)
}

// Returns true if messages from channels were exhausted.
// Blocks in accordance to .sendMonitor throttling.
func (c *MConnection) sendSomePacketMsgs(w protoio.Writer) bool {
	// Block until .sendMonitor says we can write.
	// Once we're ready we send more than we asked for,
	// but amortized it should even out.
	//
	// TODO(midas): blocking through sendMonitor is DISABLED due to high
	// amount of messages needed to be sent with multiplex. Must be adapted.
	// c.sendMonitor.Limit(c._maxPacketMsgSize, c.config.SendRate, true)

	// Now send some PacketMsgs.
	return c.sendBatchPacketMsgs(w, numBatchPacketMsgs)
}

// Returns true if messages from channels were exhausted.
func (c *MConnection) sendBatchPacketMsgs(w protoio.Writer, batchSize int) bool {
	// Send a batch of PacketMsgs.
	totalBytesWritten := 0
	defer func() {
		if totalBytesWritten > 0 {
			c.sendMonitor.Update(totalBytesWritten)
		}
	}()
	channels := c.channelProvider.GetChannels(c)
	for i := 0; i < batchSize; i++ {
		channel := selectChannelToGossipOn(channels)
		if channel == nil {
			// nothing to send across all channels.
			return true
		}
		// nothing in buffer for selected channel.
		if channel.sending == nil {
			return true
		}

		bytesWritten, err := c.sendPacketMsgOnChannel(w, channel)
		if err {
			return true
		}
		totalBytesWritten += bytesWritten
	}
	return false
}

// selects a channel to gossip our next message on.
// TODO: Make "batchChannelToGossipOn", so we can do our proto marshaling overheads in parallel,
// and we can avoid re-checking for `isSendPending`.
// We can easily mock the recentlySent differences for the batch choosing.
func selectChannelToGossipOn(channels []*Channel) *Channel {
	// Choose a channel to create a PacketMsg from.
	// The chosen channel will be the one whose recentlySent/priority is the least.
	var leastRatio float32 = math.MaxFloat32
	var leastChannel *Channel
	for _, channel := range channels {
		// If nothing to send, skip this channel
		// TODO: Skip continually looking for isSendPending on channels we've already skipped in this batch-send.
		if !channel.isSendPending() {
			continue
		}
		// Get ratio, and keep track of lowest ratio.
		// TODO: RecentlySent right now is bytes. This should be refactored to num messages to fix
		// gossip prioritization bugs.
		ratio := float32(channel.recentlySent) / float32(channel.desc.Priority)
		if ratio < leastRatio {
			leastRatio = ratio
			leastChannel = channel
		}
	}
	return leastChannel
}

// returns (num_bytes_written, error_occurred).
func (c *MConnection) sendPacketMsgOnChannel(w protoio.Writer, sendChannel *Channel) (int, bool) {
	// Make & send a PacketMsg from this channel
	n, err := sendChannel.writePacketMsgTo(w)
	if err != nil {
		c.Logger.Error("Failed to write PacketMsg", "err", err)
		c.stopForError(err)
		return n, true
	}

	// TODO: Change this to only add flush signals at the start and end of the batch.
	c.flushTimer.Set()
	return n, false
}

// recvRoutine reads PacketMsgs and reconstructs the message using the channels' "recving" buffer.
// After a whole message has been assembled, it's pushed to onReceive().
// Blocks depending on how the connection is throttled.
// Otherwise, it never blocks.
func (c *MConnection) recvRoutine() {
	defer c._recover()

	protoReader := protoio.NewDelimitedReader(c.bufConnReader, c._maxPacketMsgSize)

FOR_LOOP:
	for c.Context().Err() == nil {
		// Block until .recvMonitor says we can read.
		//
		// TODO(midas): blocking through recvMonitor is DISABLED due to high
		// amount of messages needed with multiplex. Must be adapted.
		// c.recvMonitor.Limit(c._maxPacketMsgSize, atomic.LoadInt64(&c.config.RecvRate), true)

		// Peek into bufConnReader for debugging
		/*
			if numBytes := c.bufConnReader.Buffered(); numBytes > 0 {
				bz, err := c.bufConnReader.Peek(cmtmath.MinInt(numBytes, 100))
				if err == nil {
					// return
				} else {
					c.Logger.Debug("Error peeking connection buffer", "err", err)
					// return nil
				}
				c.Logger.Info("Peek connection buffer", "numBytes", numBytes, "bz", bz)
			}
		*/

		// Read packet type
		var packet tmp2p.Packet

		_n, err := protoReader.ReadMsg(&packet)
		c.recvMonitor.Update(_n)
		if err != nil {
			// stopServices was invoked and we are shutting down
			// receiving is expected to fail since we will close the connection
			select {
			case <-c.quitRecvRoutine:
				break FOR_LOOP
			default:
			}

			if c.IsRunning() {
				if errors.Is(err, io.EOF) {
					c.Logger.Info("Connection is closed @ recvRoutine (likely by the other side)", "conn", c)
				} else {
					c.Logger.Debug("Connection failed @ recvRoutine (reading byte)", "conn", c, "err", err)
				}
				c.stopForError(err)
			}
			break FOR_LOOP
		}

		// Read more depending on packet type.
		switch pkt := packet.Sum.(type) {
		case *tmp2p.Packet_PacketPing:
			// TODO: prevent abuse, as they cause flush()'s.
			// https://github.com/tendermint/tendermint/issues/1190
			c.Logger.Debug("Receive Ping")
			select {
			case c.pong <- struct{}{}:
			default:
				// never block
			}
		case *tmp2p.Packet_PacketPong:
			c.Logger.Debug("Receive Pong")
			select {
			case c.pongTimeoutCh <- false:
			default:
				// never block
			}
		case *tmp2p.Packet_PacketMsg:
			if pkt.PacketMsg.ChannelID < 0 || pkt.PacketMsg.ChannelID > math.MaxUint8 {
				err := fmt.Errorf("unknown channel %X", pkt.PacketMsg.ChannelID)
				c.Logger.Debug("Connection failed @ recvRoutine", "conn", c, "err", err)
				c.stopForError(err)
				break FOR_LOOP
			}

			chainID := pkt.PacketMsg.ChainID
			channelID := byte(pkt.PacketMsg.ChannelID)

			channel := c.channelProvider.GetChannel(c, channelID)

			msgBytes, err := channel.recvPacketMsg(*pkt.PacketMsg)
			if err != nil {
				if c.IsRunning() {
					c.Logger.Debug("Connection failed @ recvRoutine",
						"conn", c, "chID", channelID, "err", err)
					c.stopForError(err)
				}
				break FOR_LOOP
			}
			if msgBytes != nil && len(msgBytes) > 0 {
				c.Logger.Debug("Received bytes", "ChainId", chainID, "chID", channelID, "msgBytes", msgBytes)
				// NOTE: This means the reactor.Receive runs in the same thread as the p2p recv routine
				c.onReceive(chainID, channelID, msgBytes)
			}
		default:
			err := fmt.Errorf("unknown message type %v", reflect.TypeOf(packet))
			c.Logger.Error("Connection failed @ recvRoutine", "conn", c, "err", err)
			c.stopForError(err)
			break FOR_LOOP
		}
	}

	// Cleanup
	close(c.pong)
}

// not goroutine-safe.
func (c *MConnection) stopPongTimer() {
	if c.pongTimer != nil {
		_ = c.pongTimer.Stop()
		c.pongTimer = nil
	}
}

// maxPacketMsgSize returns a maximum size of PacketMsg.
func (c *MConnection) maxPacketMsgSize(chainID string) int {
	var size int
	{
		bz, err := proto.Marshal(mustWrapPacket(&tmp2p.PacketMsg{
			ChainID:   chainID,
			ChannelID: 0x01,
			EOF:       true,
			Data:      make([]byte, c.config.MaxPacketMsgPayloadSize),
		}))
		if err != nil {
			panic(err)
		}
		size = len(bz)
	}
	return size
}

// -----------------------------------------------------------------------------

type ConnectionStatus struct {
	Duration    time.Duration
	SendMonitor flow.Status
	RecvMonitor flow.Status
	Channels    []ChannelStatus
}

// -----------------------------------------------------------------------------

type ChannelStatus struct {
	ID                byte
	SendQueueCapacity int
	SendQueueSize     int
	Priority          int
	RecentlySent      int64
}

func (c *MConnection) Status() ConnectionStatus {
	var status ConnectionStatus
	status.Duration = time.Since(c.created)
	status.SendMonitor = c.sendMonitor.Status()
	status.RecvMonitor = c.recvMonitor.Status()
	status.Channels = []ChannelStatus{}
	for _, channel := range c.channelProvider.GetChannels(c) {
		status.Channels = append(status.Channels, ChannelStatus{
			ID:                channel.desc.ID,
			SendQueueCapacity: channel.desc.SendQueueCapacity,
			SendQueueSize:     int(atomic.LoadInt32(&channel.sendQueueSize)),
			Priority:          channel.desc.Priority,
			RecentlySent:      atomic.LoadInt64(&channel.recentlySent),
		})
	}
	return status
}

// -----------------------------------------------------------------------------

// ChannelProvider defines the contract for channel providers.
type ChannelProvider interface {
	// // InitChannels should initialize the channels index of a provider.
	// InitChannels()
	// // GetChannels returns a slice of Channel instances.
	GetChannels(mconn *MConnection) []*Channel
	// GetChannel returns a Channel by ID for mconn.
	GetChannel(mconn *MConnection, chID byte) *Channel
	// GetDescriptor returns a ChannelDescriptor by ID.
	GetDescriptor(chID byte) *ChannelDescriptor
}

// -----------------------------------------------------------------------------

type ChannelDescriptor struct {
	ID                  byte
	Priority            int
	SendQueueCapacity   int
	RecvBufferCapacity  int
	RecvMessageCapacity int
	MessageType         proto.Message
}

func (chDesc ChannelDescriptor) FillDefaults() (filled *ChannelDescriptor) {
	if chDesc.SendQueueCapacity == 0 {
		chDesc.SendQueueCapacity = defaultSendQueueCapacity
	}
	if chDesc.RecvBufferCapacity == 0 {
		chDesc.RecvBufferCapacity = defaultRecvBufferCapacity
	}
	if chDesc.RecvMessageCapacity == 0 {
		chDesc.RecvMessageCapacity = defaultRecvMessageCapacity
	}
	filled = &chDesc
	return filled
}

// -----------------------------------------------------------------------------

// QueuedMessage is a wrapper for messages being queued, and maps a ChainID and
// Size to every message in the queue.
type QueuedMessage struct {
	ChainID string
	Data    []byte
	Size    int
}

// -----------------------------------------------------------------------------

type Channel struct {
	mtx *sync.Mutex

	conn *MConnection
	desc *ChannelDescriptor
	rand *cmtrand.Rand

	// recving holds bytes being received, until read.
	recving []byte
	// sending holds bytes being sent, until sent.
	sending      []byte // under mtx
	sendCID      string // under mtx
	recentlySent int64  // exponential moving average

	// sendQueue is a queue of messages to be sent.
	sendQueue chan QueuedMessage
	// sendQueueSize contains the number of messages queued.
	sendQueueSize int32 // atomic.

	nextPacketMsg           *tmp2p.PacketMsg
	nextP2pWrapperPacketMsg *tmp2p.Packet_PacketMsg
	nextPacket              *tmp2p.Packet

	maxPacketMsgPayloadSize int

	Logger log.Logger
}

func NewChannel(conn *MConnection, desc *ChannelDescriptor) *Channel {
	desc = desc.FillDefaults()
	if desc.Priority <= 0 {
		panic("Channel default priority must be a positive integer")
	}

	return &Channel{
		mtx:  new(sync.Mutex),
		conn: conn,
		desc: desc,
		rand: cmtrand.NewRand(),

		sendQueue: make(chan QueuedMessage, desc.SendQueueCapacity),

		sending: []byte{},
		recving: make([]byte, 0, desc.RecvBufferCapacity),

		nextPacketMsg: &tmp2p.PacketMsg{
			ChannelID: int32(desc.ID),
		},
		nextP2pWrapperPacketMsg: &tmp2p.Packet_PacketMsg{},
		nextPacket:              &tmp2p.Packet{},
		maxPacketMsgPayloadSize: defaultMaxPacketMsgPayloadSize,
	}
}

func (ch *Channel) SetLogger(l log.Logger) {
	ch.Logger = l
}

func (ch *Channel) Desc() *ChannelDescriptor {
	return ch.desc
}

// Queues message to send to this channel.
// Times out (and returns false) after defaultSendTimeout.
// Goroutine-safe.
func (ch *Channel) sendBytes(chainID string, bytes []byte) bool {
	select {
	case ch.sendQueue <- QueuedMessage{
		ChainID: chainID,
		Data:    bytes,
		Size:    len(bytes),
	}:
		atomic.AddInt32(&ch.sendQueueSize, 1)
		return true
	case <-time.After(defaultSendTimeout):
		return false
	case <-ch.conn.Quit():
		return false
	}
}

// Queues message to send to this channel.
// Nonblocking, returns true if successful.
// Goroutine-safe.
func (ch *Channel) trySendBytes(chainID string, bytes []byte) bool {
	select {
	case ch.sendQueue <- QueuedMessage{
		ChainID: chainID,
		Data:    bytes,
		Size:    len(bytes),
	}:
		atomic.AddInt32(&ch.sendQueueSize, 1)
		return true
	default:
		return false
	}
}

// Goroutine-safe.
func (ch *Channel) loadSendQueueSize() (size int) {
	return int(atomic.LoadInt32(&ch.sendQueueSize))
}

// Goroutine-safe
// Use only as a heuristic.
func (ch *Channel) canSend() bool {
	return ch.loadSendQueueSize() < defaultSendQueueCapacity
}

// Goroutine-safe
// Use only as a heuristic.
func (ch *Channel) canSendForChainID(_ string) bool {
	return ch.loadSendQueueSize() < defaultSendQueueCapacity
}

// Returns true if any PacketMsgs are pending to be sent.
// Call before calling updateNextPacket
// Goroutine-safe.
func (ch *Channel) isSendPending() bool {
	if ch == nil {
		return false
	}

	ch.mtx.Lock()
	isResetAfterSend := ch.sending == nil
	ch.mtx.Unlock()

	if isResetAfterSend && ch.loadSendQueueSize() == 0 {
		return false
	}

	ch.mtx.Lock()
	defer ch.mtx.Unlock()

	// Consuming send queue's next message (FIFO).
	// Non-blocking consumer avoids getting stuck here, e.g. shutdown.
	select {
	case msg := <-ch.sendQueue:
		ch.sendCID = msg.ChainID
		ch.sending = msg.Data
		return true
	default:
	}
	return false
}

// Updates the nextPacket proto message for us to send.
func (ch *Channel) updateNextPacket() {
	ch.mtx.Lock()
	defer ch.mtx.Unlock()

	maxSize := ch.maxPacketMsgPayloadSize
	if len(ch.sending) <= maxSize {
		ch.nextPacketMsg.ChainID = ch.sendCID
		ch.nextPacketMsg.Data = ch.sending
		ch.nextPacketMsg.EOF = true
		ch.sending = nil

		atomic.AddInt32(&ch.sendQueueSize, -1)
	} else {
		ch.nextPacketMsg.ChainID = ch.sendCID
		ch.nextPacketMsg.Data = ch.sending[:maxSize]
		ch.nextPacketMsg.EOF = false
		ch.sending = ch.sending[maxSize:]
	}

	ch.nextP2pWrapperPacketMsg.PacketMsg = ch.nextPacketMsg
	ch.nextPacket.Sum = ch.nextP2pWrapperPacketMsg
}

// Writes next PacketMsg to w and updates c.recentlySent.
func (ch *Channel) writePacketMsgTo(w protoio.Writer) (n int, err error) {
	ch.updateNextPacket()

	n, err = w.WriteMsg(ch.nextPacket)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&ch.recentlySent, int64(n))
	return n, nil
}

// Handles incoming PacketMsgs. It returns a message bytes if message is
// complete. NOTE message bytes may change on next call to recvPacketMsg.
// Not goroutine-safe.
func (ch *Channel) recvPacketMsg(packet tmp2p.PacketMsg) ([]byte, error) {
	if ch.Logger != nil {
		ch.Logger.Debug("Read PacketMsg", "packet", packet, "chID", ch.desc.ID)
	}

	ch.mtx.Lock()
	defer ch.mtx.Unlock()

	recvCap, recvReceived := ch.desc.RecvMessageCapacity, len(ch.recving)+len(packet.Data)
	if recvCap < recvReceived {
		return nil, fmt.Errorf("received message exceeds available capacity: %v < %v", recvCap, recvReceived)
	}
	ch.recving = append(ch.recving, packet.Data...)
	if packet.EOF {
		msgBytes := ch.recving

		// clear the slice without re-allocating.
		// http://stackoverflow.com/questions/16971741/how-do-you-clear-a-slice-in-go
		//   suggests this could be a memory leak, but we might as well keep the memory for the channel until it closes,
		//	at which point the recving slice stops being used and should be garbage collected
		ch.recving = ch.recving[:0] // make([]byte, 0, ch.desc.RecvBufferCapacity)
		return msgBytes, nil
	}
	return nil, nil
}

// Call this periodically to update stats for throttling purposes.
// Not goroutine-safe.
func (ch *Channel) updateStats() {
	// Exponential decay of stats.
	// TODO: optimize.
	atomic.StoreInt64(&ch.recentlySent, int64(float64(atomic.LoadInt64(&ch.recentlySent))*0.8))
}

// ----------------------------------------
// Packet

// mustWrapPacket takes a packet kind (oneof) and wraps it in a tmp2p.Packet message.
func mustWrapPacket(pb proto.Message) *tmp2p.Packet {
	msg := &tmp2p.Packet{}
	mustWrapPacketInto(pb, msg)
	return msg
}

func mustWrapPacketInto(pb proto.Message, dst *tmp2p.Packet) {
	switch pb := pb.(type) {
	case *tmp2p.PacketPing:
		dst.Sum = &tmp2p.Packet_PacketPing{
			PacketPing: pb,
		}
	case *tmp2p.PacketPong:
		dst.Sum = &tmp2p.Packet_PacketPong{
			PacketPong: pb,
		}
	case *tmp2p.PacketMsg:
		dst.Sum = &tmp2p.Packet_PacketMsg{
			PacketMsg: pb,
		}
	default:
		panic(fmt.Errorf("unknown packet type %T", pb))
	}
}

// ----------------------------------------
// Utils

// RandomStringOfSize generates a random string of n characters.
func RandomStringOfSize(n int) string {
	rand.Seed(time.Now().UnixNano())
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
