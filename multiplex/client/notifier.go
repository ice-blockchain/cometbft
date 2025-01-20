package client

var _ Notifier = (*StatusNotifier)(nil)

// ----------------------------------------------------------------------------
// StatusNotifier defines a broadcast status notifier
//
// StatusNotifier implements the [Notifier] interface.
type StatusNotifier struct {
	channel chan<- BroadcastStatus
}

// SetChannel registers a receive-only channel for this notifier.
//
// SetChannel implements [Notifier]
func (n *StatusNotifier) SetChannel(ch chan<- BroadcastStatus) {
	n.channel = ch
}

// GetChannel returns a receive-only channel for this notifier.
func (n *StatusNotifier) GetChannel() chan<- BroadcastStatus {
	return n.channel
}

// Error pushes a [BroadcastStatus] on the notifier and
// attaches the error.
func (n *StatusNotifier) Error(err error) {
	n.GetChannel() <- BroadcastStatus{
		Error:    err,
		TxHashes: [][]byte{},
	}
}

// Success pushes a [BroadcastStatus] on the notifier and
// attaches a nil-error and accepted transaction hashes.
func (n *StatusNotifier) Success(txHashes [][]byte) {
	n.GetChannel() <- BroadcastStatus{
		Error:    nil,
		TxHashes: txHashes,
	}
}
