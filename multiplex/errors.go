package multiplex

import "fmt"

type ErrBroadcastCancelled struct {
	TxHash string
}

func (e ErrBroadcastCancelled) Error() string {
	return fmt.Sprintf("broadcast cancelled - other relays failed to accept transaction %s", e.TxHash)
}
