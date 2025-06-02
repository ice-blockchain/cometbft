package p2p

import (
	"net"

	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
)

// ConnSet is a lookup table for connections and all their ips.
type ConnSet interface {
	Has(conn net.Conn, outbound bool) bool
	HasIP(ip net.IP) bool
	Set(conn net.Conn, ip []net.IP, outbound bool)
	Get(conn net.Conn, outbound bool) connSetItem
	Remove(conn net.Conn)
	RemoveAddr(addr net.Addr)
	ForEach(func(conn net.Conn))
}

type connSetItem struct {
	conn net.Conn
	ips  []net.IP
}

type connSet struct {
	cmtsync.RWMutex

	conns map[string]connSetItem
}

// NewConnSet returns a ConnSet implementation.
func NewConnSet() ConnSet {
	return &connSet{
		conns: map[string]connSetItem{},
	}
}

func (cs *connSet) Has(c net.Conn, outbound bool) bool {
	cs.RLock()
	defer cs.RUnlock()
	key := cs.key(c.RemoteAddr(), outbound)

	_, ok := cs.conns[key]

	return ok
}

func (cs *connSet) HasIP(ip net.IP) bool {
	cs.RLock()
	defer cs.RUnlock()

	for _, c := range cs.conns {
		for _, known := range c.ips {
			if known.Equal(ip) {
				return true
			}
		}
	}

	return false
}

func (cs *connSet) Get(c net.Conn, outbound bool) connSetItem {
	cs.Lock()
	defer cs.Unlock()

	key := cs.key(c.RemoteAddr(), outbound)
	return cs.conns[key]
}

func (cs *connSet) Remove(c net.Conn) {
	cs.Lock()
	defer cs.Unlock()
	key1 := cs.key(c.RemoteAddr(), true)
	key2 := cs.key(c.RemoteAddr(), false)
	delete(cs.conns, key1)
	delete(cs.conns, key2)
}

func (cs *connSet) RemoveAddr(addr net.Addr) {
	cs.Lock()
	defer cs.Unlock()
	key1 := cs.key(addr, true)
	key2 := cs.key(addr, false)
	delete(cs.conns, key1)
	delete(cs.conns, key2)
}

func (cs *connSet) ForEach(fn func(c net.Conn)) {
	cs.Lock()
	defer cs.Unlock()
	for _, c := range cs.conns {
		fn(c.conn)
	}
}

func (cs *connSet) Set(c net.Conn, ips []net.IP, outbound bool) {
	cs.Lock()
	defer cs.Unlock()
	key := cs.key(c.RemoteAddr(), outbound)
	cs.conns[key] = connSetItem{
		conn: c,
		ips:  ips,
	}
}

func (cs *connSet) key(addr net.Addr, outbound bool) string {
	suffix := "in"
	if outbound {
		suffix = "out"
	}
	return addr.String() + "_" + suffix
}
