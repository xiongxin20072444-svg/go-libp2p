package quicreuse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

// abortingConn simulates Android destroying the sockets of a frozen app: after
// abort, reads fail permanently while the socket stays bound to its port.
type abortingConn struct {
	net.PacketConn
	aborted atomic.Bool
}

var errSocketAborted = &net.OpError{Op: "read", Net: "udp", Err: os.NewSyscallError("recvfrom", syscall.ECONNABORTED)}

func (c *abortingConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if c.aborted.Load() {
		return 0, nil, errSocketAborted
	}
	return n, addr, err
}

func (c *abortingConn) abort() {
	c.aborted.Store(true)
	c.PacketConn.SetReadDeadline(time.Now())
}

func newAbortingConnManager(t *testing.T) (*ConnManager, func() []*abortingConn) {
	t.Helper()
	var mu sync.Mutex
	var conns []*abortingConn
	cm, err := NewConnManager(quic.StatelessResetKey{}, quic.TokenGeneratorKey{}, OverrideListenUDP(
		func(network string, laddr *net.UDPAddr) (net.PacketConn, error) {
			c, err := net.ListenUDP(network, laddr)
			if err != nil {
				return nil, err
			}
			ac := &abortingConn{PacketConn: c}
			mu.Lock()
			conns = append(conns, ac)
			mu.Unlock()
			return ac, nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { cm.Close() })
	return cm, func() []*abortingConn {
		mu.Lock()
		defer mu.Unlock()
		return append([]*abortingConn(nil), conns...)
	}
}

func TestSocketAbortIsNotTemporary(t *testing.T) {
	// quic-go keeps reading after temporary errors; the transport only shuts
	// down (and the listener only fails) for permanent ones.
	require.False(t, errSocketAborted.Temporary())
}

func TestIsTransportFailure(t *testing.T) {
	require.False(t, isTransportFailure(quic.ErrTransportClosed), "deliberate close has no cause")
	require.False(t, isTransportFailure(quic.ErrServerClosed))
	require.False(t, isTransportFailure(errors.New("other")))
}

func TestDeadListeningTransportReleasesPortImmediately(t *testing.T) {
	cm, conns := newAbortingConnManager(t)
	tlsConf := &tls.Config{NextProtos: []string{"proto"}}
	ln, err := cm.ListenQUIC(ma.StringCast("/ip4/0.0.0.0/udp/0/quic-v1"), tlsConf, nil)
	require.NoError(t, err)
	port := ln.Addr().(*net.UDPAddr).Port

	conns()[0].abort()
	_, err = ln.Accept(context.Background())
	require.Error(t, err)
	require.NoError(t, ln.Close()) // the swarm closes a listener whose Accept failed

	ln2, err := cm.ListenQUIC(ma.StringCast(fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port)), tlsConf, nil)
	require.NoError(t, err, "the port must be released without waiting for garbage collection")
	defer ln2.Close()

	tr, err := cm.TransportForDial("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	require.NoError(t, err)
	defer tr.DecreaseCount()
	require.Equal(t, port, tr.LocalAddr().(*net.UDPAddr).Port, "dials must reuse the new listening socket")
	require.NotSame(t, conns()[0], tr.(*refcountedTransport).packetConn)
}

func TestDeliberateListenerCloseKeepsTransportForReuse(t *testing.T) {
	cm, _ := newAbortingConnManager(t)
	ln, err := cm.ListenQUIC(ma.StringCast("/ip4/0.0.0.0/udp/0/quic-v1"), &tls.Config{NextProtos: []string{"proto"}}, nil)
	require.NoError(t, err)
	port := ln.Addr().(*net.UDPAddr).Port
	require.NoError(t, ln.Close())

	reuse, err := cm.getReuse("udp4")
	require.NoError(t, err)
	reuse.mutex.Lock()
	tr, ok := reuse.globalListeners[port]
	reuse.mutex.Unlock()
	require.True(t, ok, "a deliberately closed listener keeps its transport until garbage collection")
	require.False(t, tr.isDead())
}

func TestDeadDialingTransportIsReplaced(t *testing.T) {
	cm, conns := newAbortingConnManager(t)
	target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	tr, err := cm.TransportForDial("udp4", target)
	require.NoError(t, err)
	dead := tr.(*refcountedTransport)
	tr.DecreaseCount()
	conns()[0].abort()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cm.DialQUIC(ctx, ma.StringCast("/ip4/127.0.0.1/udp/1/quic-v1"), &tls.Config{NextProtos: []string{"proto"}}, nil)
	require.Error(t, err)
	require.Eventually(t, dead.isDead, 2*time.Second, 10*time.Millisecond)

	next, err := cm.TransportForDial("udp4", target)
	require.NoError(t, err)
	defer next.DecreaseCount()
	require.NotSame(t, dead, next)
}
