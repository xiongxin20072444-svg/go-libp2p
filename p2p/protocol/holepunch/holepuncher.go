package holepunch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-msgio/pbio"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// ErrHolePunchActive is returned from DirectConnect when another hole punching attempt is currently running
var ErrHolePunchActive = errors.New("another hole punching attempt to this peer is active")

const maxRetries = 3

// The holePuncher is run on the peer that's behind a NAT / Firewall.
// It observes new incoming connections via a relay that it has a reservation with,
// and initiates the DCUtR protocol with them.
// It then first tries to establish a direct connection, and if that fails, it
// initiates a hole punch.
type holePuncher struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	host     host.Host
	refCount sync.WaitGroup

	ids         identify.IDService
	listenAddrs func() []ma.Multiaddr

	directDialTimeout time.Duration

	// active hole punches for deduplicating
	activeMx sync.Mutex
	active   map[peer.ID]struct{}
	// A relay reconnect that arrives during an active attempt represents newer
	// connectivity information. Coalesce any burst into one bounded replay.
	pending   map[peer.ID]struct{}
	replaying map[peer.ID]struct{}

	closeMx sync.RWMutex
	closed  bool

	tracer                    *tracer
	filter                    AddrFilter
	directConnectionValidator DirectConnectionValidator
}

func newHolePuncher(
	h host.Host,
	ids identify.IDService,
	listenAddrs func() []ma.Multiaddr,
	tracer *tracer,
	filter AddrFilter,
	directConnectionValidator DirectConnectionValidator,
) *holePuncher {
	hp := &holePuncher{
		host:                      h,
		ids:                       ids,
		active:                    make(map[peer.ID]struct{}),
		pending:                   make(map[peer.ID]struct{}),
		replaying:                 make(map[peer.ID]struct{}),
		tracer:                    tracer,
		filter:                    filter,
		listenAddrs:               listenAddrs,
		directConnectionValidator: directConnectionValidator,
	}
	hp.ctx, hp.ctxCancel = context.WithCancel(context.Background())
	h.Network().Notify((*netNotifiee)(hp))
	return hp
}

func (hp *holePuncher) beginDirectConnect(p peer.ID) error {
	hp.closeMx.RLock()
	defer hp.closeMx.RUnlock()
	if hp.closed {
		return ErrClosed
	}

	hp.activeMx.Lock()
	defer hp.activeMx.Unlock()
	if _, ok := hp.active[p]; ok {
		return ErrHolePunchActive
	}

	hp.active[p] = struct{}{}
	delete(hp.pending, p)
	delete(hp.replaying, p)
	return nil
}

// beginOrQueueDirectConnect starts the first relay-triggered attempt and keeps
// at most one newer trigger while it is active. Triggers received during the
// replay are intentionally dropped so repeated relay connections cannot build
// an unbounded retry chain.
func (hp *holePuncher) beginOrQueueDirectConnect(p peer.ID) (bool, error) {
	hp.closeMx.RLock()
	defer hp.closeMx.RUnlock()
	if hp.closed {
		return false, ErrClosed
	}

	hp.activeMx.Lock()
	defer hp.activeMx.Unlock()
	if _, ok := hp.active[p]; ok {
		if _, replaying := hp.replaying[p]; !replaying {
			if hp.pending == nil {
				hp.pending = make(map[peer.ID]struct{})
			}
			hp.pending[p] = struct{}{}
		}
		return false, nil
	}

	if hp.active == nil {
		hp.active = make(map[peer.ID]struct{})
	}
	if hp.replaying == nil {
		hp.replaying = make(map[peer.ID]struct{})
	}
	hp.active[p] = struct{}{}
	delete(hp.pending, p)
	delete(hp.replaying, p)
	return true, nil
}

// finishDirectConnect returns true exactly once when a newer relay trigger was
// coalesced. The peer remains active while that replay runs.
func (hp *holePuncher) finishDirectConnect(p peer.ID) bool {
	hp.closeMx.RLock()
	closed := hp.closed
	hp.closeMx.RUnlock()

	hp.activeMx.Lock()
	defer hp.activeMx.Unlock()
	if !closed {
		if _, pending := hp.pending[p]; pending {
			if _, replaying := hp.replaying[p]; !replaying {
				delete(hp.pending, p)
				hp.replaying[p] = struct{}{}
				return true
			}
		}
	}
	delete(hp.active, p)
	delete(hp.pending, p)
	delete(hp.replaying, p)
	return false
}

func (hp *holePuncher) isDirectConnectActive(p peer.ID) bool {
	hp.activeMx.Lock()
	defer hp.activeMx.Unlock()
	_, ok := hp.active[p]
	return ok
}

func (hp *holePuncher) runDirectConnectRequests(p peer.ID) error {
	for {
		err := hp.directConnect(p)
		if !hp.finishDirectConnect(p) {
			return err
		}
		log.Debug("running coalesced DirectConnect trigger", "remote_peer", p)
	}
}

// DirectConnect attempts to make a direct connection with a remote peer.
// It first attempts a direct dial (if we have a public address of that peer), and then
// coordinates a hole punch over the given relay connection.
func (hp *holePuncher) DirectConnect(p peer.ID) error {
	log.Debug("beginDirectConnect", "source_peer", hp.host.ID(), "destination_peer", p)
	if err := hp.beginDirectConnect(p); err != nil {
		return err
	}

	return hp.runDirectConnectRequests(p)
}

func (hp *holePuncher) directConnect(rp peer.ID) error {
	// short-circuit check to see if we already have a direct connection
	directConnections := getDirectConnections(hp.host, rp)
	if len(directConnections) > 0 {
		if hp.directConnectionValidator == nil {
			log.Debug("already connected", "source_peer", hp.host.ID(), "destination_peer", rp)
			return nil
		}
		for _, direct := range directConnections {
			if err := hp.directConnectionValidator(hp.ctx, direct); err == nil {
				log.Debug("existing direct connection verified", "source_peer", hp.host.ID(), "destination_peer", rp, "connection_id", direct.ID())
				return nil
			} else {
				log.Debug(
					"retiring unresponsive direct connection before hole punch",
					"source_peer", hp.host.ID(),
					"destination_peer", rp,
					"connection_id", direct.ID(),
					"err", err,
				)
				if closeErr := direct.Close(); closeErr != nil {
					log.Debug(
						"failed to close unresponsive direct connection",
						"source_peer", hp.host.ID(),
						"destination_peer", rp,
						"connection_id", direct.ID(),
						"err", closeErr,
					)
				}
			}
		}
	}

	log.Debug("attempting direct dial", "source_peer", hp.host.ID(), "destination_peer", rp, "addrs", hp.host.Peerstore().Addrs(rp))
	// short-circuit hole punching if a direct dial works.
	// attempt a direct connection ONLY if we have a public address for the remote peer
	for _, a := range hp.host.Peerstore().Addrs(rp) {
		if !isRelayAddress(a) && manet.IsPublicAddr(a) {
			forceDirectConnCtx := network.WithForceDirectDial(hp.ctx, "hole-punching")
			dialCtx, cancel := context.WithTimeout(forceDirectConnCtx, hp.directDialTimeout)

			tstart := time.Now()
			// This dials *all* addresses, public and private, from the peerstore.
			err := hp.host.Connect(dialCtx, peer.AddrInfo{ID: rp})
			dt := time.Since(tstart)
			cancel()

			if err != nil {
				hp.tracer.DirectDialFailed(rp, dt, err)
				break
			}
			hp.tracer.DirectDialSuccessful(rp, dt)
			log.Debug("direct connection to peer successful, no need for a hole punch", "destination_peer", rp)
			return nil
		}
	}

	log.Debug("got inbound proxy conn", "destination_peer", rp)

	// hole punch
	for i := 1; i <= maxRetries; i++ {
		isClient := false
		// On the last attempt we switch roles in case the connection is
		// being made with a client with switched roles. Common for peers
		// running go-libp2p prior to v0.41.
		if i == maxRetries {
			isClient = true
		}
		addrs, obsAddrs, rtt, err := hp.initiateHolePunch(rp)
		if err != nil {
			hp.tracer.ProtocolError(rp, err)
			return err
		}
		synTime := rtt / 2
		log.Debug("peer RTT and starting hole punch", "rtt", rtt, "syn_time", synTime)

		// wait for sync to reach the other peer and then punch a hole for it in our NAT
		// by attempting a connect to it.
		timer := time.NewTimer(synTime)
		select {
		case start := <-timer.C:
			pi := peer.AddrInfo{
				ID:    rp,
				Addrs: addrs,
			}
			hp.tracer.StartHolePunch(rp, addrs, rtt)
			hp.tracer.HolePunchAttempt(pi.ID)
			ctx, cancel := context.WithTimeout(hp.ctx, hp.directDialTimeout)
			err := holePunchConnect(ctx, hp.host, pi, isClient)
			cancel()
			dt := time.Since(start)
			hp.tracer.EndHolePunch(rp, dt, err)
			if err == nil {
				log.Debug("hole punching with successful", "destination_peer", rp, "duration", dt)
				hp.tracer.HolePunchFinished("initiator", i, addrs, obsAddrs, getDirectConnection(hp.host, rp))
				return nil
			}
		case <-hp.ctx.Done():
			timer.Stop()
			return hp.ctx.Err()
		}
		if i == maxRetries {
			hp.tracer.HolePunchFinished("initiator", maxRetries, addrs, obsAddrs, nil)
		}
	}
	return fmt.Errorf("all retries for hole punch with peer %s failed", rp)
}

// initiateHolePunch opens a new hole punching coordination stream,
// exchanges the addresses and measures the RTT.
func (hp *holePuncher) initiateHolePunch(rp peer.ID) ([]ma.Multiaddr, []ma.Multiaddr, time.Duration, error) {
	hpCtx := network.WithAllowLimitedConn(hp.ctx, "hole-punch")
	sCtx := network.WithNoDial(hpCtx, "hole-punch")

	str, err := hp.host.NewStream(sCtx, rp, Protocol)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to open hole-punching stream: %w", err)
	}
	defer str.Close()
	log.Debug("initiateHolePunch", "remote_peer", str.Conn().RemotePeer(), "remote_multiaddr", str.Conn().RemoteMultiaddr())

	addr, obsAddr, rtt, err := hp.initiateHolePunchImpl(str)
	if err != nil {
		str.Reset()
		return addr, obsAddr, rtt, fmt.Errorf("failed to initiateHolePunch: %w", err)
	}
	return addr, obsAddr, rtt, err
}

func (hp *holePuncher) initiateHolePunchImpl(str network.Stream) ([]ma.Multiaddr, []ma.Multiaddr, time.Duration, error) {
	if err := str.Scope().SetService(ServiceName); err != nil {
		return nil, nil, 0, fmt.Errorf("error attaching stream to holepunch service: %s", err)
	}

	if err := str.Scope().ReserveMemory(maxMsgSize, network.ReservationPriorityAlways); err != nil {
		return nil, nil, 0, fmt.Errorf("error reserving memory for stream: %s", err)
	}
	defer str.Scope().ReleaseMemory(maxMsgSize)

	w := pbio.NewDelimitedWriter(str)
	rd := pbio.NewDelimitedReader(str, maxMsgSize)

	str.SetDeadline(time.Now().Add(StreamTimeout))

	// send a CONNECT and start RTT measurement.
	obsAddrs := removeRelayAddrs(hp.listenAddrs())
	if hp.filter != nil {
		obsAddrs = hp.filter.FilterLocal(str.Conn().RemotePeer(), obsAddrs)
	}
	if len(obsAddrs) == 0 {
		return nil, nil, 0, errors.New("aborting hole punch initiation as we have no public address")
	}
	log.Debug("initiating hole punch", "observed_addrs", obsAddrs)

	start := time.Now()
	if err := w.WriteMsg(&pb.HolePunch{
		Type:     pb.HolePunch_CONNECT.Enum(),
		ObsAddrs: addrsToBytes(obsAddrs),
	}); err != nil {
		str.Reset()
		return nil, nil, 0, err
	}

	// wait for a CONNECT message from the remote peer
	var msg pb.HolePunch
	if err := rd.ReadMsg(&msg); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to read CONNECT message from remote peer: %w", err)
	}
	rtt := time.Since(start)
	if t := msg.GetType(); t != pb.HolePunch_CONNECT {
		return nil, nil, 0, fmt.Errorf("expect CONNECT message, got %s", t)
	}

	addrs := removeRelayAddrs(addrsFromBytes(msg.ObsAddrs))
	if hp.filter != nil {
		addrs = hp.filter.FilterRemote(str.Conn().RemotePeer(), addrs)
	}

	if len(addrs) == 0 {
		return nil, nil, 0, errors.New("didn't receive any public addresses in CONNECT")
	}

	if err := w.WriteMsg(&pb.HolePunch{Type: pb.HolePunch_SYNC.Enum()}); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to send SYNC message for hole punching: %w", err)
	}
	return addrs, obsAddrs, rtt, nil
}

func (hp *holePuncher) Close() error {
	hp.closeMx.Lock()
	hp.closed = true
	hp.closeMx.Unlock()
	hp.ctxCancel()
	hp.refCount.Wait()
	return nil
}

type netNotifiee holePuncher

func (nn *netNotifiee) Connected(_ network.Network, conn network.Conn) {
	hs := (*holePuncher)(nn)

	// Hole punch if it's an inbound proxy connection.
	// If we already have a direct connection with the remote peer, this will be a no-op.
	if conn.Stat().Direction == network.DirInbound && isRelayAddress(conn.RemoteMultiaddr()) {
		hs.refCount.Add(1)
		go func() {
			defer hs.refCount.Done()

			select {
			// waiting for Identify here will allow us to access the peer's public and observed addresses
			// that we can dial to for a hole punch.
			case <-hs.ids.IdentifyWait(conn):
			case <-hs.ctx.Done():
				return
			}

			started, err := hs.beginOrQueueDirectConnect(conn.RemotePeer())
			if err != nil {
				log.Debug("attempt to schedule DirectConnect failed", "remote_peer", conn.RemotePeer(), "err", err)
				return
			}
			if !started {
				log.Debug("coalesced DirectConnect trigger", "remote_peer", conn.RemotePeer())
				return
			}

			err = hs.runDirectConnectRequests(conn.RemotePeer())
			if err != nil {
				log.Debug("attempt to perform DirectConnect failed", "remote_peer", conn.RemotePeer(), "err", err)
			}
		}()
	}
}

func (nn *netNotifiee) Disconnected(_ network.Network, _ network.Conn) {}
func (nn *netNotifiee) Listen(_ network.Network, _ ma.Multiaddr)       {}
func (nn *netNotifiee) ListenClose(_ network.Network, _ ma.Multiaddr)  {}
