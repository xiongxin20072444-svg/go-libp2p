package swarm

import (
	"context"
	"errors"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// dialWorkerFunc is used by dialSync to spawn a new dial worker
type dialWorkerFunc func(peer.ID, <-chan dialRequest)

// errConcurrentDialSuccessful is used to signal that a concurrent dial succeeded
var errConcurrentDialSuccessful = errors.New("concurrent dial successful")
var errConcurrentDialAddressSetConflict = errors.New("concurrent dial address set conflict")

// newDialSync constructs a new dialSync
func newDialSync(worker dialWorkerFunc) *dialSync {
	return &dialSync{
		dials:      make(map[peer.ID]*activeDial),
		dialWorker: worker,
	}
}

// dialSync is a dial synchronization helper that ensures that at most one dial
// to any given peer is active at any given time.
type dialSync struct {
	mutex      sync.Mutex
	dials      map[peer.ID]*activeDial
	dialWorker dialWorkerFunc
}

type activeDial struct {
	refCnt int

	ephemeral      bool
	ephemeralAddrs []ma.Multiaddr

	ctx         context.Context
	cancelCause func(error)

	reqch chan dialRequest
}

func (ad *activeDial) dial(ctx context.Context) (*Conn, error) {
	dialCtx := ad.ctx
	if ad.ephemeral {
		dialCtx = network.WithDialPeerAddrs(dialCtx, ad.ephemeralAddrs)
	}

	if forceDirect, reason := network.GetForceDirectDial(ctx); forceDirect {
		dialCtx = network.WithForceDirectDial(dialCtx, reason)
	}
	if simConnect, isClient, reason := network.GetSimultaneousConnect(ctx); simConnect {
		dialCtx = network.WithSimultaneousConnect(dialCtx, isClient, reason)
	}

	resch := make(chan dialResponse, 1)
	select {
	case ad.reqch <- dialRequest{ctx: dialCtx, resch: resch}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-resch:
		return res.conn, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ds *dialSync) getActiveDial(ctx context.Context, p peer.ID) (*activeDial, error) {
	requestedAddrs, requestedEphemeral := network.GetDialPeerAddrs(ctx)
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	actd, ok := ds.dials[p]
	if ok && !sameDialAddressMode(actd, requestedEphemeral, requestedAddrs) {
		return nil, errConcurrentDialAddressSetConflict
	}
	if !ok {
		// This code intentionally uses the background context. Otherwise, if the first call
		// to Dial is canceled, subsequent dial calls will also be canceled.
		ctx, cancel := context.WithCancelCause(context.Background())
		actd = &activeDial{
			ctx:            ctx,
			cancelCause:    cancel,
			reqch:          make(chan dialRequest),
			ephemeral:      requestedEphemeral,
			ephemeralAddrs: requestedAddrs,
		}
		go ds.dialWorker(p, actd.reqch)
		ds.dials[p] = actd
	}
	// increase ref count before dropping mutex
	actd.refCnt++
	return actd, nil
}

// Dial initiates a dial to the given peer if there are none in progress
// then waits for the dial to that peer to complete.
func (ds *dialSync) Dial(ctx context.Context, p peer.ID) (*Conn, error) {
	ad, err := ds.getActiveDial(ctx, p)
	if err != nil {
		return nil, err
	}

	conn, err := ad.dial(ctx)

	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	ad.refCnt--
	if ad.refCnt == 0 {
		if err == nil {
			ad.cancelCause(errConcurrentDialSuccessful)
		} else {
			ad.cancelCause(err)
		}
		close(ad.reqch)
		delete(ds.dials, p)
	}

	return conn, err
}

func sameDialAddressMode(active *activeDial, requestedEphemeral bool, requested []ma.Multiaddr) bool {
	if active.ephemeral != requestedEphemeral {
		return false
	}
	if !requestedEphemeral {
		return true
	}
	if len(active.ephemeralAddrs) != len(requested) {
		return false
	}
	seen := make(map[string]struct{}, len(active.ephemeralAddrs))
	for _, addr := range active.ephemeralAddrs {
		if addr == nil {
			return false
		}
		seen[string(addr.Bytes())] = struct{}{}
	}
	if len(seen) != len(active.ephemeralAddrs) {
		return false
	}
	requestedSeen := make(map[string]struct{}, len(requested))
	for _, addr := range requested {
		if addr == nil {
			return false
		}
		key := string(addr.Bytes())
		if _, duplicate := requestedSeen[key]; duplicate {
			return false
		}
		requestedSeen[key] = struct{}{}
		if _, ok := seen[key]; !ok {
			return false
		}
	}
	return true
}
