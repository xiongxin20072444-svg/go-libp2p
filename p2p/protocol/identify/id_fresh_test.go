package identify

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	blhost "github.com/libp2p/go-libp2p/p2p/host/blank"
	swarmt "github.com/libp2p/go-libp2p/p2p/net/swarm/testing"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
	"github.com/libp2p/go-msgio/pbio"
	"github.com/stretchr/testify/require"
)

func TestIdentifyFreshRunsAgainOnTheSameConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	var requests atomic.Int32
	const freshProtocol = protocol.ID("/wxb/fresh-identify-snapshot/1.0.0")
	initialWait := sourceIDs.IdentifyWait(conn)
	previousProtocolVersion, err := source.Peerstore().Get(target.ID(), "ProtocolVersion")
	require.NoError(t, err)
	previousAddrs := source.Peerstore().Addrs(target.ID())
	require.NoError(t, targetIDs.Close())
	events, err := source.EventBus().Subscribe([]any{
		new(event.EvtPeerIdentificationCompleted),
		new(event.EvtPeerProtocolsUpdated),
	})
	require.NoError(t, err)
	defer events.Close()
	target.SetStreamHandler(ID, func(stream network.Stream) {
		requests.Add(1)
		writer := pbio.NewDelimitedWriter(stream)
		freshVersion := "fresh-version-must-not-reach-peerstore"
		_ = writer.WriteMsg(&pb.Identify{
			Protocols:       []string{string(freshProtocol)},
			ProtocolVersion: &freshVersion,
		})
		_ = stream.Close()
	})

	snapshot, err := sourceIDs.IdentifyConnFresh(ctx, conn)
	require.NoError(t, err)
	require.Equal(t, target.ID(), snapshot.PeerID)
	require.Equal(t, conn.ID(), snapshot.ConnectionID)
	require.Equal(t, []protocol.ID{freshProtocol}, snapshot.Protocols)
	require.EqualValues(t, 1, requests.Load())
	require.Equal(t, initialWait, sourceIDs.IdentifyWait(conn), "fresh exchange must not replace IdentifyWait state")
	supported, err := source.Peerstore().SupportsProtocols(target.ID(), freshProtocol)
	require.NoError(t, err)
	require.Empty(t, supported, "fresh snapshot must not mutate the peerstore")
	protocolVersion, err := source.Peerstore().Get(target.ID(), "ProtocolVersion")
	require.NoError(t, err)
	require.Equal(t, previousProtocolVersion, protocolVersion)
	require.ElementsMatch(t, previousAddrs, source.Peerstore().Addrs(target.ID()))
	select {
	case received := <-events.Out():
		t.Fatalf("fresh snapshot emitted Identify event: %T", received)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestIdentifyFreshCancellationDoesNotCommitResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})
	var requestStartedOnce sync.Once
	const freshProtocol = protocol.ID("/wxb/fresh-identify-cancelled/1.0.0")
	target.SetStreamHandler(ID, func(stream network.Stream) {
		requestStartedOnce.Do(func() { close(requestStarted) })
		<-releaseResponse
		writer := pbio.NewDelimitedWriter(stream)
		_ = writer.WriteMsg(&pb.Identify{Protocols: []string{string(freshProtocol)}})
		_ = stream.Close()
	})

	freshCtx, freshCancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(freshCtx, conn); result <- err }()
	<-requestStarted
	freshCancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("fresh identify did not stop after context cancellation")
	}
	close(releaseResponse)

	supported, err := source.Peerstore().SupportsProtocols(target.ID(), freshProtocol)
	require.NoError(t, err)
	require.Empty(t, supported, "cancelled response must not mutate the peerstore")
}

func TestIdentifyFreshRejectsMalformedSnapshotWithoutPartialPeerstoreWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	const rejectedProtocol = protocol.ID("/wxb/fresh-identify-malformed/1.0.0")
	target.SetStreamHandler(ID, func(stream network.Stream) {
		writer := pbio.NewDelimitedWriter(stream)
		_ = writer.WriteMsg(&pb.Identify{
			Protocols:   []string{string(rejectedProtocol)},
			ListenAddrs: [][]byte{{0xff}},
		})
		_ = stream.Close()
	})

	snapshot, err := sourceIDs.IdentifyConnFresh(ctx, conn)
	require.Error(t, err)
	require.Equal(t, FreshIdentifySnapshot{}, snapshot)
	supported, err := source.Peerstore().SupportsProtocols(target.ID(), rejectedProtocol)
	require.NoError(t, err)
	require.Empty(t, supported)
}

func TestIdentifyFreshRejectsClosedConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	require.NoError(t, conn.Close())
	require.Eventually(t, conn.IsClosed, time.Second, time.Millisecond)
	_, err := sourceIDs.IdentifyConnFresh(ctx, conn)
	require.ErrorIs(t, err, ErrIdentifyConnectionUnavailable)
}

func TestIdentifyFreshSerializesCallsPerConnectionAndWaitingCallCanCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int32
	target.SetStreamHandler(ID, func(stream network.Stream) {
		if requests.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		targetIDs.handleIdentifyRequest(stream)
	})

	firstResult := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(ctx, conn); firstResult <- err }()
	<-firstStarted

	waitingCtx, waitingCancel := context.WithCancel(ctx)
	secondResult := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(waitingCtx, conn); secondResult <- err }()
	time.Sleep(20 * time.Millisecond)
	require.EqualValues(t, 1, requests.Load(), "same-connection fresh identify calls must not overlap")
	waitingCancel()

	select {
	case err := <-secondResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("waiting fresh identify did not honor cancellation")
	}
	close(releaseFirst)
	require.NoError(t, <-firstResult)
	require.EqualValues(t, 1, requests.Load())
}

func TestIdentifyFreshDoesNotSerializeDifferentConnections(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source := blhost.NewBlankHost(swarmt.GenSwarm(t))
	firstTarget := blhost.NewBlankHost(swarmt.GenSwarm(t))
	secondTarget := blhost.NewBlankHost(swarmt.GenSwarm(t))
	defer source.Close()
	defer firstTarget.Close()
	defer secondTarget.Close()
	sourceIDs, err := NewIDService(source)
	require.NoError(t, err)
	firstIDs, err := NewIDService(firstTarget)
	require.NoError(t, err)
	secondIDs, err := NewIDService(secondTarget)
	require.NoError(t, err)
	defer sourceIDs.Close()
	defer firstIDs.Close()
	defer secondIDs.Close()
	sourceIDs.Start()
	firstIDs.Start()
	secondIDs.Start()

	firstConn := connectAndWaitForIdentify(t, ctx, source, sourceIDs, firstTarget)
	secondConn := connectAndWaitForIdentify(t, ctx, source, sourceIDs, secondTarget)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstStartedOnce sync.Once
	firstTarget.SetStreamHandler(ID, func(stream network.Stream) {
		firstStartedOnce.Do(func() { close(firstStarted) })
		<-releaseFirst
		firstIDs.handleIdentifyRequest(stream)
	})
	secondTarget.SetStreamHandler(ID, secondIDs.handleIdentifyRequest)

	firstResult := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(ctx, firstConn); firstResult <- err }()
	<-firstStarted
	secondResult := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(ctx, secondConn); secondResult <- err }()
	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("fresh identify on an independent connection was globally blocked")
	}
	close(releaseFirst)
	require.NoError(t, <-firstResult)
}

func TestIdentifyFreshServiceCloseCancelsActiveAndWaitingCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer targetIDs.Close()

	requestStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var requestStartedOnce sync.Once
	target.SetStreamHandler(ID, func(stream network.Stream) {
		requestStartedOnce.Do(func() { close(requestStarted) })
		<-releaseHandler
		targetIDs.handleIdentifyRequest(stream)
	})

	activeResult := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(ctx, conn); activeResult <- err }()
	<-requestStarted
	waitingResult := make(chan error, 1)
	go func() { _, err := sourceIDs.IdentifyConnFresh(ctx, conn); waitingResult <- err }()
	closeResult := make(chan error, 1)
	go func() { closeResult <- sourceIDs.Close() }()

	for name, result := range map[string]<-chan error{
		"active":  activeResult,
		"waiting": waitingResult,
		"close":   closeResult,
	} {
		select {
		case err := <-result:
			if name == "close" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrIdentifyConnectionUnavailable)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s operation did not stop when Identify service closed", name)
		}
	}
	close(releaseHandler)
}

func TestIdentifyFreshDeliveryRejectsConnectionClosedAfterRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	binding := freshIdentifyConnectionBinding{
		connection:   conn,
		connectionID: conn.ID(),
		localPeer:    conn.LocalPeer(),
		remotePeer:   conn.RemotePeer(),
	}
	sourceIDs.connsMu.RLock()
	binding.state = sourceIDs.conns[conn].FreshState
	sourceIDs.connsMu.RUnlock()
	require.NoError(t, conn.Close())
	require.Eventually(t, conn.IsClosed, time.Second, time.Millisecond)
	snapshot, err := sourceIDs.deliverFreshIdentifySnapshot(
		ctx,
		ctx,
		func() bool { return true },
		&freshIdentifyDeliveryState{},
		FreshIdentifySnapshot{},
		binding,
	)
	require.ErrorIs(t, err, ErrIdentifyConnectionUnavailable)
	require.Equal(t, FreshIdentifySnapshot{}, snapshot)
}

func TestIdentifyFreshDeliveryRejectsReplacedConnectionStateABA(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	sourceIDs.connsMu.Lock()
	oldEntry := sourceIDs.conns[conn]
	binding := freshIdentifyConnectionBinding{
		connection:   conn,
		connectionID: conn.ID(),
		localPeer:    conn.LocalPeer(),
		remotePeer:   conn.RemotePeer(),
		state:        oldEntry.FreshState,
	}
	delete(sourceIDs.conns, conn)
	oldEntry.FreshState.gate.Lock()
	oldEntry.FreshState.retired = true
	oldEntry.FreshState.cancel()
	oldEntry.FreshState.gate.Unlock()
	sourceIDs.addConnWithLock(conn)
	replacement := sourceIDs.conns[conn].FreshState
	sourceIDs.connsMu.Unlock()
	require.NotSame(t, binding.state, replacement)

	snapshot, err := sourceIDs.deliverFreshIdentifySnapshot(
		ctx,
		ctx,
		func() bool { return true },
		&freshIdentifyDeliveryState{},
		FreshIdentifySnapshot{PeerID: target.ID(), ConnectionID: conn.ID()},
		binding,
	)
	require.ErrorIs(t, err, ErrIdentifyConnectionUnavailable)
	require.Equal(t, FreshIdentifySnapshot{}, snapshot)
}

func TestIdentifyFreshDeliveryCancellationAndSnapshotAreMutuallyExclusive(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	source, target, sourceIDs, targetIDs, conn := newFreshIdentifyPair(t, ctx)
	defer source.Close()
	defer target.Close()
	defer sourceIDs.Close()
	defer targetIDs.Close()

	sourceIDs.connsMu.RLock()
	state := sourceIDs.conns[conn].FreshState
	sourceIDs.connsMu.RUnlock()
	binding := freshIdentifyConnectionBinding{
		connection:   conn,
		connectionID: conn.ID(),
		localPeer:    conn.LocalPeer(),
		remotePeer:   conn.RemotePeer(),
		state:        state,
	}
	request, cancelRequest := context.WithCancel(ctx)
	delivery := &freshIdentifyDeliveryState{}
	callbackStarted := make(chan struct{})
	callbackDone := make(chan struct{})
	stopCancellation := context.AfterFunc(request, func() {
		close(callbackStarted)
		state.gate.Lock()
		delivery.cancelled = true
		state.gate.Unlock()
		close(callbackDone)
	})
	wanted := FreshIdentifySnapshot{PeerID: target.ID(), ConnectionID: conn.ID()}
	state.gate.Lock()
	result := make(chan struct {
		snapshot FreshIdentifySnapshot
		err      error
	}, 1)
	go func() {
		snapshot, err := sourceIDs.deliverFreshIdentifySnapshot(
			request,
			request,
			stopCancellation,
			delivery,
			wanted,
			binding,
		)
		result <- struct {
			snapshot FreshIdentifySnapshot
			err      error
		}{snapshot: snapshot, err: err}
	}()
	cancelRequest()
	<-callbackStarted
	state.gate.Unlock()
	got := <-result
	<-callbackDone
	snapshot, err := got.snapshot, got.err
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, FreshIdentifySnapshot{}, snapshot)
}

func newFreshIdentifyPair(
	t *testing.T,
	ctx context.Context,
) (*blhost.BlankHost, *blhost.BlankHost, *idService, *idService, network.Conn) {
	t.Helper()
	source := blhost.NewBlankHost(swarmt.GenSwarm(t))
	target := blhost.NewBlankHost(swarmt.GenSwarm(t))
	sourceIDsRaw, err := NewIDService(source)
	require.NoError(t, err)
	targetIDsRaw, err := NewIDService(target)
	require.NoError(t, err)
	sourceIDs := sourceIDsRaw
	targetIDs := targetIDsRaw
	sourceIDs.Start()
	targetIDs.Start()
	conn := connectAndWaitForIdentify(t, ctx, source, sourceIDs, target)
	return source, target, sourceIDs, targetIDs, conn
}

func connectAndWaitForIdentify(
	t *testing.T,
	ctx context.Context,
	source *blhost.BlankHost,
	sourceIDs *idService,
	target *blhost.BlankHost,
) network.Conn {
	t.Helper()
	require.NoError(t, source.Connect(ctx, peer.AddrInfo{ID: target.ID(), Addrs: target.Addrs()}))
	connections := source.Network().ConnsToPeer(target.ID())
	require.NotEmpty(t, connections)
	for _, candidate := range connections {
		select {
		case <-sourceIDs.IdentifyWait(candidate):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	return connections[0]
}
