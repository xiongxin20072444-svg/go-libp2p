package holepunch

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestDirectConnectTriggerCoalescesOnePendingAttempt(t *testing.T) {
	target := peer.ID("target")
	hp := &holePuncher{
		active:  make(map[peer.ID]struct{}),
		pending: make(map[peer.ID]struct{}),
	}

	started, err := hp.beginOrQueueDirectConnect(target)
	if err != nil {
		t.Fatal(err)
	}
	if !started {
		t.Fatal("first trigger must start the attempt")
	}

	for i := 0; i < 8; i++ {
		started, err = hp.beginOrQueueDirectConnect(target)
		if err != nil {
			t.Fatal(err)
		}
		if started {
			t.Fatalf("duplicate trigger %d started a concurrent attempt", i)
		}
	}

	if !hp.finishDirectConnect(target) {
		t.Fatal("latest trigger must be consumed after the active attempt finishes")
	}
	if !hp.isDirectConnectActive(target) {
		t.Fatal("peer must remain active while the coalesced attempt runs")
	}
	for i := 0; i < 8; i++ {
		started, err = hp.beginOrQueueDirectConnect(target)
		if err != nil {
			t.Fatal(err)
		}
		if started {
			t.Fatalf("trigger %d started a concurrent coalesced attempt", i)
		}
	}

	if hp.finishDirectConnect(target) {
		t.Fatal("triggers received during the coalesced attempt must not create an unbounded retry")
	}
	if hp.isDirectConnectActive(target) {
		t.Fatal("peer must be released after the coalesced attempt finishes")
	}
}
