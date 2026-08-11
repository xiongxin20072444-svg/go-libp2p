package network

import (
	"context"
	"testing"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestDefaultTimeout(t *testing.T) {
	ctx := context.Background()
	dur := GetDialPeerTimeout(ctx)
	if dur != DialPeerTimeout {
		t.Fatal("expected default peer timeout")
	}
}

func TestNonDefaultTimeout(t *testing.T) {
	customTimeout := time.Duration(1)
	ctx := context.WithValue(
		context.Background(),
		dialPeerTimeoutCtxKey{},
		customTimeout,
	)
	dur := GetDialPeerTimeout(ctx)
	if dur != customTimeout {
		t.Fatal("peer timeout doesn't match set timeout")
	}
}

func TestSettingTimeout(t *testing.T) {
	customTimeout := time.Duration(1)
	ctx := WithDialPeerTimeout(
		context.Background(),
		customTimeout,
	)
	dur := GetDialPeerTimeout(ctx)
	if dur != customTimeout {
		t.Fatal("peer timeout doesn't match set timeout")
	}
}

func TestSimultaneousConnect(t *testing.T) {
	t.Run("for the server", func(t *testing.T) {
		serverCtx := WithSimultaneousConnect(context.Background(), false, "foobar")
		ok, isClient, reason := GetSimultaneousConnect(serverCtx)
		require.True(t, ok)
		require.False(t, isClient)
		require.Equal(t, "foobar", reason)
	})
	t.Run("for the client", func(t *testing.T) {
		serverCtx := WithSimultaneousConnect(context.Background(), true, "foo")
		ok, isClient, reason := GetSimultaneousConnect(serverCtx)
		require.True(t, ok)
		require.True(t, isClient)
		require.Equal(t, "foo", reason)
	})
}

func TestDialPeerAddrsContextCopiesAddresses(t *testing.T) {
	input := []ma.Multiaddr{ma.StringCast("/ip4/192.0.2.1/tcp/4001")}
	ctx := WithDialPeerAddrs(context.Background(), input)
	input[0] = ma.StringCast("/ip4/192.0.2.2/tcp/4001")

	got, ok := GetDialPeerAddrs(ctx)
	if !ok || len(got) != 1 || got[0].String() != "/ip4/192.0.2.1/tcp/4001" {
		t.Fatalf("dial addresses = %v, present=%t", got, ok)
	}
	got[0] = ma.StringCast("/ip4/192.0.2.3/tcp/4001")
	again, ok := GetDialPeerAddrs(ctx)
	if !ok || again[0].String() != "/ip4/192.0.2.1/tcp/4001" {
		t.Fatalf("context addresses changed through result mutation: %v", again)
	}
}
