package holepunch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	ma "github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

func TestDirectConnectValidatesExistingDirectConnection(t *testing.T) {
	tests := []struct {
		name       string
		validation error
		wantClosed bool
	}{
		{
			name: "healthy direct remains connected",
		},
		{
			name:       "stale direct is retired before DCUtR continues",
			validation: errors.New("direct ping timeout"),
			wantClosed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ids identify.IDService
			local, err := libp2p.New(libp2p.WithFxOption(fx.Invoke(
				func(service identify.IDService) {
					ids = service
				},
			)))
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()

			remote, err := libp2p.New()
			if err != nil {
				t.Fatal(err)
			}
			defer remote.Close()

			if err := local.Connect(context.Background(), peer.AddrInfo{
				ID:    remote.ID(),
				Addrs: remote.Addrs(),
			}); err != nil {
				t.Fatal(err)
			}

			conns := local.Network().ConnsToPeer(remote.ID())
			if len(conns) == 0 {
				t.Fatal("expected at least one direct connection")
			}

			if ids == nil {
				t.Fatal("identify service was not populated")
			}
			validationCalls := 0
			service, err := holepunch.NewService(
				local,
				ids,
				func() []ma.Multiaddr {
					return []ma.Multiaddr{ma.StringCast("/ip4/8.8.8.8/tcp/4001")}
				},
				holepunch.WithDirectConnectionValidator(
					func(context.Context, network.Conn) error {
						validationCalls++
						return tt.validation
					},
				),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()

			_ = service.DirectConnect(remote.ID())
			if validationCalls == 0 {
				t.Fatal("expected the existing direct connection to be validated")
			}
			for _, direct := range conns {
				if direct.IsClosed() != tt.wantClosed {
					t.Fatalf("direct %s closed = %v, want %v", direct.ID(), direct.IsClosed(), tt.wantClosed)
				}
			}
		})
	}
}
