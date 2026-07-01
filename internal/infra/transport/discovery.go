package transport

import (
	"context"
	"io"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const rendezvous = "p2p-chat/dev/1.0"

type notifee struct {
	OnPeerFound func(pi peer.AddrInfo)
}

func (n *notifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.OnPeerFound != nil {
		n.OnPeerFound(pi)
	}
}

type discovery struct {
	mdns io.Closer
}

func initDiscovery(ctx context.Context, host host.Host) (*discovery, error) {
	service := mdns.NewMdnsService(host, rendezvous, &notifee{
		OnPeerFound: func(pi peer.AddrInfo) {
			_ = host.Connect(ctx, pi)
		},
	})
	if err := service.Start(); err != nil {
		return nil, err
	}
	return &discovery{
		mdns: service,
	}, nil
}

func (s *discovery) close() error {
	return s.mdns.Close()
}
