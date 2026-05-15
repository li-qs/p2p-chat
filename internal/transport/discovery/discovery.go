package discovery

import (
	"context"
	"io"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
)

const Rendezvous = "p2p-chat/dev/1.0"

type Notifee struct {
	OnPeerFound func(pi peer.AddrInfo)
}

func (n *Notifee) HandlePeerFound(pi peer.AddrInfo) {
	if n.OnPeerFound != nil {
		n.OnPeerFound(pi)
	}
}

type MdnsService struct {
	mdns io.Closer
}

func InitMdns(ctx context.Context, host host.Host) (*MdnsService, error) {
	service := mdns.NewMdnsService(host, Rendezvous, &Notifee{
		OnPeerFound: func(pi peer.AddrInfo) {
			_ = host.Connect(ctx, pi)
		},
	})
	if err := service.Start(); err != nil {
		return nil, err
	}
	return &MdnsService{
		mdns: service,
	}, nil
}

func (s *MdnsService) Close() error {
	return s.mdns.Close()
}
