package transport

import (
	"context"
	"p2pchat/internal/event"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type SessionManager struct {
	sessions map[peer.ID]*Session
	mu       sync.RWMutex
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[peer.ID]*Session),
	}
}

func (m *SessionManager) GetOrCreate(ctx context.Context, host host.Host, bus *event.EventBus, peerID peer.ID) (session *Session, loaded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session = m.sessions[peerID]
	if session != nil {
		return session, true
	}

	session = NewSession(ctx, host, bus, peerID)
	m.sessions[peerID] = session
	return
}

func (m *SessionManager) Delete(peerID peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, peerID)
}

func (m *SessionManager) Sessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

func (m *SessionManager) Range(f func(peerID peer.ID, s *Session) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for peerID, s := range m.sessions {
		ok := f(peerID, s)
		if !ok {
			break
		}
	}
}

func (m *SessionManager) Clear() {
	m.mu.TryLock()
	defer m.mu.Unlock()
	clear(m.sessions)
}
