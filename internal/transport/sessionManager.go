package transport

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

type SessionManager struct {
	sessions map[peer.ID]*Session
	mu       sync.RWMutex
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: map[peer.ID]*Session{},
	}
}

func (m *SessionManager) Load(peerID peer.ID) (session *Session, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := m.sessions[peerID]
	if s == nil {
		return nil, false
	}
	return s, true
}

func (m *SessionManager) Store(peerID peer.ID, newSessoin *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[peerID] = newSessoin
}

func (m *SessionManager) LoadOrStore(peerID peer.ID, newSessoin *Session) (session *Session, loaded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	session = m.sessions[peerID]
	if session != nil {
		return session, true
	}
	m.sessions[peerID] = newSessoin
	return newSessoin, false
}

func (m *SessionManager) Delete(peerID peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, peerID)
}

func (m *SessionManager) PeerIDs() []peer.ID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	peerIDs := make([]peer.ID, 0, len(m.sessions))
	for i := range m.sessions {
		peerIDs = append(peerIDs, i)
	}
	return peerIDs
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
