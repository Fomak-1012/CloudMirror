package relay

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/auth"
	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

type Server struct {
	mu           sync.Mutex
	listeners    map[int]*tunnel.Tunnel
	forwarders   map[int]*tunnel.Tunnel
	nextIndex    int
	maxListeners int
	password     string
}

func NewServer(password string, maxListeners int) *Server {
	return &Server{
		listeners:    make(map[int]*tunnel.Tunnel),
		forwarders:   make(map[int]*tunnel.Tunnel),
		password:     password,
		maxListeners: maxListeners,
	}
}

func (s *Server) HandleClient(conn net.Conn) {
	log.Printf("[server] HandleClient: new connection from %s", conn.RemoteAddr())
	t := tunnel.NewTunnel(conn)
	defer func() {
		log.Printf("[server] HandleClient: closing tunnel from %s", conn.RemoteAddr())
		t.Close()
	}()

	if err := auth.ServerAuth(t, s.password); err != nil {
		return
	}

	frame, err := t.Receive()
	if err != nil || frame.Type != protocol.TypeRegister {
		return
	}

	parts := strings.SplitN(string(frame.Payload), ",", 2)
	role := parts[0]
	wantIndex := -1
	if len(parts) > 1 {
		fmt.Sscanf(parts[1], "%d", &wantIndex)
	}

	var index int
	s.mu.Lock()
	switch role {
	case "listener":
		if wantIndex >= 0 {
			index = wantIndex
			// Only clean up a stale listener of the same role at this index.
			// The forwarder at this index (if any) is a valid peer — leave it alone.
			if old, ok := s.listeners[index]; ok {
				old.Close()
				delete(s.listeners, index)
			}
		} else {
			index = s.nextIndex
			s.nextIndex++
		}
		if s.maxListeners > 0 && len(s.listeners) >= s.maxListeners {
			s.mu.Unlock()
			t.Send(protocol.TypeError, []byte("too many listeners"))
			return
		}
		s.listeners[index] = t
	case "forwarder":
		if wantIndex >= 0 {
			index = wantIndex
			// Only clean up a stale forwarder of the same role at this index.
			if old, ok := s.forwarders[index]; ok {
				old.Close()
				delete(s.forwarders, index)
			}
		} else {
			if len(s.listeners) == 1 {
				for i := range s.listeners {
					index = i
					break
				}
			} else if len(s.listeners) == 0 {
				s.mu.Unlock()
				t.Send(protocol.TypeError, []byte("no listener available yet"))
				return
			} else {
				s.mu.Unlock()
				t.Send(protocol.TypeError, []byte("forwarder must specify index when multiple listeners exist"))
				return
			}
		}
		s.forwarders[index] = t
	}
	s.mu.Unlock()

	t.Send(protocol.TypeRegOK, []byte(fmt.Sprintf("%d", index)))
	log.Printf("[server] %s registered at index=%d", role, index)

	s.relayLoop(t, role, index)
}

func (s *Server) relayLoop(t *tunnel.Tunnel, role string, index int) {
	log.Printf("[server] relayLoop %s[%d]: started", role, index)
	for {
		frame, err := t.Receive()
		if err != nil {
			log.Printf("[server] relayLoop %s[%d]: recv error: %v — exiting loop", role, index, err)
			break
		}
		s.mu.Lock()
		var peer *tunnel.Tunnel
		if role == "listener" {
			peer = s.forwarders[index]
		} else {
			peer = s.listeners[index]
		}
		s.mu.Unlock()

		if peer == nil {
			continue
		}

		if err := peer.Send(frame.Type, frame.Payload); err != nil {
			break
		}
	}

	// When one side disconnects, remove it and notify the peer.
	// Do NOT forcefully close the peer — it may still be valid and can
	// be re-paired when the disconnected side reconnects at the same index.
	s.mu.Lock()
	if role == "listener" {
		log.Printf("[server] relayLoop listener[%d]: cleaning up, forwarder present=%v", index, s.forwarders[index] != nil)
		delete(s.listeners, index)
		if fwd, ok := s.forwarders[index]; ok {
			fwd.Send(protocol.TypePeerLeave, nil)
		}
	} else {
		log.Printf("[server] relayLoop forwarder[%d]: cleaning up, listener present=%v", index, s.listeners[index] != nil)
		delete(s.forwarders, index)
		if lis, ok := s.listeners[index]; ok {
			lis.Send(protocol.TypePeerLeave, nil)
		}
	}
	s.mu.Unlock()
}
