package relay

import (
	"fmt"
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
	t := tunnel.NewTunnel(conn)
	defer t.Close()

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

	s.relayLoop(t, role, index)
}

func (s *Server) relayLoop(t *tunnel.Tunnel, role string, index int) {
	for {
		frame, err := t.Receive()
		if err != nil {
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
	s.mu.Lock()
	if role == "listener" {
		delete(s.listeners, index)
		if fwd, ok := s.forwarders[index]; ok {
			fwd.Send(protocol.TypePeerLeave, nil)
		}
	} else {
		delete(s.forwarders, index)
		if lis, ok := s.listeners[index]; ok {
			lis.Send(protocol.TypePeerLeave, nil)
		}
	}
	s.mu.Unlock()
}
