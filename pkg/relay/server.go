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
	forwarders   map[int][]*tunnel.Tunnel
	nextIndex    int
	maxListeners int
	password     string
	tunCtx       *ServerTUNContext // non-nil when TUN mode is enabled
}

func NewServer(password string, maxListeners int) *Server {
	return &Server{
		listeners:    make(map[int]*tunnel.Tunnel),
		forwarders:   make(map[int][]*tunnel.Tunnel),
		password:     password,
		maxListeners: maxListeners,
	}
}

// StartTUNMode initializes TUN mode: creates the server TUN device and starts
// the background reader goroutine that routes packets to connected clients.
func (s *Server) StartTUNMode() error {
	ctx, err := NewServerTUNContext()
	if err != nil {
		return err
	}
	s.tunCtx = ctx
	log.Printf("[server] TUN mode enabled, device=%s", ctx.DevName())
	return nil
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

	payload := string(frame.Payload)
	parts := strings.Split(payload, ",")

	role := parts[0]
	wantIndex := -1
	isTUN := false
	var tunCIDR string

	// Parse registration payload.
	// Normal:  listener[,<index>]
	// TUN:     listener[,<index>],tun,<cidr>
	if len(parts) > 1 {
		if _, err := fmt.Sscanf(parts[1], "%d", &wantIndex); err == nil {
			if len(parts) > 2 && parts[2] == "tun" && len(parts) > 3 {
				isTUN = true
				tunCIDR = parts[3]
			}
		} else if parts[1] == "tun" {
			isTUN = true
			if len(parts) > 2 {
				tunCIDR = parts[2]
			}
		}
	}

	if isTUN && s.tunCtx == nil {
		t.Send(protocol.TypeError, []byte("server not in TUN mode (use -t)"))
		return
	}

	var index int
	s.mu.Lock()
	switch role {
	case "listener":
		if isTUN {
			// TUN listener registration.
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
			assignedIP, err := s.tunCtx.RegisterClient(t, index, tunCIDR)
			if err != nil {
				s.mu.Unlock()
				t.Send(protocol.TypeError, []byte(err.Error()))
				return
			}
			s.listeners[index] = t
			s.mu.Unlock()

			reply := fmt.Sprintf("%d,%s", index, assignedIP)
			t.Send(protocol.TypeRegOK, []byte(reply))
			log.Printf("[server] tun-listener registered at index=%d, IP=%s", index, assignedIP)

			serverTUNRelayLoop(t, s.tunCtx, index)
			return
		}

		// Normal listener.
		if wantIndex >= 0 {
			index = wantIndex
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
		// Multiple forwarders are allowed at the same index. Append to the list.
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
		s.forwarders[index] = append(s.forwarders[index], t)
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

		if role == "listener" {
			// Broadcast to all forwarders at this index, filtering out dead ones.
			s.mu.Lock()
			peers := s.forwarders[index]
			alive := peers[:0]
			for _, p := range peers {
				if p.Send(frame.Type, frame.Payload) == nil {
					alive = append(alive, p)
				}
			}
			if len(alive) != len(peers) {
				s.forwarders[index] = alive
			}
			s.mu.Unlock()
		} else {
			// Forwarder sends to the single listener.
			s.mu.Lock()
			peer := s.listeners[index]
			s.mu.Unlock()

			if peer == nil {
				continue
			}

			if err := peer.Send(frame.Type, frame.Payload); err != nil {
				break
			}
		}
	}

	// Cleanup.
	s.mu.Lock()
	if role == "listener" {
		log.Printf("[server] relayLoop listener[%d]: cleaning up, forwarders=%d", index, len(s.forwarders[index]))
		delete(s.listeners, index)
		// Notify all forwarders at this index and clear the slice.
		for _, fwd := range s.forwarders[index] {
			fwd.Send(protocol.TypePeerLeave, nil)
		}
		delete(s.forwarders, index)
	} else {
		log.Printf("[server] relayLoop forwarder[%d]: cleaning up, listener present=%v", index, s.listeners[index] != nil)
		// Remove this specific forwarder from the slice.
		fwdList := s.forwarders[index]
		for i, f := range fwdList {
			if f == t {
				s.forwarders[index] = append(fwdList[:i], fwdList[i+1:]...)
				break
			}
		}
		// Notify listener that a forwarder left (only if no more forwarders remain).
		if len(s.forwarders[index]) == 0 {
			if lis, ok := s.listeners[index]; ok {
				lis.Send(protocol.TypePeerLeave, nil)
			}
		}
	}
	s.mu.Unlock()
}
