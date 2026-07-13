package relay

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/Fomak-1012/CloudMirror/pkg/auth"
	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

type Server struct {
	peers       *PeerMap
	nextIndex   int
	maxListeners int
	password    string
	tunCtx      *ServerTUNContext // non-nil when TUN mode is enabled
}

func NewServer(password string, maxListeners int) *Server {
	return &Server{
		peers:        NewPeerMap(),
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
	switch role {
	case "listener":
		if isTUN {
			if wantIndex >= 0 {
				index = wantIndex
			} else {
				index = s.nextIndex
				s.nextIndex++
			}
			if s.maxListeners > 0 && s.peers.ListenerCount() >= s.maxListeners {
				t.Send(protocol.TypeError, []byte("too many listeners"))
				return
			}
			assignedIP, err := s.tunCtx.RegisterClient(t, index, tunCIDR)
			if err != nil {
				t.Send(protocol.TypeError, []byte(err.Error()))
				return
			}
			s.peers.RegisterListener(index, t)

			reply := fmt.Sprintf("%d,%s", index, assignedIP)
			t.Send(protocol.TypeRegOK, []byte(reply))
			log.Printf("[server] tun-listener registered at index=%d, IP=%s", index, assignedIP)

			serverTUNRelayLoop(t, s.tunCtx, index)
			return
		}

		// Normal listener.
		if wantIndex >= 0 {
			index = wantIndex
		} else {
			index = s.nextIndex
			s.nextIndex++
		}
		if s.maxListeners > 0 && s.peers.ListenerCount() >= s.maxListeners {
			t.Send(protocol.TypeError, []byte("too many listeners"))
			return
		}
		s.peers.RegisterListener(index, t)
	case "forwarder":
		if wantIndex >= 0 {
			index = wantIndex
		} else {
			indices := s.peers.ListenerIndices()
			if len(indices) == 0 {
				t.Send(protocol.TypeError, []byte("no listener available yet"))
				return
			}
			if len(indices) > 1 {
				t.Send(protocol.TypeError, []byte("forwarder must specify index when multiple listeners exist"))
				return
			}
			index = indices[0]
		}
		s.peers.RegisterForwarder(index, t)
	}
	t.Send(protocol.TypeRegOK, []byte(fmt.Sprintf("%d", index)))
	log.Printf("[server] %s registered at index=%d", role, index)

	s.relayLoop(t, role, index)
}

func (s *Server) relayLoop(conn protocol.FrameReadWriter, role string, index int) {
	log.Printf("[server] relayLoop %s[%d]: started", role, index)
	for {
		frame, err := conn.Receive()
		if err != nil {
			log.Printf("[server] relayLoop %s[%d]: recv error: %v — exiting loop", role, index, err)
			break
		}

		if role == "listener" {
			s.peers.BroadcastFromListener(index, frame.Type, frame.Payload)
		} else {
			if err := s.peers.SendToListener(index, frame.Type, frame.Payload); err != nil {
				break
			}
		}
	}

	// Cleanup.
	log.Printf("[server] relayLoop %s[%d]: cleaning up", role, index)
	if role == "listener" {
		s.peers.UnregisterListener(index)
	} else {
		s.peers.UnregisterForwarder(index, conn)
	}
}
