package relay

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

// RunClient is the main client entry point with automatic reconnection.
func RunClient(host string, port int, password string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly bool, tlsInsecure bool) error {

	// Determine role and detect TUN mode.
	role := ""
	isTUN := strings.Contains(listenSpec, "/")

	if isTUN {
		if forwardPort > 0 {
			return fmt.Errorf("TUN mode (-l with CIDR) cannot be used with -f")
		}
		role = "listener"
	} else if forwardPort > 0 && listenSpec == "" {
		role = "forwarder"
	} else if listenSpec != "" && forwardPort == 0 {
		role = "listener"
	} else {
		return fmt.Errorf("-f or -l must be specified and cannot be used simultaneously")
	}

	serverAddr := net.JoinHostPort(host, strconv.Itoa(port))

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	actualIndex := wantIndex
	for {
		assignedIndex, err := runOnce(serverAddr, host, password, role, forwardPort, listenSpec, actualIndex, udpOnly, tlsInsecure, isTUN)
		if err == nil {
			return nil
		}
		actualIndex = assignedIndex
		log.Printf("client disconnected: %v, reconnecting in %v...", err, backoff)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runOnce executes one complete client lifecycle.
// Returns the assigned index (for reconnection stability) and an error.
func runOnce(serverAddr, host, password, role string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly, tlsInsecure, isTUN bool) (int, error) {

	// 1. Establish TCP / TLS connection
	var conn net.Conn
	var err error
	if tlsInsecure {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         host,
		}
		conn, err = tls.Dial("tcp", serverAddr, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", serverAddr)
	}
	if err != nil {
		return 0, fmt.Errorf("dial error: %w", err)
	}

	// 2. Wrap into a Tunnel (implements FrameReadWriter) and then a Session.
	tun := tunnel.NewTunnel(conn)
	sess := session.NewSession(tun, 90*time.Second)
	defer sess.Close()

	// 3. Keep-alive
	StartKeepAlive(sess, 30*time.Second)

	// 4. Authenticate
	if err := sess.Send(protocol.TypeAuth, []byte(password)); err != nil {
		return wantIndex, fmt.Errorf("auth send error: %w", err)
	}
	if _, err := waitForFrame(sess, protocol.TypeAuthOK, 10*time.Second); err != nil {
		return wantIndex, fmt.Errorf("auth failed: %w", err)
	}
	log.Println("auth pass")

	// 5. Register the role
	var regPayload string
	if isTUN {
		regPayload, _ = configureTUNClient(listenSpec, wantIndex)
	} else {
		regPayload = role
		if wantIndex >= 0 {
			regPayload = fmt.Sprintf("%s,%d", role, wantIndex)
		}
	}

	if err := sess.Send(protocol.TypeRegister, []byte(regPayload)); err != nil {
		return wantIndex, fmt.Errorf("register send error: %w", err)
	}

	regFrame, err := waitForFrame(sess, protocol.TypeRegOK, 10*time.Second)
	if err != nil {
		return wantIndex, fmt.Errorf("registration error: %w", err)
	}

	// Parse reply: "<index>" (normal) or "<index>,<assigned_ip>" (TUN)
	regReply := string(regFrame.Payload)
	assignedIndex := 0
	var assignedIP string
	if isTUN {
		replyParts := strings.SplitN(regReply, ",", 2)
		assignedIndex, _ = strconv.Atoi(replyParts[0])
		if len(replyParts) > 1 {
			assignedIP = replyParts[1]
		}
	} else {
		assignedIndex, _ = strconv.Atoi(regReply)
	}
	log.Printf("sign pass, assigned index = %d", assignedIndex)

	// 6. Build the appropriate Mode and run it (polymorphic dispatch).
	mode, err := newClientMode(role, listenSpec, forwardPort, assignedIndex, assignedIP, udpOnly, isTUN)
	if err != nil {
		return assignedIndex, err
	}
	return assignedIndex, mode.Run(sess, assignedIndex)
}

// newClientMode creates the Mode for the client side, resolving addresses
// against the assigned index.
func newClientMode(role, listenSpec string, forwardPort, index int, assignedIP string, udpOnly, isTUN bool) (Mode, error) {
	switch role {
	case "listener":
		if isTUN {
			return &tunListenMode{cidr: listenSpec, assignedIP: assignedIP}, nil
		}
		port, err := resolvePort(listenSpec, index)
		if err != nil {
			return nil, fmt.Errorf("resolve port: %w", err)
		}
		addr := fmt.Sprintf(":%d", port)
		if udpOnly {
			return udpListenMode{addr: addr}, nil
		}
		return tcpListenMode{addr: addr}, nil
	case "forwarder":
		target := fmt.Sprintf("127.0.0.1:%d", forwardPort)
		if udpOnly {
			return udpForwardMode{target: target}, nil
		}
		return tcpForwardMode{target: target}, nil
	default:
		return nil, fmt.Errorf("unknown role: %s", role)
	}
}

// waitForFrame reads from the session's frame channel and returns the first
// frame matching the expected type. It returns an error if the session
// closes or the timeout is exceeded.
func waitForFrame(sess *session.Session, typ byte, timeout time.Duration) (*protocol.Frame, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case frame, ok := <-sess.FrameCh():
			if !ok {
				return nil, fmt.Errorf("session closed")
			}
			if frame.Type == typ {
				return frame, nil
			}
		case <-timer.C:
			return nil, fmt.Errorf("timeout waiting for frame 0x%x", typ)
		}
	}
}

// resolvePort calculates the actual listening port based on the spec and assigned index.
func resolvePort(spec string, index int) (int, error) {
	if !strings.Contains(spec, ",") {
		base, err := strconv.Atoi(spec)
		if err != nil {
			return 0, err
		}
		return base + index, nil
	}
	parts := strings.Split(spec, ",")
	ports := make([]int, len(parts))
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return 0, err
		}
		ports[i] = v
	}
	if index < len(ports) {
		return ports[index], nil
	}
	last := ports[len(ports)-1]
	return last + (index - len(ports) + 1), nil
}
