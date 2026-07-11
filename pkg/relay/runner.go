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

	// Determine role.
	role := ""
	if forwardPort > 0 && listenSpec == "" {
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
		assignedIndex, err := runOnce(serverAddr, host, password, role, forwardPort, listenSpec, actualIndex, udpOnly, tlsInsecure)
		if err == nil {
			return nil // normal exit (not implemented yet)
		}
		actualIndex = assignedIndex // remember index for reconnect to avoid port drift
		log.Printf("client disconnected: %v, reconnecting in %v...", err, backoff)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runOnce executes one complete client lifecycle: connect, authenticate,
// register, and start the role‑specific forwarding loop.
// It returns the assigned index and an error. The caller should reuse the
// assigned index on reconnection to avoid port drift.
func runOnce(serverAddr, host, password, role string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly, tlsInsecure bool) (int, error) {

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
		return 0, fmt.Errorf("dial error: %v", err)
	}

	// 2. Wrap the raw connection into a Tunnel, then a Session.
	//    The Session starts a dedicated read‑pump goroutine.
	tun := tunnel.NewTunnel(conn)
	sess := session.NewSession(tun, 90*time.Second) // idle timeout
	defer sess.Close()

	// 3. Start periodic keep‑alive frames (send only, no reads).
	StartKeepAlive(sess, 30*time.Second)

	// 4. Authenticate (inline, avoids modifying the auth package for now)
	if err := sess.Send(protocol.TypeAuth, []byte(password)); err != nil {
		return wantIndex, fmt.Errorf("auth send error: %v", err)
	}
	if _, err := waitForFrame(sess, protocol.TypeAuthOK, 10*time.Second); err != nil {
		return wantIndex, fmt.Errorf("auth failed: %v", err)
	}
	log.Println("auth pass")

	// 5. Register the role
	regPayload := role
	if wantIndex >= 0 {
		regPayload = fmt.Sprintf("%s,%d", role, wantIndex)
	}
	if err := sess.Send(protocol.TypeRegister, []byte(regPayload)); err != nil {
		return wantIndex, fmt.Errorf("register send error: %v", err)
	}

	regFrame, err := waitForFrame(sess, protocol.TypeRegOK, 10*time.Second)
	if err != nil {
		return wantIndex, fmt.Errorf("registration error: %v", err)
	}
	assignedIndex, _ := strconv.Atoi(string(regFrame.Payload))
	log.Printf("sign pass, assigned index = %d", assignedIndex)

	// 6. Start the role‑specific forwarding loop.
	//    Each function (RunTCPListener, RunUDPListener, RunTCPForwarder,
	//    RunUDPForwarder) receives the session and reads frames exclusively
	//    from sess.FrameCh().
	switch role {
	case "listener":
		port, err := resolvePort(listenSpec, assignedIndex)
		if err != nil {
			return assignedIndex, fmt.Errorf("resolve port: %v", err)
		}
		addr := fmt.Sprintf(":%d", port)

		if udpOnly {
			log.Printf("UDP listener running, listen %s", addr)
			return assignedIndex, RunUDPListener(sess, addr)
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return assignedIndex, fmt.Errorf("tcp listen: %v", err)
		}
		defer ln.Close()
		log.Printf("TCP listener running, listen %s", addr)
		return assignedIndex, RunTCPListener(sess, ln)

	case "forwarder":
		targetAddr := fmt.Sprintf("127.0.0.1:%d", forwardPort)
		if udpOnly {
			log.Printf("UDP forwarder running, forward to %s", targetAddr)
			return assignedIndex, RunUDPForwarder(sess, targetAddr)
		}
		log.Printf("TCP forwarder running, forward to %s", targetAddr)
		return assignedIndex, RunTCPForwarder(sess, targetAddr)
	}
	return assignedIndex, nil
}

// waitForFrame reads from the session's frame channel and returns the first
// frame matching the expected type. Any other frame type is silently dropped.
// It returns an error if the session closes or the timeout is exceeded.
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
			// For now, ignore unexpected frame types (e.g. keepalive).
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
