package relay

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/auth"
	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
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

	for {
		err := runOnce(serverAddr, host, password, role, forwardPort, listenSpec, wantIndex, udpOnly, tlsInsecure)
		if err == nil {
			return nil // normal exit (not implemented yet)
		}
		log.Printf("client disconnected: %v, reconnecting in %v...", err, backoff)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func runOnce(serverAddr string, host string, password string, role string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly bool, tlsInsecure bool) error {

	// Dial server.
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
		return fmt.Errorf("dial error: %v", err)
	}

	tun := tunnel.NewTunnel(conn)
	defer tun.Close()

	// Start keepalive – close tunnel on timeout.
	StartKeepAlive(tun, 30*time.Second, 90*time.Second, func() {
		tun.Close()
	})

	// Authenticate.
	if err := auth.ClientAuth(tun, password); err != nil {
		return fmt.Errorf("auth failed: %v", err)
	}
	log.Println("auth pass")

	// Register role.
	regPayload := role
	if wantIndex >= 0 {
		regPayload = fmt.Sprintf("%s,%d", role, wantIndex)
	}
	if err := tun.Send(protocol.TypeRegister, []byte(regPayload)); err != nil {
		return fmt.Errorf("register send error: %v", err)
	}

	frame, err := tun.Receive()
	if err != nil {
		return fmt.Errorf("register receive error: %v", err)
	}
	if frame.Type != protocol.TypeRegOK {
		return fmt.Errorf("registration rejected: %s", string(frame.Payload))
	}

	assignedIndex, _ := strconv.Atoi(string(frame.Payload))
	log.Printf("sign pass, assigned index = %d", assignedIndex)

	// Start forwarding.
	switch role {
	case "listener":
		port, err := resolvePort(listenSpec, assignedIndex)
		if err != nil {
			return fmt.Errorf("resolve port: %v", err)
		}
		addr := fmt.Sprintf(":%d", port)

		if udpOnly {
			log.Printf("UDP listener running, listen %s", addr)
			return RunUDPListener(tun, addr)
		} else {
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("tcp listen: %v", err)
			}
			defer ln.Close()
			log.Printf("TCP listener running, listen %s", addr)
			return RunTCPListener(tun, ln)
		}

	case "forwarder":
		targetAddr := fmt.Sprintf("127.0.0.1:%d", forwardPort)
		if udpOnly {
			log.Printf("UDP forwarder running, forward to %s", targetAddr)
			return RunUDPForwarder(tun, targetAddr)
		} else {
			log.Printf("TCP forwarder running, forward to %s", targetAddr)
			return RunTCPForwarder(tun, targetAddr)
		}
	}
	return nil
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
