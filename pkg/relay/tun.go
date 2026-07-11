package relay

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
	"github.com/Fomak-1012/CloudMirror/pkg/tun"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

// ServerTUNContext holds the server-side TUN state shared across all TUN clients.
type ServerTUNContext struct {
	mu        sync.Mutex
	dev       *tun.Dev
	pool      *tun.Pool
	cidrStr   string              // CIDR string from the first TUN client
	ipToTun   map[string]*tunnel.Tunnel // dest IP → client tunnel
	indexToIP map[int]string       // index → assigned IP
}

// NewServerTUNContext creates a TUN device and starts the reader goroutine.
func NewServerTUNContext() (*ServerTUNContext, error) {
	dev, err := tun.New("crfl%d")
	if err != nil {
		return nil, fmt.Errorf("create TUN device: %w", err)
	}

	ctx := &ServerTUNContext{
		dev:       dev,
		ipToTun:   make(map[string]*tunnel.Tunnel),
		indexToIP: make(map[int]string),
	}

	// Start the TUN reader goroutine that distributes packets to clients.
	go ctx.tunReadLoop()

	return ctx, nil
}

// tunReadLoop reads IP packets from the server's TUN device and forwards
// each packet to the client that owns the destination IP.
func (ctx *ServerTUNContext) tunReadLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, err := ctx.dev.Read(buf)
		if err != nil {
			log.Printf("[server-tun] read error: %v", err)
			return
		}
		if n < 20 {
			continue // too short for an IP header
		}

		packet := make([]byte, n)
		copy(packet, buf[:n])

		// Extract destination IP from the IPv4 header (bytes 16-19).
		destIP := net.IP(packet[16:20]).String()

		ctx.mu.Lock()
		peer := ctx.ipToTun[destIP]
		ctx.mu.Unlock()

		if peer != nil {
			if err := peer.Send(protocol.TypeDataTUN, packet); err != nil {
				log.Printf("[server-tun] send to %s error: %v", destIP, err)
			}
		}
		// Packets for unknown destinations are silently dropped
		// (the kernel will handle routing if routes are set up).
	}
}

// ensurePool creates the IP pool from the first TUN client's CIDR,
// configures the TUN device, and brings it up.
func (ctx *ServerTUNContext) ensurePool(cidr string) error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.pool != nil {
		// Pool already initialized — verify CIDR matches.
		if cidr != ctx.cidrStr {
			return fmt.Errorf("CIDR mismatch: pool is %s, got %s", ctx.cidrStr, cidr)
		}
		return nil
	}

	pool, err := tun.NewPool(cidr)
	if err != nil {
		return err
	}

	ctx.pool = pool
	ctx.cidrStr = cidr

	// Configure the TUN device with the gateway IP and bring it up.
	gw := pool.GatewayIP()
	mask := pool.MaskSize()
	addr := fmt.Sprintf("%s/%d", gw.String(), mask)

	if err := exec.Command("ip", "addr", "add", addr, "dev", ctx.dev.Name).Run(); err != nil {
		return fmt.Errorf("ip addr add %s dev %s: %w", addr, ctx.dev.Name, err)
	}
	if err := exec.Command("ip", "link", "set", ctx.dev.Name, "up").Run(); err != nil {
		return fmt.Errorf("ip link set %s up: %w", ctx.dev.Name, err)
	}

	log.Printf("[server-tun] device %s up with %s, capacity=%d clients, cidr=%s",
		ctx.dev.Name, addr, pool.Capacity(), cidr)
	return nil
}

// RegisterClient assigns an IP to a TUN client and records the tunnel mapping.
// Returns the assigned IP string.
func (ctx *ServerTUNContext) RegisterClient(t *tunnel.Tunnel, index int, cidr string) (string, error) {
	if err := ctx.ensurePool(cidr); err != nil {
		return "", err
	}

	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	clientIP := ctx.pool.ClientIP(index)
	if clientIP == nil {
		return "", fmt.Errorf("index %d out of range (capacity=%d)", index, ctx.pool.Capacity())
	}

	ipStr := clientIP.String()

	// Clean up any previous tunnel at this IP or index.
	if oldTun, ok := ctx.ipToTun[ipStr]; ok {
		oldTun.Close()
	}
	if oldIP, ok := ctx.indexToIP[index]; ok {
		delete(ctx.ipToTun, oldIP)
	}

	ctx.ipToTun[ipStr] = t
	ctx.indexToIP[index] = ipStr

	log.Printf("[server-tun] client index=%d assigned IP=%s", index, ipStr)
	return ipStr, nil
}

// UnregisterClient removes a TUN client's mappings.
func (ctx *ServerTUNContext) UnregisterClient(index int) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ipStr, ok := ctx.indexToIP[index]; ok {
		delete(ctx.ipToTun, ipStr)
		delete(ctx.indexToIP, index)
		log.Printf("[server-tun] client index=%d (IP=%s) unregistered", index, ipStr)
	}
}

// WriteToTUN writes a raw IP packet to the server's TUN device.
func (ctx *ServerTUNContext) WriteToTUN(packet []byte) error {
	_, err := ctx.dev.Write(packet)
	return err
}

// DevName returns the TUN device name.
func (ctx *ServerTUNContext) DevName() string {
	return ctx.dev.Name
}

// Close shuts down the TUN context and releases the device.
func (ctx *ServerTUNContext) Close() error {
	return ctx.dev.Close()
}

// serverTUNRelayLoop handles the relay loop for a TUN-mode listener.
// It reads TypeDataTUN frames from the client and writes them to the server TUN device.
func serverTUNRelayLoop(t *tunnel.Tunnel, ctx *ServerTUNContext, index int) {
	for {
		frame, err := t.Receive()
		if err != nil {
			log.Printf("[server-tun] relayLoop index=%d recv error: %v", index, err)
			break
		}
		if frame.Type == protocol.TypeDataTUN {
			if err := ctx.WriteToTUN(frame.Payload); err != nil {
				log.Printf("[server-tun] relayLoop index=%d write TUN error: %v", index, err)
				break
			}
		}
		// Ignore other frame types in TUN mode.
	}

	ctx.UnregisterClient(index)
}

// --- Client-side TUN ---

// runTUNListener creates a local TUN device, configures it with the assigned IP,
// and starts forwarding IP packets between the local TUN and the server session.
func runTUNListener(sess *session.Session, cidr string, index int, assignedIP string) error {
	devName := fmt.Sprintf("crfl%d", index)

	// Remove any leftover device from a previous connection (ignore errors).
	exec.Command("ip", "link", "del", devName).Run()

	dev, err := tun.New(devName)
	if err != nil {
		return fmt.Errorf("create local TUN: %w", err)
	}
	defer dev.Close()
	defer func() {
		// Remove the device from the system on exit (reconnect / shutdown).
		exec.Command("ip", "link", "del", devName).Run()
	}()

	// Configure the TUN device with the assigned IP and bring it up.
	addr := assignedIP
	_, nw, _ := net.ParseCIDR(cidr)
	if nw != nil {
		ones, _ := nw.Mask.Size()
		addr = fmt.Sprintf("%s/%d", assignedIP, ones)
	}

	if err := exec.Command("ip", "addr", "add", addr, "dev", dev.Name).Run(); err != nil {
		return fmt.Errorf("ip addr add %s dev %s: %w", addr, dev.Name, err)
	}
	if err := exec.Command("ip", "link", "set", dev.Name, "up").Run(); err != nil {
		return fmt.Errorf("ip link set %s up: %w", dev.Name, err)
	}

	log.Printf("[tun-listener] local TUN %s up with %s", dev.Name, addr)

	// Goroutine: local TUN → session
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := dev.Read(buf)
			if err != nil {
				log.Printf("[tun-listener] read local TUN error: %v", err)
				return
			}
			packet := make([]byte, n)
			copy(packet, buf[:n])
			if err := sess.Send(protocol.TypeDataTUN, packet); err != nil {
				log.Printf("[tun-listener] send to session error: %v", err)
				return
			}
		}
	}()

	// Main goroutine: session → local TUN
	for frame := range sess.FrameCh() {
		if frame.Type == protocol.TypeDataTUN {
			if _, err := dev.Write(frame.Payload); err != nil {
				return fmt.Errorf("write local TUN: %w", err)
			}
		}
		// Other frame types ignored in TUN mode.
	}

	return fmt.Errorf("session closed")
}

// configureTUNClient returns the registration payload for a TUN listener.
func configureTUNClient(listenSpec string, wantIndex int) (regPayload string, cidr string) {
	cidr = listenSpec
	regPayload = fmt.Sprintf("listener")
	if wantIndex >= 0 {
		regPayload = fmt.Sprintf("listener,%d", wantIndex)
	}
	regPayload = fmt.Sprintf("%s,tun,%s", regPayload, cidr)
	return regPayload, cidr
}
