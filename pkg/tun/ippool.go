package tun

import (
	"fmt"
	"net"
)

// Pool manages virtual IP address allocation from a CIDR range.
// Index 0 maps to the first assignable host address (network + 2),
// index 1 to network + 3, etc.
// network + 1 is reserved for the gateway (server TUN device).
type Pool struct {
	network  *net.IPNet
	gateway  net.IP // network + 1
	capacity int    // max number of assignable IPs
}

// NewPool creates an IP pool from a CIDR string like "192.168.1.0/24".
// The gateway (server) gets the first usable host address.
// Clients get subsequent addresses based on their index.
func NewPool(cidr string) (*Pool, error) {
	_, nw, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("parse CIDR %s: %w", cidr, err)
	}

	// Calculate gateway IP: network + 1
	ip := make(net.IP, len(nw.IP))
	copy(ip, nw.IP)
	addOne(ip)

	// Total usable hosts = 2^(bits) - 2 (network + broadcast) - 1 (gateway)
	ones, bits := nw.Mask.Size()
	totalHosts := (1 << (bits - ones)) - 2
	capacity := totalHosts - 1
	if capacity < 0 {
		capacity = 0
	}

	return &Pool{
		network:  nw,
		gateway:  ip,
		capacity: capacity,
	}, nil
}

// ClientIP returns the virtual IP assigned to the client with the given index.
// Index 0 → network + 2, index 1 → network + 3, etc.
// Returns nil if the index is out of range.
func (p *Pool) ClientIP(index int) net.IP {
	if index < 0 || index >= p.capacity {
		return nil
	}
	ip := make(net.IP, len(p.gateway))
	copy(ip, p.gateway)
	// gateway + 1 + index = network + 2 + index
	addOne(ip)
	for i := 0; i < index; i++ {
		addOne(ip)
	}
	return ip
}

// GatewayIP returns the gateway (server TUN) IP address.
func (p *Pool) GatewayIP() net.IP {
	return p.gateway
}

// CIDR returns the network's CIDR string and the mask size for interface config.
func (p *Pool) CIDR() string {
	return p.network.String()
}

// MaskSize returns the prefix length (e.g., 24 for /24).
func (p *Pool) MaskSize() int {
	ones, _ := p.network.Mask.Size()
	return ones
}

// Capacity returns the maximum number of clients this pool can support.
func (p *Pool) Capacity() int {
	return p.capacity
}

// addOne increments an IP address by 1 in-place.
func addOne(ip net.IP) {
	for i := len(ip) - 1; i >= 0; i-- {
		ip[i]++
		if ip[i] != 0 {
			break
		}
	}
}
