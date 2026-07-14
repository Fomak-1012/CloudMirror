package tun

import (
	"fmt"
	"net"
)

// Pool 从 CIDR 网段中管理虚拟 IP 的分配。
// 分配规则：
//   - 网段 + 1 = 网关（服务端 TUN 设备）
//   - 网段 + 2 + index = 客户端 IP
//     例如 192.168.1.0/24：网关 192.168.1.1，index=0 → 192.168.1.2
type Pool struct {
	network  *net.IPNet // CIDR 网段
	gateway  net.IP     // 网关 IP（网段 + 1）
	capacity int        // 最大可分配客户端数
}

// NewPool 从 CIDR 字符串创建 IP 池。
func NewPool(cidr string) (*Pool, error) {
	_, nw, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, fmt.Errorf("parse CIDR %s: %w", cidr, err)
	}

	// 计算网关 IP：网段地址 + 1
	ip := make(net.IP, len(nw.IP))
	copy(ip, nw.IP)
	addOne(ip)

	// 可用于客户端数 = 总主机数 - 2（网络和广播地址）- 1（网关）
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

// ClientIP 返回指定 index 对应的客户端 IP。
// index 0 → 网段 + 2，index 1 → 网段 + 3，以此类推。
// index 超出容量返回 nil。
func (p *Pool) ClientIP(index int) net.IP {
	if index < 0 || index >= p.capacity {
		return nil
	}
	ip := make(net.IP, len(p.gateway))
	copy(ip, p.gateway)
	// +1 跳过网关自身，再 + index
	addOne(ip)
	for i := 0; i < index; i++ {
		addOne(ip)
	}
	return ip
}

// GatewayIP 返回网关（服务端 TUN）的 IP 地址。
func (p *Pool) GatewayIP() net.IP { return p.gateway }

// CIDR 返回网段的 CIDR 字符串表示。
func (p *Pool) CIDR() string { return p.network.String() }

// MaskSize 返回子网掩码的位长度（如 24 表示 /24）。
func (p *Pool) MaskSize() int {
	ones, _ := p.network.Mask.Size()
	return ones
}

// Capacity 返回池最多可支持多少个客户端。
func (p *Pool) Capacity() int { return p.capacity }

// addOne 将 IP 地址原地加 1。
func addOne(ip net.IP) {
	for i := len(ip) - 1; i >= 0; i-- {
		ip[i]++
		if ip[i] != 0 {
			break
		}
	}
}
