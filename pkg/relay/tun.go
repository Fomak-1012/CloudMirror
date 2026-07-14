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
)

// ServerTUNContext 持有服务端 TUN 模式的共享状态，包括 TUN 设备、
// IP 池和 IP → 客户端隧道的路由表。
type ServerTUNContext struct {
	mu        sync.Mutex
	dev       *tun.Dev                           // 服务端 TUN 设备
	pool      *tun.Pool                          // IP 分配池
	cidrStr   string                             // 首个客户端注册的 CIDR
	ipToTun   map[string]protocol.FrameReadWriter // 目标 IP → 客户端隧道
	indexToIP map[int]string                      // index → 分配的 IP
}

// NewServerTUNContext 创建服务端 TUN 设备并启动后台路由 goroutine。
func NewServerTUNContext() (*ServerTUNContext, error) {
	dev, err := tun.New("crfl%d")
	if err != nil {
		return nil, fmt.Errorf("create TUN device: %w", err)
	}

	ctx := &ServerTUNContext{
		dev:       dev,
		ipToTun:   make(map[string]protocol.FrameReadWriter),
		indexToIP: make(map[int]string),
	}

	go ctx.tunReadLoop()
	return ctx, nil
}

// tunReadLoop 在后台从服务端 TUN 设备读取 IP 包，根据目标 IP 路由到对应客户端。
// 命中 ipToTun 的包直接转发，未命中的丢给内核路由（如果系统有路由规则）。
func (ctx *ServerTUNContext) tunReadLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, err := ctx.dev.Read(buf)
		if err != nil {
			log.Printf("[server-tun] read error: %v", err)
			return
		}
		if n < 20 {
			continue // 太短，不是有效 IP 头
		}

		packet := make([]byte, n)
		copy(packet, buf[:n])

		// IPv4 头字节 16-19 为目标地址
		destIP := net.IP(packet[16:20]).String()

		ctx.mu.Lock()
		peer := ctx.ipToTun[destIP]
		ctx.mu.Unlock()

		if peer != nil {
			if err := peer.Send(protocol.TypeDataTUN, packet); err != nil {
				log.Printf("[server-tun] send to %s error: %v", destIP, err)
			}
		}
	}
}

// ensurePool 确保 IP 池已初始化（首个客户端注册时）。若已存在则校验 CIDR 一致性。
// 池初始化后会自动配置 TUN 设备的网关 IP 并启用。
func (ctx *ServerTUNContext) ensurePool(cidr string) error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.pool != nil {
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

	// 配置 TUN 设备 IP 并启用
	gw := pool.GatewayIP()
	mask := pool.MaskSize()
	addr := fmt.Sprintf("%s/%d", gw.String(), mask)

	if err := exec.Command("ip", "addr", "add", addr, "dev", ctx.dev.Name()).Run(); err != nil {
		return fmt.Errorf("ip addr add %s dev %s: %w", addr, ctx.dev.Name(), err)
	}
	if err := exec.Command("ip", "link", "set", ctx.dev.Name(), "up").Run(); err != nil {
		return fmt.Errorf("ip link set %s up: %w", ctx.dev.Name(), err)
	}

	log.Printf("[server-tun] device %s up with %s, capacity=%d clients, cidr=%s",
		ctx.dev.Name(), addr, pool.Capacity(), cidr)
	return nil
}

// RegisterClient 为 TUN 客户端分配 IP 并建立 IP → 隧道路由。
// 返回分配的 IP 字符串。
func (ctx *ServerTUNContext) RegisterClient(conn protocol.FrameReadWriter, index int, cidr string) (string, error) {
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

	// 清理旧路由条目
	if oldTun, ok := ctx.ipToTun[ipStr]; ok {
		oldTun.Close()
	}
	if oldIP, ok := ctx.indexToIP[index]; ok {
		delete(ctx.ipToTun, oldIP)
	}

	ctx.ipToTun[ipStr] = conn
	ctx.indexToIP[index] = ipStr

	log.Printf("[server-tun] client index=%d assigned IP=%s", index, ipStr)
	return ipStr, nil
}

// UnregisterClient 移除客户端的 IP 路由映射。
func (ctx *ServerTUNContext) UnregisterClient(index int) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ipStr, ok := ctx.indexToIP[index]; ok {
		delete(ctx.ipToTun, ipStr)
		delete(ctx.indexToIP, index)
		log.Printf("[server-tun] client index=%d (IP=%s) unregistered", index, ipStr)
	}
}

// WriteToTUN 将原始 IP 包写入服务端 TUN 设备。
func (ctx *ServerTUNContext) WriteToTUN(packet []byte) error {
	_, err := ctx.dev.Write(packet)
	return err
}

// DevName 返回服务端 TUN 设备的名称。
func (ctx *ServerTUNContext) DevName() string { return ctx.dev.Name() }

// Close 关闭服务端 TUN 设备。
func (ctx *ServerTUNContext) Close() error { return ctx.dev.Close() }

// serverTUNRelayLoop 在服务端为每个 TUN 客户端运行转发循环：
// 从客户端接收 TypeDataTUN 帧 → 写入服务端 TUN 设备。
func serverTUNRelayLoop(conn protocol.FrameReadWriter, ctx *ServerTUNContext, index int) {
	for {
		frame, err := conn.Receive()
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
	}
	ctx.UnregisterClient(index)
}

// ---- 客户端 TUN ----

// runTUNListener 在客户端创建本地 TUN 设备并启动双向转发：
//   goroutine: 本地 TUN → Session（发送 IP 包到服务端）
//   主循环:    Session → 本地 TUN（接收来自服务端的 IP 包）
func runTUNListener(sess *session.Session, cidr string, index int, assignedIP string) error {
	devName := fmt.Sprintf("crfl%d", index)

	// 清理上次残留的设备
	exec.Command("ip", "link", "del", devName).Run()

	dev, err := tun.New(devName)
	if err != nil {
		return fmt.Errorf("create local TUN: %w", err)
	}
	defer dev.Close()
	defer exec.Command("ip", "link", "del", devName).Run() // 退出时删除设备

	// 配置 TUN 设备 IP 并启用
	addr := assignedIP
	_, nw, _ := net.ParseCIDR(cidr)
	if nw != nil {
		ones, _ := nw.Mask.Size()
		addr = fmt.Sprintf("%s/%d", assignedIP, ones)
	}
	if err := exec.Command("ip", "addr", "add", addr, "dev", dev.Name()).Run(); err != nil {
		return fmt.Errorf("ip addr add %s dev %s: %w", addr, dev.Name(), err)
	}
	if err := exec.Command("ip", "link", "set", dev.Name(), "up").Run(); err != nil {
		return fmt.Errorf("ip link set %s up: %w", dev.Name(), err)
	}
	log.Printf("[tun-listener] local TUN %s up with %s", dev.Name(), addr)

	// 发送：本地 TUN → Session
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

	// 接收：Session → 本地 TUN
	for frame := range sess.FrameCh() {
		if frame.Type == protocol.TypeDataTUN {
			if _, err := dev.Write(frame.Payload); err != nil {
				return fmt.Errorf("write local TUN: %w", err)
			}
		}
	}
	return fmt.Errorf("session closed")
}

// configureTUNClient 构造 TUN 监听器的注册载荷。
// 格式：listener[,<index>],tun,<cidr>
func configureTUNClient(listenSpec string, wantIndex int) (regPayload string, cidr string) {
	cidr = listenSpec
	if wantIndex >= 0 {
		regPayload = fmt.Sprintf("listener,%d,tun,%s", wantIndex, cidr)
	} else {
		regPayload = fmt.Sprintf("listener,tun,%s", cidr)
	}
	return regPayload, cidr
}
