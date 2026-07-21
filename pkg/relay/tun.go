package relay

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
	"github.com/Fomak-1012/CloudMirror/pkg/tun"
)

// ============================================================================
// 服务端 TUN 上下文
// ============================================================================

// ServerTUNContext 持有服务端 TUN 模式的共享状态：
// TUN 设备、IP 分配池、IP → 客户端隧道路由表。
type ServerTUNContext struct {
	mu        sync.Mutex
	dev       *tun.Dev                            // 服务端 TUN 设备
	pool      *tun.Pool                           // IP 分配池
	cidrStr   string                              // 首个客户端注册的 CIDR
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

	safeGo("tun-server-readloop", ctx.tunReadLoop)
	return ctx, nil
}

// ---- 设备信息 ----

func (ctx *ServerTUNContext) DevName() string { return ctx.dev.Name() }
func (ctx *ServerTUNContext) Close() error    { return ctx.dev.Close() }
func (ctx *ServerTUNContext) WriteToTUN(packet []byte) error {
	_, err := ctx.dev.Write(packet)
	return err
}

// ---- IP 池管理 ----

// ensurePool 初始化或校验 IP 池。首次调用时创建池并配置 TUN 设备。
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

	if err := configureTUNDevice(ctx.dev.Name(), pool.GatewayIP(), pool.MaskSize()); err != nil {
		return err
	}

	log.Printf("[tun] server device %s up, cidr=%s, capacity=%d", ctx.dev.Name(), cidr, pool.Capacity())
	return nil
}

// ---- 客户端路由 ----

// RegisterClient 为 TUN 客户端分配 IP 并建立 IP → 隧道路由。返回分配的 IP。
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

	// 清理旧路由（重连场景）
	if old, ok := ctx.ipToTun[ipStr]; ok {
		old.Close()
	}
	if oldIP, ok := ctx.indexToIP[index]; ok {
		delete(ctx.ipToTun, oldIP)
	}

	ctx.ipToTun[ipStr] = conn
	ctx.indexToIP[index] = ipStr
	log.Printf("[tun] client index=%d → %s", index, ipStr)
	return ipStr, nil
}

// UnregisterClient 移除客户端的 IP 路由映射。
func (ctx *ServerTUNContext) UnregisterClient(index int) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ipStr, ok := ctx.indexToIP[index]; ok {
		delete(ctx.ipToTun, ipStr)
		delete(ctx.indexToIP, index)
	}
}

// ---- 后台读循环 ----

// tunReadLoop 从服务端 TUN 设备读取 IP 包，根据目标 IP 路由到对应客户端。
func (ctx *ServerTUNContext) tunReadLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, err := ctx.dev.Read(buf)
		if err != nil {
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
			peer.Send(protocol.TypeDataTUN, packet)
		}
	}
}

// ---- TUN 转发循环 ----

// serverTUNRelayLoop 在服务端为每个 TUN 客户端运行转发循环：
// 客户端帧 → 服务端 TUN 设备。
func serverTUNRelayLoop(conn protocol.FrameReadWriter, ctx *ServerTUNContext, index int) {
	for {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		frame, err := conn.Receive()
		if err != nil {
			break
		}
		if frame.Type == protocol.TypeDataTUN {
			if err := ctx.WriteToTUN(frame.Payload); err != nil {
				break
			}
		}
	}
	ctx.UnregisterClient(index)
}

// ============================================================================
// OS 级 TUN 设备配置
// ============================================================================

// configureTUNDevice 给 TUN 设备分配 IP 地址并启用。
func configureTUNDevice(devName string, gwIP net.IP, maskSize int) error {
	addr := fmt.Sprintf("%s/%d", gwIP.String(), maskSize)
	if err := exec.Command("ip", "addr", "add", addr, "dev", devName).Run(); err != nil {
		return fmt.Errorf("ip addr add %s dev %s: %w", addr, devName, err)
	}
	if err := exec.Command("ip", "link", "set", devName, "up").Run(); err != nil {
		return fmt.Errorf("ip link set %s up: %w", devName, err)
	}
	return nil
}

// ============================================================================
// 客户端 TUN
// ============================================================================

// runTUNListener 在客户端创建本地 TUN 设备并启动双向转发。
//
// 架构：
//
//	goroutine: 本地 TUN → Session（上传 IP 包到服务端）
//	主循环:    Session → 本地 TUN（接收服务端的 IP 包）
func runTUNListener(sess *session.Session, cidr string, index int, assignedIP string) error {
	devName := fmt.Sprintf("crfl%d", index)

	// 清理上次残留的设备
	exec.Command("ip", "link", "del", devName).Run()

	dev, err := tun.New(devName)
	if err != nil {
		return fmt.Errorf("create local TUN: %w", err)
	}
	defer dev.Close()
	defer exec.Command("ip", "link", "del", devName).Run()

	// 配置 IP 并启用设备
	if err := configureTUNDevice(dev.Name(), net.ParseIP(assignedIP), maskFromCIDR(cidr)); err != nil {
		return err
	}

	// 发送方向：本地 TUN → Session
	safeGo("tun-client-send", func() { tunSendLoop(sess, dev) })

	// 接收方向：Session → 本地 TUN
	return tunRecvLoop(sess, dev)
}

// tunSendLoop 从本地 TUN 设备读取 IP 包并发送到 Session。
func tunSendLoop(sess *session.Session, dev *tun.Dev) {
	buf := make([]byte, 64*1024)
	for {
		n, err := dev.Read(buf)
		if err != nil {
			return
		}
		packet := make([]byte, n)
		copy(packet, buf[:n])
		if err := sess.Send(protocol.TypeDataTUN, packet); err != nil {
			return
		}
	}
}

// tunRecvLoop 从 Session 接收 IP 包并写入本地 TUN 设备。
func tunRecvLoop(sess *session.Session, dev *tun.Dev) error {
	for frame := range sess.FrameCh() {
		if frame.Type == protocol.TypeDataTUN {
			if _, err := dev.Write(frame.Payload); err != nil {
				return fmt.Errorf("write local TUN: %w", err)
			}
		}
	}
	return fmt.Errorf("session closed")
}

// ============================================================================
// TUN 工具函数
// ============================================================================

// maskFromCIDR 从 CIDR 字符串中提取子网掩码位数。
func maskFromCIDR(cidr string) int {
	_, nw, err := net.ParseCIDR(cidr)
	if err != nil {
		return 0
	}
	ones, _ := nw.Mask.Size()
	return ones
}

// 构造 TUN 的注册载荷
// listener[,<index>],tun,<cidr>
func configureTUNClient(listenSpec string, wantIndex int) (regPayload string, cidr string) {
	cidr = listenSpec
	return formatRegPayload("listener", wantIndex, "tun", cidr), cidr
}
