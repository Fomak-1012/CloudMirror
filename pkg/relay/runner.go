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

// RunClient 是客户端的主入口，负责建立连接、认证、注册，并在连接断开时自动重连。
// isTUN 由调用方根据 listenSpec 是否包含 "/" 来判断。
func RunClient(host string, port int, password string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly bool, tlsInsecure bool) error {

	// 确定角色
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

	// 指数退避重连
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	actualIndex := wantIndex
	for {
		assignedIndex, err := runOnce(serverAddr, host, password, role, forwardPort,
			listenSpec, actualIndex, udpOnly, tlsInsecure, isTUN)
		if err == nil {
			return nil
		}
		// 记住上次分配到的 index，重连时请求相同的 index 以避免端口漂移
		actualIndex = assignedIndex
		log.Printf("client disconnected: %v, reconnecting in %v...", err, backoff)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// runOnce 执行一次完整的客户端生命周期：连接 → 认证 → 注册 → 模式转发。
// 返回服务端分配到的 index 用于重连时保持 index 不变。
func runOnce(serverAddr, host, password, role string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly, tlsInsecure, isTUN bool) (int, error) {

	// 1. 建立 TCP 或 TLS 连接
	var conn net.Conn
	var err error
	if tlsInsecure {
		tlsConfig := &tls.Config{InsecureSkipVerify: true, ServerName: host}
		conn, err = tls.Dial("tcp", serverAddr, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", serverAddr)
	}
	if err != nil {
		return 0, fmt.Errorf("dial error: %w", err)
	}

	// 2. 包装为 Tunnel → Session（启动后台读泵）
	tun := tunnel.NewTunnel(conn)
	sess := session.NewSession(tun, 90*time.Second)
	defer sess.Close()

	// 3. 启动心跳
	StartKeepAlive(sess, 30*time.Second)

	// 4. 认证
	if err := sess.Send(protocol.TypeAuth, []byte(password)); err != nil {
		return wantIndex, fmt.Errorf("auth send error: %w", err)
	}
	if _, err := waitForFrame(sess, protocol.TypeAuthOK, 10*time.Second); err != nil {
		return wantIndex, fmt.Errorf("auth failed: %w", err)
	}
	log.Println("auth pass")

	// 5. 构造并发送注册载荷
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

	// 6. 等待注册回复
	regFrame, err := waitForFrame(sess, protocol.TypeRegOK, 10*time.Second)
	if err != nil {
		return wantIndex, fmt.Errorf("registration error: %w", err)
	}

	// 7. 解析回复
	// 普通模式："<index>"，TUN 模式："<index>,<assigned_ip>"
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

	// 8. 构造对应的 Mode 并启动转发（多态分派，消除 switch）
	mode, err := newClientMode(role, listenSpec, forwardPort, assignedIndex, assignedIP, udpOnly, isTUN)
	if err != nil {
		return assignedIndex, err
	}
	return assignedIndex, mode.Run(sess, assignedIndex)
}

// newClientMode 根据角色和参数创建对应的转发模式实例。
// 这是 Mode 接口的工厂函数，根据 index 解析出实际监听端口/地址。
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

// waitForFrame 从 Session 的帧通道中等待指定类型的帧，超时返回错误。
// 其他类型的帧会被忽略（如心跳帧）。
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

// resolvePort 根据监听规格和分配的 index 计算实际监听端口。
// spec 为单个端口时：port + index
// spec 为逗号分隔列表时：按 index 取对应值，超出则尾部递增
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
