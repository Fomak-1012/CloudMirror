package relay

import (
	"context"
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

type clientConfig struct {
	host         string
	port         int
	password     string
	forwardPort  int
	listenSpec   string
	wantIndex    int
	udpOnly      bool
	fileSendPath string
	fileRecvDir  string
}

func (c *clientConfig) serverAddr() string {
	return net.JoinHostPort(c.host, strconv.Itoa(c.port))
}

func (c *clientConfig) isTUN() bool {
	return strings.Contains(c.listenSpec, "/")
}

// 用 string 来区分客户端角色
type clientRole string

const (
	roleListener     clientRole = "listener"
	roleForwarder    clientRole = "forwarder"
	roleFileSender   clientRole = "file-sender"
	roleFileReceiver clientRole = "file-receiver"
)

// 推导客户端的角色
func (c *clientConfig) resolveRole() (clientRole, error) {
	switch {
	case c.fileSendPath != "":
		return roleFileSender, nil
	case c.fileRecvDir != "":
		return roleFileReceiver, nil
	case c.isTUN():
		if c.forwardPort > 0 {
			return "", fmt.Errorf("TUN mode (-l with CIDR) cannot be used with -f")
		}
		return roleListener, nil
	case c.forwardPort > 0 && c.listenSpec == "":
		return roleForwarder, nil
	case c.listenSpec != "" && c.forwardPort == 0:
		return roleListener, nil
	default:
		return "", fmt.Errorf("-f or -l must be specified and cannot be used simultaneously")
	}
}

// 客户端入口
func RunClient(ctx context.Context, host string, port int, password string, forwardPort int,
	listenSpec string, wantIndex int, udpOnly bool, fileSendPath string, fileRecvDir string) error {

	cfg := &clientConfig{
		host:         host,
		port:         port,
		password:     password,
		forwardPort:  forwardPort,
		listenSpec:   listenSpec,
		wantIndex:    wantIndex,
		udpOnly:      udpOnly,
		fileSendPath: fileSendPath,
		fileRecvDir:  fileRecvDir,
	}

	role, err := cfg.resolveRole()
	if err != nil {
		return err
	}

	const initialBackoff = 1 * time.Second
	const maxBackoff = 30 * time.Second

	backoff := initialBackoff
	actualIndex := wantIndex

	for {
		// 检查终止信号
		if err := ctx.Err(); err != nil {
			log.Println("client shutting down")
			return err
		}

		assignedIndex, err := connectAndRun(cfg, role, actualIndex)
		if err == nil {
			return nil // 正常退出
		}

		// 优先复用 index
		// 避免端口漂移
		actualIndex = assignedIndex
		log.Printf("client disconnected: %v, reconnecting in %v...", err, backoff)

		if err := sleepWithCancel(ctx, backoff); err != nil {
			log.Println("client shutting down")
			return err
		}

		// 指数退避
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// 执行一次完整的流程，返回服务端分配的 index
// 连接 -> 认证 -> 注册 -> 转发
func connectAndRun(cfg *clientConfig, role clientRole, wantIndex int) (int, error) {
	// 建立网络连接
	conn, err := dialServer(cfg)
	if err != nil {
		return wantIndex, fmt.Errorf("dial error: %w", err)
	}

	// 把得到的连接包装成 Session
	tun := tunnel.NewTunnel(conn)
	sess := session.NewSession(tun, 90*time.Second)
	defer sess.Close()

	// 启动心跳
	StartKeepAlive(sess, 30*time.Second)

	// 认证
	if err := authenticate(sess, cfg.password); err != nil {
		return wantIndex, err
	}

	// 角色注册
	assignedIndex, assignedIP, err := register(sess, role, cfg, wantIndex)
	if err != nil {
		return wantIndex, err
	}

	// 开始转发
	mode, err := newClientMode(
		string(role), cfg.listenSpec, cfg.forwardPort,
		assignedIndex, assignedIP,
		cfg.udpOnly, cfg.isTUN(),
		cfg.fileSendPath, cfg.fileRecvDir,
	)
	if err != nil {
		return assignedIndex, err
	}
	return assignedIndex, mode.Run(sess, assignedIndex)
}

// 域名 -> TLS
// IP 地址 -> TCP
func dialServer(cfg *clientConfig) (net.Conn, error) {
	addr := cfg.serverAddr()
	if net.ParseIP(cfg.host) == nil {
		return tls.Dial("tcp", addr, &tls.Config{ServerName: cfg.host})
	}
	return net.Dial("tcp", addr)
}

// 认证
func authenticate(sess *session.Session, password string) error {
	// 发送密码
	if err := sess.Send(protocol.TypeAuth, []byte(password)); err != nil {
		return fmt.Errorf("auth send error: %w", err)
	}
	// 等待确认
	if _, err := waitForFrame(sess, protocol.TypeAuthOK, 10*time.Second); err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}
	log.Println("auth pass")
	return nil
}

// 注册，返回 index 和 返回的 IP (TUN)
// register 构造并发送注册载荷，解析服务端返回的 index 和（TUN 模式下）分配的 IP。
func register(sess *session.Session, role clientRole, cfg *clientConfig, wantIndex int) (index int, assignedIP string, err error) {
	payload := buildRegisterPayload(role, cfg, wantIndex)

	// 发送注册与注册载荷
	if err := sess.Send(protocol.TypeRegister, []byte(payload)); err != nil {
		return wantIndex, "", fmt.Errorf("register send error: %w", err)
	}

	// 等待返回
	regFrame, err := waitForFrame(sess, protocol.TypeRegOK, 10*time.Second)
	if err != nil {
		return wantIndex, "", fmt.Errorf("registration error: %w", err)
	}

	// 解析返回的载荷
	index, assignedIP = parseRegReply(string(regFrame.Payload), cfg.isTUN())
	log.Printf("sign pass, assigned index = %d", index)
	return index, assignedIP, nil
}

// 根据角色和配置构造载荷
// 格式：
// listener/forwarder:  <role>,<index>,<proto>,<port_or_target>
// file-sender/receiver: <role>,<index>,file,<path>
// TUN listener:         在 configureTUNClient
func buildRegisterPayload(role clientRole, cfg *clientConfig, wantIndex int) string {
	switch role {
	case roleFileSender:
		return formatRegPayload("listener", wantIndex, "file", cfg.fileSendPath)
	case roleFileReceiver:
		return formatRegPayload("forwarder", wantIndex, "file", cfg.fileRecvDir)
	case roleListener:
		if cfg.isTUN() {
			payload, _ := configureTUNClient(cfg.listenSpec, wantIndex)
			return payload
		}
		port, _ := resolvePort(cfg.listenSpec, wantIndex)
		return formatRegPayload("listener", wantIndex, pickProto(cfg.udpOnly), strconv.Itoa(port))
	default: // roleForwarder
		target := fmt.Sprintf("127.0.0.1:%d", cfg.forwardPort)
		return formatRegPayload("forwarder", wantIndex, pickProto(cfg.udpOnly), target)
	}
}

// 拼接参数
func formatRegPayload(role string, index int, fields ...string) string {
	parts := []string{role}
	if index >= 0 {
		parts = append(parts, strconv.Itoa(index))
	} else {
		parts = append(parts, "")
	}
	parts = append(parts, fields...)
	return strings.Join(parts, ",")
}

func pickProto(udpOnly bool) string {
	if udpOnly {
		return "udp"
	}
	return "tcp"
}

// 解析注册回复
// 格式：
// "<index>(,<assigned_ip>)(TUN)"
func parseRegReply(reply string, isTUN bool) (index int, assignedIP string) {
	if !isTUN {
		index, _ = strconv.Atoi(reply)
		return
	}
	parts := strings.SplitN(reply, ",", 2)
	index, _ = strconv.Atoi(parts[0])
	if len(parts) > 1 {
		assignedIP = parts[1]
	}
	return
}

// 阻塞等待指定类型的帧
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
			// 忽略其他帧
		case <-timer.C:
			return nil, fmt.Errorf("timeout waiting for frame 0x%x", typ)
		}
	}
}

// 可中断的 sleep，ctx 取消时立即返回
func sleepWithCancel(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// 计算实际监听端口。

func resolvePort(spec string, index int) (int, error) {
	// 单个端口, port + index
	if !strings.Contains(spec, ",") {
		base, err := strconv.Atoi(spec)
		if err != nil {
			return 0, err
		}
		return base + index, nil
	}

	// 多端口分割列表, 按 index 取对应值
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
	// 超出列表从最后一个端口递增
	last := ports[len(ports)-1]
	return last + (index - len(ports) + 1), nil
}
