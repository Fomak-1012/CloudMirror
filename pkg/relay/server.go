package relay

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/auth"
	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

// ============================================================================
// 类型定义
// ============================================================================

// peerMeta 记录每个 peer 连接的元信息，用于 Web 控制台展示。
type peerMeta struct {
	connectedAt time.Time
	mode        string // "tcp", "udp", "tun"
	listenPort  string // listener 端口（仅 listener）
	target      string // forwarder 目标地址（仅 forwarder）
}

// regRequest 是从注册帧载荷解析出的结构化请求。
//
// 支持的载荷格式：
//   - 旧格式: listener[,<index>]
//   - 新格式: listener,<index>,<mode>,<port|target>
//   - TUN:    listener[,<index>],tun,<cidr>
type regRequest struct {
	role      clientRole // "listener" 或 "forwarder"
	wantIndex int        // 请求的 index，-1 表示自动分配
	mode      string     // "tcp"（默认）, "udp", "tun"
	metaStr   string     // listener 端口 或 forwarder 目标地址
	tunCIDR   string     // TUN 模式的 CIDR（仅 isTUN 时有效）
	isTUN     bool
}

// parseRegRequest 解析注册帧载荷为结构化请求。
//
// 字段按位置解析：parts[0]=角色, parts[1]=index 或 "tun"（旧格式）,
// parts[2]=模式 或 "tun"（新格式）, parts[3]=端口/目标/CIDR。
func parseRegRequest(payload string) *regRequest {
	parts := strings.Split(payload, ",")
	req := &regRequest{
		role:      clientRole(parts[0]),
		wantIndex: -1,
		mode:      "tcp",
	}

	// 解析 parts[1]：要么是数字 index，要么是 "tun"（旧格式）
	req.isTUN = req.tryParseTUN(parts, 1)

	// 如果 parts[1] 不是 TUN，尝试按数字 index 解析
	if !req.isTUN && len(parts) > 1 && parts[1] != "" {
		fmt.Sscanf(parts[1], "%d", &req.wantIndex)
	}

	// 解析 parts[2]+：模式信息（新格式）
	if !req.isTUN && len(parts) > 2 {
		if req.tryParseTUN(parts, 2) {
			req.isTUN = true
		} else {
			req.mode = parts[2]
			if len(parts) > 3 {
				req.metaStr = parts[3]
			}
		}
	}

	return req
}

// tryParseTUN 检查 parts[pos] 是否为 "tun" 标记，若是则解析后续 CIDR。
func (req *regRequest) tryParseTUN(parts []string, pos int) bool {
	if pos >= len(parts) || parts[pos] != "tun" {
		return false
	}
	req.mode = "tun"
	if len(parts) > pos+1 {
		req.tunCIDR = parts[pos+1]
	}
	return true
}

// Server 是中继服务的核心，负责接纳客户端连接、认证、注册，
// 并将 listener 与 forwarder 配对转发数据。
type Server struct {
	peers        *PeerMap          // 配对管理器
	nextIndex    int               // 自动分配的下一个 index
	maxListeners int               // 最大 listener 数量（0 表示无限制）
	password     string            // 预共享密钥
	tunCtx       *ServerTUNContext // TUN 模式上下文（-t 时非空）
	startTime    time.Time         // 服务启动时间
	onPeerChange func()            // peers 变更时的回调（用于 SSE 推送）

	metaMu sync.Mutex
	meta   map[protocol.FrameReadWriter]*peerMeta // 连接 → 元信息
	wg     sync.WaitGroup                         // 追踪活跃连接，用于优雅关闭
}

// ============================================================================
// 构造与生命周期
// ============================================================================

// NewServer 创建一个中继服务实例。
func NewServer(password string, maxListeners int) *Server {
	return &Server{
		peers:        NewPeerMap(),
		password:     password,
		maxListeners: maxListeners,
		startTime:    time.Now(),
		meta:         make(map[protocol.FrameReadWriter]*peerMeta),
	}
}

// SetPeerChangeCallback 设置 peers 变更时的回调（用于 Web 控制台 SSE 推送）。
func (s *Server) SetPeerChangeCallback(fn func()) {
	s.onPeerChange = fn
}

func (s *Server) notifyPeerChange() {
	if s.onPeerChange != nil {
		s.onPeerChange()
	}
}

// StartTUNMode 初始化 TUN 模式，创建服务端 TUN 设备。
func (s *Server) StartTUNMode() error {
	ctx, err := NewServerTUNContext()
	if err != nil {
		return err
	}
	s.tunCtx = ctx
	log.Printf("[server] TUN mode enabled, device=%s", ctx.DevName())
	return nil
}

// TrackConnection 递增活跃连接计数。
func (s *Server) TrackConnection() { s.wg.Add(1) }

// TrackConnectionDone 递减活跃连接计数。
func (s *Server) TrackConnectionDone() { s.wg.Done() }

// Shutdown 等待所有活跃连接完成，或超时后强制退出。
func (s *Server) Shutdown(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Println("all connections finished")
	case <-time.After(timeout):
		log.Printf("shutdown timeout after %v, forcing exit", timeout)
	}
}

// ============================================================================
// 连接处理：入口
// ============================================================================

// HandleClient 处理一个客户端连接的全生命周期。
//
// 流程：认证 → 解析注册请求 → 角色分派 → 进入转发循环。
func (s *Server) HandleClient(conn net.Conn) {
	s.TrackConnection()
	defer s.TrackConnectionDone()

	t := tunnel.NewTunnel(conn)
	defer t.Close()

	// 阶段一：认证
	if err := auth.ServerAuth(t, s.password); err != nil {
		return
	}

	// 阶段二：读取注册帧
	frame, err := t.Receive()
	if err != nil || frame.Type != protocol.TypeRegister {
		return
	}

	// 阶段三：解析注册请求
	req := parseRegRequest(string(frame.Payload))

	// 阶段四：校验 TUN 模式
	if req.isTUN && s.tunCtx == nil {
		replyError(t, "server not in TUN mode (use -t)")
		return
	}

	// 阶段五：根据角色分派注册
	index, err := s.dispatchReg(t, req)
	if err != nil {
		return
	}

	// TUN listener 有自己的转发循环（已在 dispatchReg 中启动）
	if req.isTUN {
		return
	}

	// 阶段六：回复注册成功
	t.Send(protocol.TypeRegOK, []byte(fmt.Sprintf("%d", index)))

	// 阶段七：进入转发循环
	stopKeepalive := make(chan struct{})
	StartServerKeepAlive(t, 30*time.Second, stopKeepalive)
	s.relayLoop(t, req.role, index)
	close(stopKeepalive)
}

// dispatchReg 根据角色将注册请求分派到对应的 handler。
func (s *Server) dispatchReg(conn protocol.FrameReadWriter, req *regRequest) (int, error) {
	switch req.role {
	case roleListener:
		return s.handleListenerReg(conn, req)
	case roleForwarder:
		return s.handleForwarderReg(conn, req)
	default:
		replyError(conn, "unknown role: "+string(req.role))
		return 0, fmt.Errorf("unknown role: %s", req.role)
	}
}

// ============================================================================
// 连接处理：Listener 注册
// ============================================================================

// handleListenerReg 分派 listener 注册到 TUN 或 TCP/UDP 子处理器。
func (s *Server) handleListenerReg(conn protocol.FrameReadWriter, req *regRequest) (int, error) {
	if req.isTUN {
		return s.handleTUNListener(conn, req)
	}
	return s.handleTCPListener(conn, req)
}

// handleTUNListener 注册 TUN listener：分配 IP → 建立路由 → 启动 TUN 转发循环。
// 此函数会阻塞直到客户端断开。
func (s *Server) handleTUNListener(conn protocol.FrameReadWriter, req *regRequest) (int, error) {
	index := s.assignIndex(req.wantIndex)

	if err := s.checkListenerLimit(conn); err != nil {
		return index, err
	}

	assignedIP, err := s.tunCtx.RegisterClient(conn, index, req.tunCIDR)
	if err != nil {
		replyError(conn, err.Error())
		return index, err
	}

	s.peers.RegisterListener(index, conn)
	s.storeMeta(conn, &peerMeta{connectedAt: time.Now(), mode: "tun", listenPort: req.tunCIDR})

	conn.Send(protocol.TypeRegOK, []byte(fmt.Sprintf("%d,%s", index, assignedIP)))

	serverTUNRelayLoop(conn, s.tunCtx, index)
	return index, nil
}

// handleTCPListener 注册普通（TCP/UDP）listener：分配 index → 记录元信息。
func (s *Server) handleTCPListener(conn protocol.FrameReadWriter, req *regRequest) (int, error) {
	index := s.assignIndex(req.wantIndex)

	if err := s.checkListenerLimit(conn); err != nil {
		return index, err
	}

	s.peers.RegisterListener(index, conn)
	s.storeMeta(conn, &peerMeta{connectedAt: time.Now(), mode: req.mode, listenPort: req.metaStr})
	s.notifyPeerChange()
	return index, nil
}

// ============================================================================
// 连接处理：Forwarder 注册
// ============================================================================

// handleForwarderReg 注册 forwarder：解析配对 index → 协议匹配 → 记录元信息。
func (s *Server) handleForwarderReg(conn protocol.FrameReadWriter, req *regRequest) (int, error) {
	index, err := s.resolveForwarderIndex(conn, req)
	if err != nil {
		return index, err
	}

	if err := s.checkProtocolMatch(conn, index, req.mode); err != nil {
		return index, err
	}

	s.peers.RegisterForwarder(index, conn)
	s.storeMeta(conn, &peerMeta{connectedAt: time.Now(), mode: req.mode, target: req.metaStr})
	s.notifyPeerChange()
	return index, nil
}

// resolveForwarderIndex 解析 forwarder 应配对的 listener index。
//
// wantIndex >= 0 → 直接使用；否则自动选择（仅在恰好一个 listener 时允许）。
func (s *Server) resolveForwarderIndex(conn protocol.FrameReadWriter, req *regRequest) (int, error) {
	if req.wantIndex >= 0 {
		return req.wantIndex, nil
	}

	indices := s.peers.ListenerIndices()
	switch {
	case len(indices) == 0:
		replyError(conn, "no listener available yet")
		return 0, fmt.Errorf("no listener available")
	case len(indices) > 1:
		replyError(conn, "forwarder must specify index when multiple listeners exist")
		return 0, fmt.Errorf("multiple listeners, must specify index")
	}
	return indices[0], nil
}

// checkProtocolMatch 检查 forwarder 与配对 listener 的协议模式是否一致。
func (s *Server) checkProtocolMatch(conn protocol.FrameReadWriter, index int, mode string) error {
	lis := s.peers.GetListener(index)
	if lis == nil {
		return nil // listener 可能尚未完成注册
	}

	s.metaMu.Lock()
	lisMeta, ok := s.meta[lis]
	s.metaMu.Unlock()

	if ok && lisMeta.mode != mode {
		replyError(conn, fmt.Sprintf("protocol mismatch: listener is %s, forwarder is %s", lisMeta.mode, mode))
		return fmt.Errorf("protocol mismatch at index %d", index)
	}
	return nil
}

// ============================================================================
// Index 分配与限制检查
// ============================================================================

// assignIndex 分配 index：指定则沿用，否则自增。
func (s *Server) assignIndex(wantIndex int) int {
	if wantIndex >= 0 {
		return wantIndex
	}
	idx := s.nextIndex
	s.nextIndex++
	return idx
}

// checkListenerLimit 检查 listener 数量是否超过上限。
func (s *Server) checkListenerLimit(conn protocol.FrameReadWriter) error {
	if s.maxListeners > 0 && s.peers.ListenerCount() >= s.maxListeners {
		replyError(conn, "too many listeners")
		return fmt.Errorf("too many listeners (max=%d)", s.maxListeners)
	}
	return nil
}

// ============================================================================
// 转发循环
// ============================================================================

// relayLoop 是每个客户端连接（非 TUN）的转发主循环。
//
// listener → 广播给所有配对的 forwarder
// forwarder → 转发给配对的 listener
func (s *Server) relayLoop(conn protocol.FrameReadWriter, role clientRole, index int) {
	defer s.removeMeta(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		frame, err := conn.Receive()
		if err != nil {
			break
		}

		if role == roleListener {
			s.peers.BroadcastFromListener(index, frame.Type, frame.Payload)
		} else {
			if err := s.peers.SendToListener(index, frame.Type, frame.Payload); err != nil {
				break
			}
		}
	}

	// 清理
	if role == roleListener {
		s.peers.UnregisterListener(index)
	} else {
		s.peers.UnregisterForwarder(index, conn)
	}
	s.notifyPeerChange()
}

// ============================================================================
// 元信息管理
// ============================================================================

func (s *Server) storeMeta(conn protocol.FrameReadWriter, m *peerMeta) {
	s.metaMu.Lock()
	s.meta[conn] = m
	s.metaMu.Unlock()
}

func (s *Server) removeMeta(conn protocol.FrameReadWriter) {
	s.metaMu.Lock()
	delete(s.meta, conn)
	s.metaMu.Unlock()
}

// ============================================================================
// Web API：数据结构
// ============================================================================

// PeerInfo 是对外暴露的 peer 信息（JSON 序列化用）。
type PeerInfo struct {
	Index  int    `json:"index"`
	Remote string `json:"remote"`
	Uptime string `json:"uptime"`
	Mode   string `json:"mode"`
	Port   string `json:"port,omitempty"`
	Target string `json:"target,omitempty"`
}

// PeersResponse 是 /api/peers 的响应。
type PeersResponse struct {
	Listeners  []PeerInfo `json:"listeners"`
	Forwarders []PeerInfo `json:"forwarders"`
}

// StatsResponse 是 /api/stats 的响应。
type StatsResponse struct {
	Uptime         string `json:"uptime"`
	ListenerCount  int    `json:"listener_count"`
	ForwarderCount int    `json:"forwarder_count"`
	TotalStreams   int    `json:"total_streams"`
	TUNEnabled     bool   `json:"tun_enabled"`
	Version        string `json:"version"`
}

// ============================================================================
// Web API：实现
// ============================================================================

// GetPeers 返回当前所有 listener 和 forwarder 的快照信息。
func (s *Server) GetPeers() PeersResponse {
	resp := PeersResponse{}

	s.peers.RangeListeners(func(idx int, conn protocol.FrameReadWriter) {
		resp.Listeners = append(resp.Listeners, s.buildPeerInfo(conn, idx, "listener"))
	})
	s.peers.RangeForwarders(func(idx int, conn protocol.FrameReadWriter) {
		resp.Forwarders = append(resp.Forwarders, s.buildPeerInfo(conn, idx, "forwarder"))
	})

	return resp
}

// buildPeerInfo 构造单个 peer 的信息。
func (s *Server) buildPeerInfo(conn protocol.FrameReadWriter, idx int, role string) PeerInfo {
	info := PeerInfo{
		Index:  idx,
		Remote: conn.RemoteAddr().String(),
	}

	s.metaMu.Lock()
	m, ok := s.meta[conn]
	s.metaMu.Unlock()

	if ok {
		info.Uptime = formatDuration(time.Since(m.connectedAt))
		info.Mode = m.mode
		if role == "listener" {
			info.Port = m.listenPort
		} else {
			info.Target = m.target
		}
	} else {
		info.Mode = "tcp"
	}

	return info
}

// GetStats 返回服务端运行统计。
func (s *Server) GetStats() StatsResponse {
	return StatsResponse{
		Uptime:         formatDuration(time.Since(s.startTime)),
		ListenerCount:  s.peers.ListenerCount(),
		ForwarderCount: s.peers.ForwarderTotal(),
		TUNEnabled:     s.tunCtx != nil,
		Version:        "1.0.0",
	}
}

// ============================================================================
// 工具函数
// ============================================================================

// replyError 向客户端发送错误帧。单行封装，消除重复的 2 行模式。
func replyError(conn protocol.FrameReadWriter, msg string) {
	conn.Send(protocol.TypeError, []byte(msg))
}

// formatDuration 将 time.Duration 格式化为简洁可读字符串。
//
// 示例：3h2m15s / 5m30s / 42s
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
