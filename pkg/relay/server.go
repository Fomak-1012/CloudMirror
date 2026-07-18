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

// peerMeta 记录每个 peer 连接的元信息，用于 Web 控制台展示。
type peerMeta struct {
	connectedAt time.Time
	mode        string // "tcp", "udp", "tun"
	listenPort  string // listener 端口（仅 listener）
	target      string // forwarder 目标地址（仅 forwarder）
}

// Server 是中继服务的核心，负责接纳客户端连接、认证、注册，并将
// listener 与 forwarder 配对转发数据。
type Server struct {
	peers        *PeerMap         // 配对管理器
	nextIndex    int              // 自动分配的下一个 index
	maxListeners int              // 最大 listener 数量（0 表示无限制）
	password     string           // 预共享密钥
	tunCtx       *ServerTUNContext // TUN 模式上下文（-t 时非空）
	startTime    time.Time        // 服务启动时间
	onPeerChange func()           // peers 变更时的回调（通常用于触发 SSE 推送）

	metaMu sync.Mutex
	meta   map[protocol.FrameReadWriter]*peerMeta // 连接 → 元信息
	wg     sync.WaitGroup                          // 追踪活跃连接，用于优雅关闭
}

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

// StartTUNMode 初始化 TUN 模式。
func (s *Server) StartTUNMode() error {
	ctx, err := NewServerTUNContext()
	if err != nil {
		return err
	}
	s.tunCtx = ctx
	log.Printf("[server] TUN mode enabled, device=%s", ctx.DevName())
	return nil
}

// TrackConnection 递增活跃连接计数，用于优雅关闭。
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

// HandleClient 处理一个客户端连接的全生命周期。
func (s *Server) HandleClient(conn net.Conn) {
	s.TrackConnection()
	defer s.TrackConnectionDone()

	t := tunnel.NewTunnel(conn)
	defer t.Close()

	if err := auth.ServerAuth(t, s.password); err != nil {
		return
	}

	frame, err := t.Receive()
	if err != nil || frame.Type != protocol.TypeRegister {
		return
	}

	// 解析注册载荷，支持多种格式：
	//   旧格式: listener[,<index>]
	//   新格式: listener,<index>,<mode>,<port|target>
	//   TUN:    listener[,<index>],tun,<cidr>
	payload := string(frame.Payload)
	parts := strings.Split(payload, ",")

	role := parts[0]
	wantIndex := -1
	mode := "tcp"     // 默认 TCP
	metaStr := ""     // listener 端口 或 forwarder 目标地址
	isTUN := false
	var tunCIDR string

	// 解析 parts[1]: index 或模式标记
	if len(parts) > 1 && parts[1] != "" {
		if _, err := fmt.Sscanf(parts[1], "%d", &wantIndex); err != nil {
			// parts[1] 不是数字，可能是 "tun"（旧 TUN 格式）
			if parts[1] == "tun" {
				isTUN = true
				if len(parts) > 2 {
					tunCIDR = parts[2]
				}
			}
		}
	}
	// 解析 parts[2] 及之后：模式信息
	if len(parts) > 2 && !isTUN {
		if parts[2] == "tun" {
			isTUN = true
			if len(parts) > 3 {
				tunCIDR = parts[3]
			}
		} else {
			mode = parts[2]
			if len(parts) > 3 {
				metaStr = parts[3]
			}
		}
	}

	if isTUN && s.tunCtx == nil {
		t.Send(protocol.TypeError, []byte("server not in TUN mode (use -t)"))
		return
	}

	var index int
	switch role {
	case "listener":
		if isTUN {
			if wantIndex >= 0 {
				index = wantIndex
			} else {
				index = s.nextIndex
				s.nextIndex++
			}
			if s.maxListeners > 0 && s.peers.ListenerCount() >= s.maxListeners {
				t.Send(protocol.TypeError, []byte("too many listeners"))
				return
			}
			assignedIP, err := s.tunCtx.RegisterClient(t, index, tunCIDR)
			if err != nil {
				t.Send(protocol.TypeError, []byte(err.Error()))
				return
			}
			s.peers.RegisterListener(index, t)
			s.storeMeta(t, &peerMeta{connectedAt: time.Now(), mode: "tun", listenPort: tunCIDR})

			reply := fmt.Sprintf("%d,%s", index, assignedIP)
			t.Send(protocol.TypeRegOK, []byte(reply))
			serverTUNRelayLoop(t, s.tunCtx, index)
			return
		}

		if wantIndex >= 0 {
			index = wantIndex
		} else {
			index = s.nextIndex
			s.nextIndex++
		}
		if s.maxListeners > 0 && s.peers.ListenerCount() >= s.maxListeners {
			t.Send(protocol.TypeError, []byte("too many listeners"))
			return
		}
		s.peers.RegisterListener(index, t)
		s.storeMeta(t, &peerMeta{connectedAt: time.Now(), mode: mode, listenPort: metaStr})
		s.notifyPeerChange()

	case "forwarder":
		if wantIndex >= 0 {
			index = wantIndex
		} else {
			indices := s.peers.ListenerIndices()
			if len(indices) == 0 {
				t.Send(protocol.TypeError, []byte("no listener available yet"))
				return
			}
			if len(indices) > 1 {
				t.Send(protocol.TypeError, []byte("forwarder must specify index when multiple listeners exist"))
				return
			}
			index = indices[0]
		}
		// 检查协议匹配：TCP-only forwarder 不能连 UDP-only listener（反之亦然）
		if lis := s.peers.GetListener(index); lis != nil {
			s.metaMu.Lock()
			lisMeta, ok := s.meta[lis]
			s.metaMu.Unlock()
			if ok && lisMeta.mode != mode {
				t.Send(protocol.TypeError, []byte(fmt.Sprintf("protocol mismatch: listener is %s, forwarder is %s", lisMeta.mode, mode)))
				return
			}
		}
		s.peers.RegisterForwarder(index, t)
		s.storeMeta(t, &peerMeta{connectedAt: time.Now(), mode: mode, target: metaStr})
		s.notifyPeerChange()
	}

	t.Send(protocol.TypeRegOK, []byte(fmt.Sprintf("%d", index)))

	// 启动服务端心跳，配合 relayLoop 中的读超时检测死连接
	stopKeepalive := make(chan struct{})
	StartServerKeepAlive(t, 30*time.Second, stopKeepalive)
	s.relayLoop(t, role, index)
	close(stopKeepalive)
}

// storeMeta 记录连接的元信息。
func (s *Server) storeMeta(conn protocol.FrameReadWriter, m *peerMeta) {
	s.metaMu.Lock()
	s.meta[conn] = m
	s.metaMu.Unlock()
}

// removeMeta 删除连接的元信息。
func (s *Server) removeMeta(conn protocol.FrameReadWriter) {
	s.metaMu.Lock()
	delete(s.meta, conn)
	s.metaMu.Unlock()
}

// relayLoop 是每个客户端连接的转发主循环。
func (s *Server) relayLoop(conn protocol.FrameReadWriter, role string, index int) {
	defer s.removeMeta(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		frame, err := conn.Receive()
		if err != nil {
			break
		}
		if role == "listener" {
			s.peers.BroadcastFromListener(index, frame.Type, frame.Payload)
		} else {
			if err := s.peers.SendToListener(index, frame.Type, frame.Payload); err != nil {
				break
			}
		}
	}

	if role == "listener" {
		s.peers.UnregisterListener(index)
	} else {
		s.peers.UnregisterForwarder(index, conn)
	}
	s.notifyPeerChange()
}

// ---- Web API 数据结构 ----

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

// GetPeers 返回当前所有 listener 和 forwarder 的快照信息。
func (s *Server) GetPeers() PeersResponse {
	resp := PeersResponse{}

	s.peers.mu.Lock()
	for idx, lis := range s.peers.listeners {
		info := s.buildPeerInfo(lis, idx, "listener")
		resp.Listeners = append(resp.Listeners, info)
	}
	for idx, fwdList := range s.peers.forwarders {
		for _, fwd := range fwdList {
			info := s.buildPeerInfo(fwd, idx, "forwarder")
			resp.Forwarders = append(resp.Forwarders, info)
		}
	}
	s.peers.mu.Unlock()

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
	totalFwd := 0
	s.peers.mu.Lock()
	for _, fwdList := range s.peers.forwarders {
		totalFwd += len(fwdList)
	}
	lisCount := len(s.peers.listeners)
	s.peers.mu.Unlock()

	return StatsResponse{
		Uptime:         formatDuration(time.Since(s.startTime)),
		ListenerCount:  lisCount,
		ForwarderCount: totalFwd,
		TUNEnabled:     s.tunCtx != nil,
		Version:        "1.0.0",
	}
}

// formatDuration 将 time.Duration 格式化为可读字符串。
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
