package relay

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/Fomak-1012/CloudMirror/pkg/auth"
	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

// Server 是中继服务的核心，负责接纳客户端连接、认证、注册，并将
// listener 与 forwarder 配对转发数据。
type Server struct {
	peers        *PeerMap         // 配对管理器
	nextIndex    int              // 自动分配的下一个 index
	maxListeners int              // 最大 listener 数量（0 表示无限制）
	password     string           // 预共享密钥
	tunCtx       *ServerTUNContext // TUN 模式上下文（-t 时非空）
}

// NewServer 创建一个中继服务实例。
func NewServer(password string, maxListeners int) *Server {
	return &Server{
		peers:        NewPeerMap(),
		password:     password,
		maxListeners: maxListeners,
	}
}

// StartTUNMode 初始化 TUN 模式：创建服务端 TUN 设备并启动后台读循环，
// 将 IP 包从 TUN 设备路由到对应客户端。
func (s *Server) StartTUNMode() error {
	ctx, err := NewServerTUNContext()
	if err != nil {
		return err
	}
	s.tunCtx = ctx
	log.Printf("[server] TUN mode enabled, device=%s", ctx.DevName())
	return nil
}

// HandleClient 处理一个客户端连接的全生命周期：
// 认证 → 解析注册信息 → 分配 index → 进入 relayLoop 转发。
func (s *Server) HandleClient(conn net.Conn) {
	log.Printf("[server] HandleClient: new connection from %s", conn.RemoteAddr())
	t := tunnel.NewTunnel(conn)
	defer func() {
		log.Printf("[server] HandleClient: closing tunnel from %s", conn.RemoteAddr())
		t.Close()
	}()

	// 阶段 1：认证
	if err := auth.ServerAuth(t, s.password); err != nil {
		return
	}

	// 阶段 2：接收注册帧
	frame, err := t.Receive()
	if err != nil || frame.Type != protocol.TypeRegister {
		return
	}

	// 阶段 3：解析注册载荷
	// 普通模式：listener[,<index>] 或 forwarder[,<index>]
	// TUN 模式：listener[,<index>],tun,<cidr>
	payload := string(frame.Payload)
	parts := strings.Split(payload, ",")

	role := parts[0]
	wantIndex := -1
	isTUN := false
	var tunCIDR string

	if len(parts) > 1 {
		if _, err := fmt.Sscanf(parts[1], "%d", &wantIndex); err == nil {
			if len(parts) > 2 && parts[2] == "tun" && len(parts) > 3 {
				isTUN = true
				tunCIDR = parts[3]
			}
		} else if parts[1] == "tun" {
			isTUN = true
			if len(parts) > 2 {
				tunCIDR = parts[2]
			}
		}
	}

	if isTUN && s.tunCtx == nil {
		t.Send(protocol.TypeError, []byte("server not in TUN mode (use -t)"))
		return
	}

	// 阶段 4：分配 index 并注册到 PeerMap
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
			// 向 TUN 上下文注册，获取分配的虚拟 IP
			assignedIP, err := s.tunCtx.RegisterClient(t, index, tunCIDR)
			if err != nil {
				t.Send(protocol.TypeError, []byte(err.Error()))
				return
			}
			s.peers.RegisterListener(index, t)

			reply := fmt.Sprintf("%d,%s", index, assignedIP)
			t.Send(protocol.TypeRegOK, []byte(reply))
			log.Printf("[server] tun-listener registered at index=%d, IP=%s", index, assignedIP)

			serverTUNRelayLoop(t, s.tunCtx, index)
			return
		}

		// 普通 listener
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

	case "forwarder":
		if wantIndex >= 0 {
			index = wantIndex
		} else {
			// 自动配对：仅一个 listener 时自动选择其 index
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
		s.peers.RegisterForwarder(index, t)
	}

	// 阶段 5：回复注册成功并进入转发循环
	t.Send(protocol.TypeRegOK, []byte(fmt.Sprintf("%d", index)))
	log.Printf("[server] %s registered at index=%d", role, index)

	s.relayLoop(t, role, index)
}

// relayLoop 是每个客户端连接的转发主循环。
// listener：收到的帧广播给该 index 下的所有 forwarder。
// forwarder：收到的帧转发给该 index 的 listener。
func (s *Server) relayLoop(conn protocol.FrameReadWriter, role string, index int) {
	log.Printf("[server] relayLoop %s[%d]: started", role, index)
	for {
		frame, err := conn.Receive()
		if err != nil {
			log.Printf("[server] relayLoop %s[%d]: recv error: %v — exiting loop", role, index, err)
			break
		}

		if role == "listener" {
			// 广播：一份数据 → 所有 forwarder
			s.peers.BroadcastFromListener(index, frame.Type, frame.Payload)
		} else {
			// 单播：forwarder 数据 → listener
			if err := s.peers.SendToListener(index, frame.Type, frame.Payload); err != nil {
				break
			}
		}
	}

	// 清理：从 PeerMap 移除自己，通知对端
	log.Printf("[server] relayLoop %s[%d]: cleaning up", role, index)
	if role == "listener" {
		s.peers.UnregisterListener(index)
	} else {
		s.peers.UnregisterForwarder(index, conn)
	}
}
