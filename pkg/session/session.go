package session

import (
	"log"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

// Session wraps a tunnel and manages all reading from the underlying connection.
// Only the readLoop goroutine reads from the tunnel; everyone else receives
// frames via the FrameCh channel.
type Session struct {
	tun     *tunnel.Tunnel
	frameCh chan *protocol.Frame // 业务帧分发
	closeCh chan struct{}        // 通知关闭
	doneCh  chan struct{}        // 读泵退出信号
}

// NewSession creates a Session but does not start the read loop yet.
// readTimeout is the idle timeout for the underlying connection;
// if no frame arrives within this duration, the session will be closed.
func NewSession(tun *tunnel.Tunnel, readTimeout time.Duration) *Session {
	s := &Session{
		tun:     tun,
		frameCh: make(chan *protocol.Frame, 16), // 带缓冲，避免读泵阻塞
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	// 启动读泵
	go s.readLoop(readTimeout)
	return s
}

// Send writes a frame through the tunnel. It is safe for concurrent use.
func (s *Session) Send(typ byte, payload []byte) error {
	return s.tun.Send(typ, payload)
}

// FrameCh returns the channel on which incoming frames are delivered.
// This channel will be closed when the session is closed.
func (s *Session) FrameCh() <-chan *protocol.Frame {
	return s.frameCh
}

// Done returns a channel that is closed when the session's read loop exits
// (due to connection loss, timeout, or explicit Close).
func (s *Session) Done() <-chan struct{} {
	return s.doneCh
}

// Close shuts down the session and the underlying tunnel.
func (s *Session) Close() error {
	select {
	case <-s.closeCh: // 已经关闭
	default:
		close(s.closeCh)
	}
	// 等待读泵退出
	<-s.doneCh
	return s.tun.Close()
}

// readLoop is the sole goroutine that reads from the tunnel.
func (s *Session) readLoop(readTimeout time.Duration) {
	defer close(s.doneCh)
	defer close(s.frameCh)

	log.Printf("[session] readLoop started")
	for {
		// 设置读超时，超时后 ReadFrame 会返回错误，触发连接关闭
		s.tun.SetReadDeadline(time.Now().Add(readTimeout))

		frame, err := s.tun.Receive()
		if err != nil {
			// 连接断开或超时，退出读泵
			select {
			case <-s.closeCh:
				// 正常关闭
			default:
				log.Printf("[session] read error: %v", err)
			}
			return
		}

		// 将帧放入 channel，如果 channel 满则丢弃（可根据需求调整）
		select {
		case s.frameCh <- frame:
		case <-s.closeCh:
			return
		default:
			// channel 满，丢弃帧并记录（生产环境应监控）
			log.Printf("[session] frame dropped (type=%d)", frame.Type)
		}
	}
}
