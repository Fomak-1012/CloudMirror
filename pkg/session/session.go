// Package session 在 FrameReadWriter 之上提供异步帧收发能力，
// 通过后台读泵将"读帧"和"处理帧"解耦到不同的 goroutine。
package session

import (
	"log"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// Session 包装一个 FrameReadWriter，启动后台 goroutine 持续读取帧，
// 并通过 channel 分发给业务层。业务层只需从 FrameCh 读取即可。
type Session struct {
	conn    protocol.FrameReadWriter // 底层帧连接
	frameCh chan *protocol.Frame     // 帧分发通道，读泵写入，业务层读取
	closeCh chan struct{}            // 通知读泵主动关闭
	doneCh  chan struct{}            // 读泵退出信号
}

// NewSession 创建 Session 并启动后台读泵。readTimeout 为空闲超时时间，
// 超时后底层连接会被关闭，读泵退出。
func NewSession(conn protocol.FrameReadWriter, readTimeout time.Duration) *Session {
	s := &Session{
		conn:    conn,
		frameCh: make(chan *protocol.Frame, 16),
		closeCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	go s.readLoop(readTimeout)
	return s
}

// Send 通过底层连接发送一帧。并发安全。
func (s *Session) Send(typ byte, payload []byte) error {
	return s.conn.Send(typ, payload)
}

// FrameCh 返回帧分发通道。通道关闭表示 Session 已结束。
func (s *Session) FrameCh() <-chan *protocol.Frame {
	return s.frameCh
}

// Done 返回一个在 Session 读泵退出后关闭的通道。
func (s *Session) Done() <-chan struct{} {
	return s.doneCh
}

// Close 关闭 Session：通知读泵停止，等待其退出，然后关闭底层连接。
func (s *Session) Close() error {
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	<-s.doneCh
	return s.conn.Close()
}

// readLoop 是唯一的帧读取 goroutine。它持续从底层连接读帧并写入 frameCh。
// 当连接断开、超时或被主动关闭时退出。
func (s *Session) readLoop(readTimeout time.Duration) {
	defer close(s.doneCh)
	defer close(s.frameCh)

	for {
		s.conn.SetReadDeadline(time.Now().Add(readTimeout))

		frame, err := s.conn.Receive()
		if err != nil {
			select {
			case <-s.closeCh:
			default:
				log.Printf("[session] read error: %v", err)
			}
			return
		}

		select {
		case s.frameCh <- frame:
		case <-s.closeCh:
			return
		default:
			log.Printf("[session] frame dropped (type=%d)", frame.Type)
		}
	}
}
