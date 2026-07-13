package session

import (
	"log"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// Session wraps a FrameReadWriter and manages all reading from the
// underlying connection. Only the readLoop goroutine reads from the
// connection; everyone else receives frames via the FrameCh channel.
type Session struct {
	conn    protocol.FrameReadWriter
	frameCh chan *protocol.Frame // 业务帧分发
	closeCh chan struct{}        // 通知关闭
	doneCh  chan struct{}        // 读泵退出信号
}

// NewSession creates a Session and starts the read loop.
// readTimeout is the idle timeout for the underlying connection;
// if no frame arrives within this duration, the session will be closed.
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

// Send writes a frame through the connection. It is safe for concurrent use.
func (s *Session) Send(typ byte, payload []byte) error {
	return s.conn.Send(typ, payload)
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

// Close shuts down the session and the underlying connection.
func (s *Session) Close() error {
	select {
	case <-s.closeCh: // 已经关闭
	default:
		close(s.closeCh)
	}
	// 等待读泵退出
	<-s.doneCh
	return s.conn.Close()
}

// readLoop is the sole goroutine that reads from the connection.
func (s *Session) readLoop(readTimeout time.Duration) {
	defer close(s.doneCh)
	defer close(s.frameCh)

	log.Printf("[session] readLoop started")
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
