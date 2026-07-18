// Package auth 实现客户端与服务端之间的预共享密钥（PSK）认证握手。
package auth

import (
	"errors"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// ClientAuth 执行客户端认证握手：发送密码 → 等待 AuthOK。
// conn 只需要 Send/Receive 能力，不需要完整的 Tunnel。
func ClientAuth(conn protocol.FrameReadWriter, password string) error {
	if err := conn.Send(protocol.TypeAuth, []byte(password)); err != nil {
		return err
	}
	frame, err := conn.Receive()
	if err != nil {
		return err
	}
	if frame.Type != protocol.TypeAuthOK {
		return errors.New("auth failed: " + string(frame.Payload))
	}
	return nil
}

// ServerAuth 执行服务端认证握手：接收密码 → 校验 → 回复 AuthOK 或 Error。
func ServerAuth(conn protocol.FrameReadWriter, expectedPassword string) error {
	frame, err := conn.Receive()
	if err != nil {
		return err
	}
	if frame.Type != protocol.TypeAuth {
		return errors.New("expected AUTH frame")
	}
	if string(frame.Payload) != expectedPassword {
		conn.Send(protocol.TypeError, []byte("password mismatch"))
		return errors.New("auth failed: wrong password")
	}
	return conn.Send(protocol.TypeAuthOK, nil)
}
