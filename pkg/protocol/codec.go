// Package protocol 实现了一个简单的二进制帧协议。
// 帧格式：1 字节类型 + 3 字节大端长度（24 位）+ 可变长度有效载荷。
package protocol

import (
	"io"
)

// PutUint24 将一个 24 位无符号整数（val 的低 24 位）按大端序写入 bt 的前 3 个字节。
func PutUint24(bt []byte, val uint32) {
	bt[0] = byte(val >> 16)
	bt[1] = byte(val >> 8)
	bt[2] = byte(val)
}

// Uint24 将 bt 的前 3 个字节按大端序还原为 24 位无符号整数。
func Uint24(bt []byte) uint32 {
	return uint32(bt[0])<<16 | uint32(bt[1])<<8 | uint32(bt[2])
}

// WriteFrame 将一帧数据写入 wrt。帧头 4 字节：1 字节类型 + 3 字节大端长度。
func WriteFrame(wrt io.Writer, frameType byte, payload []byte) error {
	header := make([]byte, 4)
	header[0] = frameType
	PutUint24(header[1:], uint32(len(payload)))
	if _, err := wrt.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := wrt.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadFrame 从 rd 中读满一帧。先读 4 字节头，再根据头中的长度读载荷。
func ReadFrame(rd io.Reader) (*Frame, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(rd, header); err != nil {
		return nil, err
	}
	frameType := header[0]
	length := Uint24(header[1:])
	if length > 0 {
		payload := make([]byte, length)
		if _, err := io.ReadFull(rd, payload); err != nil {
			return nil, err
		}
		return &Frame{Type: frameType, Payload: payload}, nil
	}
	return &Frame{Type: frameType, Payload: nil}, nil
}
