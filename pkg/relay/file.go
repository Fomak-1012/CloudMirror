package relay

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// ============================================================================
// 文件传输载荷编码
// ============================================================================

// encodeFileMeta 构造 FileMeta 帧载荷：[2B 文件名长度][文件名][8B 文件大小]。
func encodeFileMeta(name string, size int64) []byte {
	payload := make([]byte, 2+len(name)+8)
	binary.BigEndian.PutUint16(payload[:2], uint16(len(name)))
	copy(payload[2:], name)
	binary.BigEndian.PutUint64(payload[2+len(name):], uint64(size))
	return payload
}

// encodeFileChunk 构造 FileData 帧载荷：[4B 块序号][数据]。
func encodeFileChunk(seq uint32, data []byte) []byte {
	payload := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(payload[:4], seq)
	copy(payload[4:], data)
	return payload
}

// encodeFileEnd 构造 FileEnd 帧载荷：[4B 总块数]。
func encodeFileEnd(totalChunks uint32) []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, totalChunks)
	return payload
}

// ============================================================================
// 文件发送
// ============================================================================

// RunFileSender 读取本地文件并通过 Session 分块发送。
//
// 流程：FileMeta → N × FileData → FileEnd
func RunFileSender(sess *session.Session, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	fileName := filepath.Base(filePath)
	fileSize := info.Size()

	// 阶段一：发送元信息
	if err := sess.Send(protocol.TypeFileMeta, encodeFileMeta(fileName, fileSize)); err != nil {
		return fmt.Errorf("send meta: %w", err)
	}
	log.Printf("[file] sending %s (%d bytes)", fileName, fileSize)

	// 阶段二：分块发送
	const chunkSize = 32 * 1024
	buf := make([]byte, chunkSize)
	var seq uint32

	for {
		n, err := f.Read(buf)
		if n > 0 {
			if err := sess.Send(protocol.TypeFileData, encodeFileChunk(seq, buf[:n])); err != nil {
				return fmt.Errorf("send chunk %d: %w", seq, err)
			}
			seq++
		}
		if err != nil {
			break // EOF
		}
	}

	// 阶段三：发送结束标记
	if err := sess.Send(protocol.TypeFileEnd, encodeFileEnd(seq)); err != nil {
		return fmt.Errorf("send end: %w", err)
	}
	log.Printf("[file] sent %d chunks, done", seq)
	return nil
}

// ============================================================================
// 文件接收
// ============================================================================

// recvState 跟踪文件接收过程中的状态。
type recvState struct {
	file           *os.File
	fileName       string
	expectedChunks uint32
	receivedChunks uint32
}

// RunFileReceiver 从 Session 接收文件并写入磁盘。
//
// 流程：等 FileMeta → N × FileData → FileEnd
func RunFileReceiver(sess *session.Session, outputDir string) error {
	var state recvState

	for frame := range sess.FrameCh() {
		switch frame.Type {
		case protocol.TypeFileMeta:
			if err := state.handleMeta(frame, outputDir); err != nil {
				return err
			}

		case protocol.TypeFileData:
			if err := state.handleData(frame); err != nil {
				return err
			}

		case protocol.TypeFileEnd:
			return state.handleEnd(frame)
		}
	}
	return fmt.Errorf("session closed before receiving FileEnd")
}

func (s *recvState) handleMeta(frame *protocol.Frame, outputDir string) error {
	if len(frame.Payload) < 10 {
		return nil
	}
	nameLen := binary.BigEndian.Uint16(frame.Payload[:2])
	fileName := string(frame.Payload[2 : 2+nameLen])
	fileSize := binary.BigEndian.Uint64(frame.Payload[2+nameLen:])

	outPath := filepath.Join(outputDir, fileName)
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}

	s.file = f
	s.fileName = fileName
	log.Printf("[file] receiving %s (%d bytes) → %s", fileName, fileSize, outPath)
	return nil
}

func (s *recvState) handleData(frame *protocol.Frame) error {
	if s.file == nil || len(frame.Payload) < 4 {
		return nil
	}
	seq := binary.BigEndian.Uint32(frame.Payload[:4])
	if _, err := s.file.Write(frame.Payload[4:]); err != nil {
		s.file.Close()
		return fmt.Errorf("write chunk %d: %w", seq, err)
	}
	s.receivedChunks++
	return nil
}

func (s *recvState) handleEnd(frame *protocol.Frame) error {
	if s.file != nil {
		s.file.Close()
	}
	if len(frame.Payload) >= 4 {
		s.expectedChunks = binary.BigEndian.Uint32(frame.Payload)
	}
	if s.receivedChunks == s.expectedChunks {
		log.Printf("[file] received %s: %d/%d chunks", s.fileName, s.receivedChunks, s.expectedChunks)
	} else {
		log.Printf("[file] warning: expected %d chunks, got %d", s.expectedChunks, s.receivedChunks)
	}
	return nil
}
