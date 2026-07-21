package relay

import (
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// Mode 抽象了客户端进入转发模式后的运行行为。
// 每个角色（listener / forwarder / file sender / file receiver）对应一种 Mode 实现。
type Mode interface {
	Run(sess *session.Session, index int) error
}

// ModeFunc 将普通函数适配为 Mode 接口。
type ModeFunc func(sess *session.Session, index int) error

func (f ModeFunc) Run(sess *session.Session, index int) error { return f(sess, index) }

// ============================================================================
// 工厂函数
// ============================================================================

// newClientMode 根据角色和参数创建对应的转发模式。
func newClientMode(role, listenSpec string, forwardPort, index int, assignedIP string, udpOnly, isTUN bool, fileSendPath, fileRecvDir string) (Mode, error) {
	switch clientRole(role) {
	case roleFileSender:
		return newFileSenderMode(fileSendPath), nil
	case roleFileReceiver:
		return newFileReceiverMode(fileRecvDir), nil
	case roleListener:
		return newListenerMode(listenSpec, index, assignedIP, udpOnly, isTUN)
	case roleForwarder:
		return newForwarderMode(forwardPort, udpOnly), nil
	default:
		return nil, fmt.Errorf("unknown role: %s", role)
	}
}

// ============================================================================
// 各角色模式构造
// ============================================================================

func newFileSenderMode(filePath string) Mode {
	return ModeFunc(func(sess *session.Session, _ int) error {
		log.Printf("File sender running, file=%s", filePath)
		return RunFileSender(sess, filePath)
	})
}

func newFileReceiverMode(outputDir string) Mode {
	return ModeFunc(func(sess *session.Session, _ int) error {
		log.Printf("File receiver running, output=%s", outputDir)
		return RunFileReceiver(sess, outputDir)
	})
}

func newListenerMode(listenSpec string, index int, assignedIP string, udpOnly, isTUN bool) (Mode, error) {
	if isTUN {
		return ModeFunc(func(sess *session.Session, _ int) error {
			log.Printf("TUN listener running, IP=%s, CIDR=%s", assignedIP, listenSpec)
			return runTUNListener(sess, listenSpec, index, assignedIP)
		}), nil
	}

	port, err := resolvePort(listenSpec, index)
	if err != nil {
		return nil, fmt.Errorf("resolve port: %w", err)
	}
	addr := fmt.Sprintf(":%d", port)

	if udpOnly {
		return ModeFunc(func(sess *session.Session, _ int) error {
			log.Printf("UDP listener running, listen %s", addr)
			return RunUDPListener(sess, addr)
		}), nil
	}

	return ModeFunc(func(sess *session.Session, _ int) error {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp listen: %w", err)
		}
		defer ln.Close()
		log.Printf("TCP listener running, listen %s", addr)
		return RunTCPListener(sess, ln)
	}), nil
}

func newForwarderMode(forwardPort int, udpOnly bool) Mode {
	target := fmt.Sprintf("127.0.0.1:%d", forwardPort)

	if udpOnly {
		return ModeFunc(func(sess *session.Session, _ int) error {
			log.Printf("UDP forwarder running, forward to %s", target)
			return RunUDPForwarder(sess, target)
		})
	}

	return ModeFunc(func(sess *session.Session, _ int) error {
		log.Printf("TCP forwarder running, forward to %s", target)
		return RunTCPForwarder(sess, target)
	})
}
