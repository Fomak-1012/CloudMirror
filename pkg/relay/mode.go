package relay

import (
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// Mode 抽象了客户端的转发模式。
type Mode interface {
	Run(sess *session.Session, index int) error
}

// ModeFunc 将普通函数适配为 Mode 接口，避免为每种模式定义独立的 struct。
type ModeFunc func(sess *session.Session, index int) error

func (f ModeFunc) Run(sess *session.Session, index int) error { return f(sess, index) }

// newClientMode 根据角色和参数创建对应的转发模式（工厂函数）。
func newClientMode(role, listenSpec string, forwardPort, index int, assignedIP string, udpOnly, isTUN bool) (Mode, error) {
	switch role {
	case "listener":
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
	case "forwarder":
		target := fmt.Sprintf("127.0.0.1:%d", forwardPort)
		if udpOnly {
			return ModeFunc(func(sess *session.Session, _ int) error {
				log.Printf("UDP forwarder running, forward to %s", target)
				return RunUDPForwarder(sess, target)
			}), nil
		}
		return ModeFunc(func(sess *session.Session, _ int) error {
			log.Printf("TCP forwarder running, forward to %s", target)
			return RunTCPForwarder(sess, target)
		}), nil
	default:
		return nil, fmt.Errorf("unknown role: %s", role)
	}
}
