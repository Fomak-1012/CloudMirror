package relay

import (
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// Mode 抽象了客户端的转发模式。每种模式（TCP/UDP listener/forwarder、TUN）
// 都实现此接口，Run 方法阻塞执行直到连接断开。
type Mode interface {
	// Run 启动当前模式的转发循环，阻塞直到 Session 关闭或发生致命错误。
	Run(sess *session.Session, index int) error
}

// ---- TCP Listener ----

// tcpListenMode 在本地监听 TCP 端口，将新连接通过 Session 转发到 forwarder。
type tcpListenMode struct{ addr string }

func (m tcpListenMode) Run(sess *session.Session, index int) error {
	ln, err := net.Listen("tcp", m.addr)
	if err != nil {
		return fmt.Errorf("tcp listen: %w", err)
	}
	defer ln.Close()
	log.Printf("TCP listener running, listen %s", m.addr)
	return RunTCPListener(sess, ln)
}

// ---- UDP Listener ----

// udpListenMode 在本地监听 UDP 端口，将数据包通过 Session 转发到 forwarder。
type udpListenMode struct{ addr string }

func (m udpListenMode) Run(sess *session.Session, index int) error {
	log.Printf("UDP listener running, listen %s", m.addr)
	return RunUDPListener(sess, m.addr)
}

// ---- TCP Forwarder ----

// tcpForwardMode 将 Session 中的新连接请求转发到本地 TCP 目标地址。
type tcpForwardMode struct{ target string }

func (m tcpForwardMode) Run(sess *session.Session, index int) error {
	log.Printf("TCP forwarder running, forward to %s", m.target)
	return RunTCPForwarder(sess, m.target)
}

// ---- UDP Forwarder ----

// udpForwardMode 将 Session 中的 UDP 数据包转发到本地 UDP 目标地址。
type udpForwardMode struct{ target string }

func (m udpForwardMode) Run(sess *session.Session, index int) error {
	log.Printf("UDP forwarder running, forward to %s", m.target)
	return RunUDPForwarder(sess, m.target)
}

// ---- TUN Listener ----

// tunListenMode 在本地创建 TUN 虚拟网卡并分配 IP，通过 Session 转发 IP 包。
type tunListenMode struct {
	cidr       string // 虚拟网络 CIDR
	assignedIP string // 服务端分配的虚拟 IP
}

func (m tunListenMode) Run(sess *session.Session, index int) error {
	log.Printf("TUN listener running, IP=%s, CIDR=%s", m.assignedIP, m.cidr)
	return runTUNListener(sess, m.cidr, index, m.assignedIP)
}
