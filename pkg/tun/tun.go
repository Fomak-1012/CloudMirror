// Package tun 提供 Linux TUN 虚拟网卡的创建和读写能力。
// TUN 设备工作在 IP 层（OSI 第 3 层），读写的是原始 IP 数据报。
package tun

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	cIFF_TUN   = 0x0001 // TUN 设备标志
	cIFF_NO_PI = 0x1000 // 不附加 4 字节的协议信息头
	cTUNSETIFF = 0x400454ca // TUNSETIFF ioctl 命令
)

// Dev 表示一个 TUN 虚拟网卡设备，可读写原始 IP 包。
type Dev struct {
	file *os.File // /dev/net/tun 的文件描述符
	name string   // 内核分配的设备名（如 crfl0）
}

// Name 返回内核分配的设备名称。
func (d *Dev) Name() string { return d.name }

// New 创建一个 TUN 设备。name 为请求名称，若以 %d 结尾，内核自动编号。
func New(name string) (*Dev, error) {
	fd, err := syscall.Open("/dev/net/tun", syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open /dev/net/tun: %w", err)
	}

	// 通过 ioctl TUNSETIFF 配置设备
	var ifr [40]byte
	copy(ifr[:syscall.IFNAMSIZ], name)
	flags := uint16(cIFF_TUN | cIFF_NO_PI)
	*(*uint16)(unsafe.Pointer(&ifr[syscall.IFNAMSIZ])) = flags

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd),
		uintptr(cTUNSETIFF), uintptr(unsafe.Pointer(&ifr)))
	if errno != 0 {
		syscall.Close(fd)
		return nil, fmt.Errorf("TUNSETIFF ioctl: %w", errno)
	}

	// 读取内核返回的实际设备名（可能因重名而不同）
	devName := string(ifr[:syscall.IFNAMSIZ])
	for i, b := range ifr[:syscall.IFNAMSIZ] {
		if b == 0 {
			devName = string(ifr[:i])
			break
		}
	}

	return &Dev{
		file: os.NewFile(uintptr(fd), "/dev/net/tun"),
		name: devName,
	}, nil
}

// Read 从 TUN 设备读取一个原始 IP 包。
func (d *Dev) Read(p []byte) (int, error) { return d.file.Read(p) }

// Write 向 TUN 设备写入一个原始 IP 包。
func (d *Dev) Write(p []byte) (int, error) { return d.file.Write(p) }

// Close 关闭 TUN 设备（注意：不会自动删除网络接口）。
func (d *Dev) Close() error { return d.file.Close() }
