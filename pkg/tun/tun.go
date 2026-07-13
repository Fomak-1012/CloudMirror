package tun

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

const (
	cIFF_TUN   = 0x0001
	cIFF_NO_PI = 0x1000
	cTUNSETIFF = 0x400454ca
)

// Dev is a TUN device that can read and write raw IP packets.
type Dev struct {
	file *os.File
	name string
}

// Name returns the kernel-assigned device name.
func (d *Dev) Name() string { return d.name }

// New creates a TUN device with the given name.
// On Linux this opens /dev/net/tun and configures it via ioctl.
func New(name string) (*Dev, error) {
	fd, err := syscall.Open("/dev/net/tun", syscall.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("open /dev/net/tun: %w", err)
	}

	var ifr [40]byte
	copy(ifr[:syscall.IFNAMSIZ], name)
	flags := uint16(cIFF_TUN | cIFF_NO_PI)
	*(*uint16)(unsafe.Pointer(&ifr[syscall.IFNAMSIZ])) = flags

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(cTUNSETIFF), uintptr(unsafe.Pointer(&ifr)))
	if errno != 0 {
		syscall.Close(fd)
		return nil, fmt.Errorf("TUNSETIFF ioctl: %w", errno)
	}

	// Read back the actual device name (may differ if name was empty or truncated).
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

// Read reads a raw IP packet from the TUN device.
func (d *Dev) Read(p []byte) (int, error) {
	return d.file.Read(p)
}

// Write writes a raw IP packet to the TUN device.
func (d *Dev) Write(p []byte) (int, error) {
	return d.file.Write(p)
}

// Close closes the TUN device.
func (d *Dev) Close() error {
	return d.file.Close()
}

// File returns the underlying os.File for use in select/poll.
func (d *Dev) File() *os.File {
	return d.file
}
