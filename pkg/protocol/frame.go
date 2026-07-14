// Package protocol 定义 CloudMirror 的二进制帧协议格式和帧类型常量。
package protocol

// 帧类型常量。每个帧由 1 字节类型 + 3 字节长度 + 可变载荷组成。
const (
	TypeAuth      byte = 0x01 // 客户端 → 服务端：认证请求，载荷为密码
	TypeAuthOK    byte = 0x02 // 服务端 → 客户端：认证通过
	TypeRegister  byte = 0x03 // 客户端 → 服务端：角色注册，载荷为"listener"或"forwarder[,index]"
	TypeRegOK     byte = 0x04 // 服务端 → 客户端：注册成功，载荷为分配到的 index
	TypeDataTCP   byte = 0x05 // TCP 数据帧，前 2 字节为流 ID，后续为应用数据
	TypeDataUDP   byte = 0x06 // UDP 数据帧，前 2 字节为流 ID，后续为应用数据
	TypePeerJoin  byte = 0x07 // 通知对端：新连接建立，载荷为流 ID
	TypePeerLeave byte = 0x08 // 通知对端：连接关闭，载荷为流 ID（或空表示整条隧道断开）
	TypeError     byte = 0x09 // 错误响应，载荷为错误描述
	TypeKeepalive byte = 0x0A // 心跳帧，无载荷，用于保持连接活跃
	TypeDataTUN   byte = 0x0B // TUN 模式 IP 包，载荷为原始 IP 数据报
)

// Frame 表示一帧协议数据，包含帧类型和可选的载荷数据。
type Frame struct {
	Type    byte   // 帧类型，参见 Type* 常量
	Payload []byte // 载荷数据，可能为 nil
}
