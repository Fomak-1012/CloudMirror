package protocol

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
	TypeFileMeta  byte = 0x0C // 文件传输：元信息，载荷 [2B 文件名长度][文件名][8B 大小]
	TypeFileData  byte = 0x0D // 文件传输：数据块，载荷 [4B 块序号][数据]
	TypeFileEnd   byte = 0x0E // 文件传输：结束标记，载荷 [4B 总块数]
)

type Frame struct {
	Type    byte
	Payload []byte
}
