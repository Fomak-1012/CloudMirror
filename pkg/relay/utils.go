package relay

// uint16ToBytes 将 uint16 按大端序编码为 2 字节切片。
// 用于在数据帧载荷前附加流 ID。
func uint16ToBytes(val uint16) []byte {
	b := make([]byte, 2)
	b[0] = byte(val >> 8)
	b[1] = byte(val)
	return b
}
