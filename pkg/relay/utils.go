package relay

func uint16ToBytes(val uint16) []byte {
	b := make([]byte, 2)
	b[0] = byte(val >> 8)
	b[1] = byte(val)
	return b
}
