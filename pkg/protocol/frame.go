package protocol

const (
	TypeAuth      byte = 0x01
	TypeAuthOK    byte = 0x02
	TypeRegister  byte = 0x03
	TypeRegOK     byte = 0x04
	TypeDataTCP   byte = 0x05
	TypeDataUDP   byte = 0x06
	TypePeerJoin  byte = 0x07
	TypePeerLeave byte = 0x08
	TypeError     byte = 0x09
	TypeKeepalive byte = 0x0A
	TypeDataTUN   byte = 0x0B
)

type Frame struct {
	Type    byte
	Payload []byte
}
