package common

import (
	"github.com/google/gopacket"
)

type Monitor interface {
	Feed(packet gopacket.Packet)
	SetWriter(writer Writer)
}
