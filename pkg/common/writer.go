package common

import (
	"github.com/google/gopacket/layers"
	"net"
)

type Writer interface {
	Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error
}
