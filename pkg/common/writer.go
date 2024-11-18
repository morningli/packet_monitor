package common

import (
	"github.com/google/gopacket/layers"
	"net"
)

type Writer interface {
	FlowIn(srcHost net.IP, srcPort layers.TCPPort, data []byte) error
	FlowOut(dstHost net.IP, dstPort layers.TCPPort, data []byte) error
}
