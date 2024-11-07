package common

import (
	"fmt"
	"github.com/google/gopacket/layers"
	"net"
)

func RemoteKey(srcHost net.IP, srcPort layers.TCPPort) string {
	return fmt.Sprintf("%s:%d", srcHost, srcPort)
}
