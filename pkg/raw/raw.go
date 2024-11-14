package raw

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	"log"
	"strings"
)

type Monitor struct {
}

func (m *Monitor) SetWriter(writer common.Writer) {
	return
}

func (m *Monitor) Feed(packet gopacket.Packet) {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer == nil {
		return
	}
	ip, _ := ipLayer.(*layers.IPv4)

	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer == nil {
		return
	}
	tcp, _ := tcpLayer.(*layers.TCP)

	flags := []string{}
	if tcp.FIN {
		flags = append(flags, "FIN")
	}
	if tcp.SYN {
		flags = append(flags, "SYN")
	}
	if tcp.RST {
		flags = append(flags, "RST")
	}
	if tcp.PSH {
		flags = append(flags, "PSH")
	}
	if tcp.ACK {
		flags = append(flags, "ACK")
	}
	if tcp.URG {
		flags = append(flags, "URG")
	}
	if tcp.ECE {
		flags = append(flags, "ECE")
	}
	if tcp.CWR {
		flags = append(flags, "CWR")
	}
	if tcp.NS {
		flags = append(flags, "NS")
	}

	var nextSeq uint32
	if tcp.SYN {
		nextSeq = tcp.Seq + 1
	} else {
		nextSeq = tcp.Seq + uint32(len(tcp.Payload))
	}

	log.Printf("[%s:%s->%s:%s][%s][SEQ=%d:%d ACK=%d WIN=%d LEN=%d]",
		ip.SrcIP, tcp.SrcPort, ip.DstIP, tcp.DstPort, strings.Join(flags, ","), tcp.Seq, nextSeq, tcp.Ack, tcp.Window, len(tcp.Payload))
}
