package main

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	packetsMiss uint64
)

type Session struct {
	localHost   net.IP
	localPort   layers.TCPPort
	nextSeq     uint32
	packets     *rbt.Tree
	mux         sync.Mutex
	lastSuccess time.Time
}

func NewSession(localHost net.IP, localPort layers.TCPPort) *Session {
	return &Session{localHost: localHost, localPort: localPort, packets: rbt.NewWith(utils.UInt32Comparator)}
}

func (s *Session) AddPacket(packet gopacket.Packet) {
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

	// in
	if ip.DstIP.Equal(s.localHost) && tcp.DstPort == s.localPort {
		if s.nextSeq > tcp.Seq {
			// expired packet
			return
		}
		if len(tcp.Payload) == 0 {
			return
		}
	}

	// out
	if ip.SrcIP.Equal(s.localHost) && tcp.SrcPort == s.localPort {
		// TODO
		return
	}

	if _, ok := s.packets.Get(tcp.Seq); !ok {
		s.packets.Put(tcp.Seq, packet)
	}
}

func (s *Session) TryGetPacket() (packet gopacket.Packet, ok bool) {
	if s.packets.Empty() {
		ok = false
		return
	}
	if s.nextSeq == 0 || s.packets.Left().Key == s.nextSeq || s.packets.Size() > 20 {
		if s.nextSeq > 0 && s.nextSeq != s.packets.Left().Key {
			atomic.AddUint64(&packetsMiss, 1)
		}

		packet = s.packets.Left().Value.(gopacket.Packet)
		tcpLayer := packet.Layer(layers.LayerTypeTCP)
		if tcpLayer == nil {
			return
		}
		tcp, _ := tcpLayer.(*layers.TCP)

		if tcp.SYN {
			s.nextSeq = tcp.Seq + 1
			s.packets.Remove(tcp.Seq)
			return
		}

		s.nextSeq = tcp.Seq + uint32(len(tcp.Payload))
		s.packets.Remove(tcp.Seq)
		ok = true
		//s.lastSuccess = time.Now()
	}
	return
}
