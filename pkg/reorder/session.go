package reorder

import (
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	packetsMiss    uint64
	packetsProcess uint64
)

type Session struct {
	remoteHost net.IP
	remotePort layers.TCPPort
	localHost  net.IP
	localPort  layers.TCPPort
	nextSeqIn  uint32
	packetsIn  *rbt.Tree
	nextSeqOut uint32
	packetsOut *rbt.Tree
	mux        sync.Mutex
	lastTime   time.Time
}

func NewSession(localHost net.IP, localPort layers.TCPPort, remoteHost net.IP, remotePort layers.TCPPort) *Session {
	return &Session{
		localHost:  localHost,
		localPort:  localPort,
		remoteHost: remoteHost,
		remotePort: remotePort,
		packetsIn:  rbt.NewWith(utils.UInt32Comparator),
		packetsOut: rbt.NewWith(utils.UInt32Comparator)}
}

func (s *Session) AddPacket(packet gopacket.Packet) {

	s.lastTime = time.Now()

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
		if s.nextSeqIn > tcp.Seq {
			// expired packet
			return
		}
		if len(tcp.Payload) == 0 {
			return
		}
		if _, ok := s.packetsIn.Get(tcp.Seq); !ok {
			s.packetsIn.Put(tcp.Seq, packet)
		}
	}

	// out
	if ip.SrcIP.Equal(s.localHost) && tcp.SrcPort == s.localPort {
		if s.nextSeqOut > tcp.Seq {
			// expired packet
			return
		}
		if len(tcp.Payload) == 0 {
			return
		}
		if _, ok := s.packetsOut.Get(tcp.Seq); !ok {
			s.packetsOut.Put(tcp.Seq, packet)
		}
	}
}

func (s *Session) TryGetPacket(in bool) (packet gopacket.Packet, ok bool) {
	var (
		packets = s.packetsIn
		nextSeq = &s.nextSeqIn
	)
	if !in {
		packets = s.packetsOut
		nextSeq = &s.nextSeqOut
	}

	if packets.Empty() {
		ok = false
		return
	}
	if *nextSeq == 0 || packets.Left().Key == *nextSeq || packets.Size() > 200 {
		if *nextSeq > 0 && *nextSeq != packets.Left().Key {
			log.Debugf("%s:%s->%s:%s expect %d but %d",
				s.remoteHost.String(), s.remotePort.String(), s.localHost.String(), s.localPort.String(), *nextSeq, packets.Left().Key)
			atomic.AddUint64(&packetsMiss, 1)
		}

		packet = packets.Left().Value.(gopacket.Packet)
		tcpLayer := packet.Layer(layers.LayerTypeTCP)
		if tcpLayer == nil {
			return
		}
		tcp, _ := tcpLayer.(*layers.TCP)

		if tcp.SYN {
			*nextSeq = tcp.Seq + 1
			packets.Remove(tcp.Seq)
			return
		}

		*nextSeq = tcp.Seq + uint32(len(tcp.Payload))
		packets.Remove(tcp.Seq)
		ok = true
		atomic.AddUint64(&packetsProcess, 1)
	}
	return
}
