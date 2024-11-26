package reorder

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var sessionNum uint64

type Monitor struct {
	localHost net.IP
	localPort layers.TCPPort
	sessions  sync.Map //Session
	wr        common.Writer
	onlyIn    bool
}

const SessionTimeout = time.Minute * 30

func NewMonitor(localHost net.IP, localPort layers.TCPPort, onlyIn bool) *Monitor {
	m := &Monitor{localHost: localHost, localPort: localPort, onlyIn: onlyIn}
	go func() {
		for {
			time.Sleep(time.Second * 300)
			log.Infof("[Stats]session:%d,process:%d,miss:%d",
				atomic.LoadUint64(&sessionNum),
				atomic.LoadUint64(&packetsProcess),
				atomic.LoadUint64(&packetsMiss))
		}
	}()
	go func() {
		tick := time.NewTicker(time.Minute * 5)
		defer tick.Stop()
		for {
			_, ok := <-tick.C
			if !ok {
				return
			}
			total := 0
			m.sessions.Range(func(key, value interface{}) bool {
				session := value.(*Session)
				if time.Since(session.lastTime) > SessionTimeout {
					m.sessions.Delete(key)
				} else {
					total++
				}
				return true
			})
			atomic.StoreUint64(&sessionNum, uint64(total))
		}
	}()
	return m
}

func (s *Monitor) SetWriter(writer common.Writer) {
	s.wr = writer
}

func (s *Monitor) processOne(packet gopacket.Packet) {
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

	if !s.onlyIn && ip.SrcIP.Equal(s.localHost) && tcp.SrcPort == s.localPort {
		if s.wr != nil {
			err := s.wr.FlowOut(ip.DstIP, tcp.DstPort, tcpLayer.LayerPayload())
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	}

	if ip.DstIP.Equal(s.localHost) && tcp.DstPort == s.localPort {
		if s.wr != nil {
			err := s.wr.FlowIn(ip.SrcIP, tcp.SrcPort, tcpLayer.LayerPayload())
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	}
}

func (s *Monitor) Feed(packet gopacket.Packet) {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer == nil {
		return
	}
	ip, _ := ipLayer.(*layers.IPv4)

	tcpLayer := packet.Layer(layers.LayerTypeTCP)
	if tcpLayer == nil {
		log.Fatalf("no tcp\n")
	}
	tcp, _ := tcpLayer.(*layers.TCP)

	var (
		key        string
		remoteHost net.IP
		remotePort layers.TCPPort
		in         bool
	)

	if ip.SrcIP.Equal(s.localHost) && tcp.SrcPort == s.localPort {
		// out
		key = fmt.Sprintf("%s:%s", ip.DstIP.String(), tcp.DstPort.String())
		if tcp.RST {
			s.sessions.Delete(key)
			return
		}
		remoteHost = ip.DstIP
		remotePort = tcp.DstPort
		in = false
	} else if ip.DstIP.Equal(s.localHost) && tcp.DstPort == s.localPort {
		// in
		key = fmt.Sprintf("%s:%s", ip.SrcIP.String(), tcp.SrcPort.String())
		if tcp.FIN {
			s.sessions.Delete(key)
			return
		}
		remoteHost = ip.SrcIP
		remotePort = tcp.SrcPort
		in = true
	} else {
		return
	}

	if s.onlyIn && !in {
		return
	}

	session := NewSession(s.localHost, s.localPort, remoteHost, remotePort)
	tmp, loaded := s.sessions.LoadOrStore(key, session)
	if loaded {
		session = tmp.(*Session)
	}

	session.AddPacket(packet)

	for {
		packet, ok := session.TryGetPacket(in)
		if !ok {
			break
		}
		s.processOne(packet)
	}
	return
}
