package main

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	"log"
	"net"
	"sync"
)

type SessionMgr struct {
	localHost net.IP
	localPort layers.TCPPort
	sessions  sync.Map //Session
	monitor   common.Monitor
}

func NewSessionMgr(localHost net.IP, localPort layers.TCPPort) *SessionMgr {
	return &SessionMgr{localHost: localHost, localPort: localPort}
}

func (s *SessionMgr) SetMonitor(monitor common.Monitor) {
	s.monitor = monitor
}

func (s *SessionMgr) PacketArrive(packet gopacket.Packet) {
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
	)

	if ip.SrcIP.Equal(s.localHost) && tcp.SrcPort == s.localPort {
		// out
		key = fmt.Sprintf("%s:%s", ip.DstIP.String(), tcp.DstPort.String())
		if tcp.RST {
			s.sessions.Delete(key)
		}
		return
	} else if ip.DstIP.Equal(s.localHost) && tcp.DstPort == s.localPort {
		// in
		key = fmt.Sprintf("%s:%s", ip.SrcIP.String(), tcp.SrcPort.String())
		if tcp.FIN {
			s.sessions.Delete(key)
			return
		}
		remoteHost = ip.SrcIP
		remotePort = tcp.SrcPort
	} else {
		return
	}

	session := NewSession(s.localHost, s.localPort, remoteHost, remotePort)
	tmp, loaded := s.sessions.LoadOrStore(key, session)
	if loaded {
		session = tmp.(*Session)
	}

	session.mux.Lock()
	defer session.mux.Unlock()

	session.AddPacket(packet)

	for {
		packet, ok := session.TryGetPacket()
		if !ok {
			break
		}
		if s.monitor != nil {
			s.monitor.Feed(packet)
		}
	}
	return
}
