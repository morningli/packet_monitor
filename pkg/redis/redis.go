package redis

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/resp"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type Monitor struct {
	localHost net.IP
	localPort layers.TCPPort
	wr        common.Writer
}

func NewMonitor(localHost net.IP, localPort layers.TCPPort) *Monitor {
	return &Monitor{localHost: localHost, localPort: localPort}
}

func (r *Monitor) SetWriter(writer common.Writer) {
	r.wr = writer
}

func (r *Monitor) Feed(packet gopacket.Packet) {
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

	if !tcp.PSH {
		return
	}

	if bytes.Compare(ip.SrcIP, r.localHost) == 0 && tcp.SrcPort == r.localPort {
		// TODO packet out，ignore
		return
	}

	if bytes.Compare(ip.DstIP, r.localHost) == 0 && tcp.DstPort == r.localPort {
		if r.wr != nil {
			err := r.wr.Write(ip.SrcIP, tcp.SrcPort, tcpLayer.LayerPayload())
			if err != nil {
				log.Fatal(err)
			}
			return
		}
	}
}

type NetworkWriter struct {
	dstHost     net.IP
	dstPort     layers.TCPPort
	clusterMode bool
	sessions    sync.Map
}

func NewNetworkWriter(dstHost net.IP, dstPort layers.TCPPort) *NetworkWriter {
	return &NetworkWriter{}
}

func (w *NetworkWriter) Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	var s redis.UniversalClient

	if _s, ok := w.sessions.Load([2]interface{}{srcHost, srcPort}); !ok {
		if w.clusterMode {
			s = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{},
			})
		} else {
			s = redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
			})
			err := s.ClusterSlots(context.Background()).Err()
			if err == nil {
				_ = s.Close()
				s = redis.NewClusterClient(&redis.ClusterOptions{
					Addrs: []string{},
				})
				w.clusterMode = true
			}
		}
		_s, loaded := w.sessions.LoadOrStore([2]interface{}{srcHost, srcPort}, s)
		if loaded {
			s = _s.(redis.UniversalClient)
		}
	} else {
		s = _s.(redis.UniversalClient)
	}
	return nil
}

type FileWriter struct {
	f *os.File
}

func NewFileWriter(f *os.File) *FileWriter {
	return &FileWriter{f: f}
}

func (w *FileWriter) Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	_, err := fmt.Fprintf(w.f, "%v %s->%s\n", time.Now(), srcHost.String(), srcPort.String())
	if err != nil {
		log.Fatal(err)
	}
	rd := resp.NewReader(bytes.NewBuffer(data))
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		_, err = fmt.Fprintf(w.f, "Read %s\n", v.Type())
		if err != nil {
			log.Fatal(err)
		}
		if v.Type() == resp.Array {
			for i, v := range v.Array() {
				_, err = fmt.Fprintf(w.f, "  #%d %s, value: '%s'\n", i, v.Type(), v)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	return nil
}
