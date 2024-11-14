package redis

import (
	"context"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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

	if ip.SrcIP.Equal(r.localHost) && tcp.SrcPort == r.localPort {
		// TODO packet out，ignore
		return
	}

	if ip.DstIP.Equal(r.localHost) && tcp.DstPort == r.localPort {
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
	address  string
	cluster  bool
	sessions sync.Map
	client   redis.UniversalClient
}

func NewNetworkWriter(address string, cluster bool) *NetworkWriter {
	return &NetworkWriter{address: address, cluster: cluster}
}

func (w *NetworkWriter) Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	if w.client == nil {
		if w.cluster {
			w.client = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: strings.Split(w.address, ","),
			})
		} else {
			w.client = redis.NewClient(&redis.Options{
				Addr: w.address,
			})
		}
	}

	var s *RespBuffer
	if _s, ok := w.sessions.Load(common.RemoteKey(srcHost, srcPort)); !ok {
		s = &RespBuffer{}
		_s, loaded := w.sessions.LoadOrStore(common.RemoteKey(srcHost, srcPort), s)
		if loaded {
			s = _s.(*RespBuffer)
		}
	} else {
		s = _s.(*RespBuffer)
	}
	s.Feed(data)

	eg := errgroup.Group{}
	for {
		args := s.TryFetch()
		if args == nil {
			break
		}

		eg.Go(func() error {
			err := w.client.Do(context.Background(), args...).Err()
			if err != nil {
				log.Printf("excute command fail.args:%+v,err:%s\n", args, err)
			}
			return nil
		})
	}
	_ = eg.Wait()
	return nil
}

type FileWriter struct {
	f        *os.File
	sessions sync.Map
}

func NewFileWriter(f *os.File) *FileWriter {
	return &FileWriter{f: f}
}

func (w *FileWriter) Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	var s *RespBuffer
	if _s, ok := w.sessions.Load(common.RemoteKey(srcHost, srcPort)); !ok {
		s = &RespBuffer{}
		_s, loaded := w.sessions.LoadOrStore(common.RemoteKey(srcHost, srcPort), s)
		if loaded {
			s = _s.(*RespBuffer)
		}
	} else {
		s = _s.(*RespBuffer)
	}
	s.Feed(data)

	for {
		args := s.TryFetch()
		if args == nil {
			break
		}

		buff := strings.Builder{}
		buff.Write(strconv.AppendFloat(nil, float64(time.Now().UnixMicro())/1e6, 'f', 6, 64))
		buff.WriteString(" [0 ")
		buff.WriteString(srcHost.String())
		buff.WriteString(":")
		buff.WriteString(srcPort.String())
		buff.WriteString("]")

		for _, v := range args {
			buff.WriteString(" \"")
			buff.Write(v.([]byte))
			buff.WriteString("\"")
		}

		_, err := fmt.Fprintln(w.f, buff.String())
		if err != nil {
			log.Fatal(err)
		}
	}
	return nil
}
