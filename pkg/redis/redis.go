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
	"golang.org/x/sync/errgroup"
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
	dstHost net.IP
	dstPort layers.TCPPort

	sessions sync.Map

	clusterMode bool
	client      redis.UniversalClient
}

func NewNetworkWriter(dstHost net.IP, dstPort layers.TCPPort) *NetworkWriter {
	return &NetworkWriter{}
}

func (w *NetworkWriter) Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	if w.client == nil {
		addr := fmt.Sprintf("%s:%d", w.dstHost.String(), w.dstPort)

		var s redis.UniversalClient
		s = redis.NewClient(&redis.Options{
			Addr: addr,
		})
		err := s.ClusterSlots(context.Background()).Err()
		if err == nil {
			_ = s.Close()
			s = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{addr},
			})
			w.clusterMode = true
		}
		w.client = s
	}

	var s *bytes.Buffer
	if _s, ok := w.sessions.Load(common.RemoteKey(srcHost, srcPort)); !ok {
		s = &bytes.Buffer{}
		_s, loaded := w.sessions.LoadOrStore(common.RemoteKey(srcHost, srcPort), s)
		if loaded {
			s = _s.(*bytes.Buffer)
		}
	} else {
		s = _s.(*bytes.Buffer)
	}

	_, err := s.Write(data)
	if err != nil {
		log.Fatal(err)
	}

	eg := errgroup.Group{}

	rd := resp.NewReader(s)
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if v.Type() == resp.Array {
			var args []interface{}
			for _, v := range v.Array() {
				if v.Type() == resp.BulkString {
					args = append(args, v.Bytes())
				}
			}
			eg.Go(func() error {
				return w.client.Do(context.Background(), args...).Err()
			})
		}
	}
	err = eg.Wait()
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

type FileWriter struct {
	f *os.File

	sessions sync.Map
}

func NewFileWriter(f *os.File) *FileWriter {
	return &FileWriter{f: f}
}

func (w *FileWriter) Write(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	var s *bytes.Buffer
	if _s, ok := w.sessions.Load(common.RemoteKey(srcHost, srcPort)); !ok {
		s = &bytes.Buffer{}
		_s, loaded := w.sessions.LoadOrStore(common.RemoteKey(srcHost, srcPort), s)
		if loaded {
			s = _s.(*bytes.Buffer)
		}
	} else {
		s = _s.(*bytes.Buffer)
	}

	_, err := s.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	rd := resp.NewReader(s)
	for {
		v, _, err := rd.ReadValue()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		_, err = fmt.Fprintf(w.f, "%v %s->%s\n", time.Now(), srcHost.String(), srcPort.String())
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
