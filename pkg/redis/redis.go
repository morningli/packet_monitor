package redis

import (
	"context"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"strconv"
	"strings"
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
			err := r.wr.FlowIn(ip.SrcIP, tcp.SrcPort, tcpLayer.LayerPayload())
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
	client   redis.UniversalClient
	sessions *SessionMgr
}

func (w *NetworkWriter) FlowOut(dstHost net.IP, dstPort layers.TCPPort, data []byte) error {
	// ignore
	return nil
}

func NewNetworkWriter(address string, cluster bool) *NetworkWriter {
	return &NetworkWriter{address: address, cluster: cluster, sessions: NewSessionMgr()}
}

func (w *NetworkWriter) FlowIn(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
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

	requests := w.sessions.AppendAndFetch(common.RemoteKey(srcHost, srcPort), data)
	if len(requests) == 0 {
		return nil
	}

	eg := errgroup.Group{}
	for _, args := range requests {
		eg.Go(func() error {
			err := w.client.Do(context.Background(), args...).Err()
			if err != nil {
				log.Errorf("execute command fail.args:%+v,err:%s", args, err)
			}
			return nil
		})
	}
	_ = eg.Wait()
	return nil
}

type FileWriter struct {
	f        *os.File
	sessions *SessionMgr
}

func (w *FileWriter) FlowOut(dstHost net.IP, dstPort layers.TCPPort, data []byte) error {
	// ignore
	return nil
}

func NewFileWriter(f *os.File) *FileWriter {
	return &FileWriter{f: f, sessions: NewSessionMgr()}
}

func (w *FileWriter) FlowIn(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	requests := w.sessions.AppendAndFetch(common.RemoteKey(srcHost, srcPort), data)
	if len(requests) == 0 {
		return nil
	}

	for _, args := range requests {
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
