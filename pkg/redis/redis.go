package redis

import (
	"context"
	"fmt"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	runningWrite int64
	success      uint64
	fail         uint64
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
	w := &NetworkWriter{address: address, cluster: cluster, sessions: NewSessionMgr()}
	if cluster {
		w.client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: strings.Split(w.address, ","),
		})
	} else {
		w.client = redis.NewClient(&redis.Options{
			Addr: w.address,
		})
	}
	go func() {
		for {
			time.Sleep(time.Second * 300)
			log.Infof("[Stats]running write:%d,success request:%d,fail:%d",
				atomic.LoadInt64(&runningWrite),
				atomic.LoadUint64(&success),
				atomic.LoadUint64(&fail))
		}
	}()
	return w
}

func (w *NetworkWriter) FlowIn(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	requests := w.sessions.AppendAndFetch(common.RemoteKey(srcHost, srcPort), data, true)
	if len(requests) == 0 {
		return nil
	}

	go func() {
		atomic.AddInt64(&runningWrite, 1)
		for _, args := range requests {
			err := w.client.Do(context.Background(), args.([]interface{})...).Err()
			if err != nil && err != redis.Nil {
				log.Errorf("execute command fail.args:%+v,err:%s", args, err)
				atomic.AddUint64(&fail, 1)
			} else {
				atomic.AddUint64(&success, 1)
			}
		}
		atomic.AddInt64(&runningWrite, -1)
	}()

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
	requests := w.sessions.AppendAndFetch(common.RemoteKey(srcHost, srcPort), data, true)
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

		for _, v := range args.([]interface{}) {
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

type CountWriter struct {
	sessions *SessionMgr
	wCounts  [2]sync.Map
	rCounts  [2]sync.Map
	min      int64
	pos      int64
	mtime    int64
}

func (w *CountWriter) FlowOut(dstHost net.IP, dstPort layers.TCPPort, data []byte) error {
	// ignore
	return nil
}

func NewCountWriter(minCount int) *CountWriter {
	return &CountWriter{min: int64(minCount), mtime: time.Now().Unix(), sessions: NewSessionMgr()}
}

func (w *CountWriter) FlowIn(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	requests := w.sessions.AppendAndFetch(common.RemoteKey(srcHost, srcPort), data, true)
	if len(requests) == 0 {
		return nil
	}

	const statTime = 1000000

	for _, r := range requests {
		args := r.([]interface{})
		if len(args) != 2 {
			continue
		}
		cmd := strings.ToLower(args[0].(string))
		key := common.GetFirstKey(cmd, args)
		write := common.IsWrite(cmd)

		if len(key) == 0 {
			continue
		}

		var c *sync.Map
		if write {
			c = &w.wCounts[atomic.LoadInt64(&w.pos)]
		} else {
			c = &w.rCounts[atomic.LoadInt64(&w.pos)]
		}
		var count int64 = 1
		ic, ok := c.LoadOrStore(key, &count)
		if ok {
			cc := ic.(*int64)
			atomic.AddInt64(cc, 1)
		}
	}

	oldTime := atomic.LoadInt64(&w.mtime)
	if time.Now().UnixMicro()-oldTime < statTime {
		return nil
	}

	ok := atomic.CompareAndSwapInt64(&w.mtime, oldTime, time.Now().UnixMicro())
	if !ok {
		return nil
	}

	p := atomic.LoadInt64(&w.pos)
	np := (p + 1) & 0x1
	ok = atomic.CompareAndSwapInt64(&w.pos, p, np)
	if !ok {
		return nil
	}

	w.rCounts[p].Range(func(key, value interface{}) bool {
		c := value.(*int64)
		if *c >= w.min {
			fmt.Printf("[%d]read key:%s, freq:%d\n", oldTime, key, *c)
		}
		w.rCounts[p].Delete(key)
		return ok
	})

	w.wCounts[p].Range(func(key, value interface{}) bool {
		c := value.(*int64)
		if *c >= w.min {
			fmt.Printf("[%d]write key:%s, freq:%d\n", oldTime, key, *c)
		}
		w.wCounts[p].Delete(key)
		return ok
	})

	return nil
}

type HistogramWriter struct {
	sessions  *SessionMgr
	mux       sync.RWMutex
	histogram *hdrhistogram.WindowedHistogram
	mtime     int64
	target    string
}

func (w *HistogramWriter) FlowOut(dstHost net.IP, dstPort layers.TCPPort, data []byte) error {
	const statTime = 300000000

	if w.target != "rsp.size" {
		return nil
	}

	requests := w.sessions.AppendAndFetch(common.RemoteKey(dstHost, dstPort), data, false)
	if len(requests) == 0 {
		return nil
	}

	w.mux.RLock()
	for _, r := range requests {
		total := 0
		switch rsp := r.(type) {
		case []interface{}:
			for _, a := range rsp {
				total += len(a.(string))
			}
		case []byte:
			total = len(rsp)
		}

		err := w.histogram.Current.RecordValue(int64(total))
		if err != nil {
			log.Errorf("stat req size fail, err:%s", err.Error())
		}
	}
	w.mux.RUnlock()

	oldTime := atomic.LoadInt64(&w.mtime)
	if time.Now().Unix()-oldTime < statTime {
		return nil
	}

	ok := atomic.CompareAndSwapInt64(&w.mtime, oldTime, time.Now().Unix())
	if !ok {
		return nil
	}

	w.mux.RLock()
	results := w.histogram.Merge().ValueAtPercentiles([]float64{90, 95, 99})
	w.mux.RUnlock()

	w.mux.Lock()
	w.histogram.Rotate()
	w.mux.Unlock()

	for p, r := range results {
		fmt.Printf("[%d]rsp.size %f%%:%d\n", oldTime, p, r)
	}
	return nil
}

const bucketNum = 10

func NewHistogramWriter(minValue, maxValue int64, target string) *HistogramWriter {
	return &HistogramWriter{target: target, histogram: hdrhistogram.NewWindowed(bucketNum, minValue, maxValue, 2), sessions: NewSessionMgr()}
}

func (w *HistogramWriter) FlowIn(srcHost net.IP, srcPort layers.TCPPort, data []byte) error {
	const statTime = 300000000

	if w.target != "req.size" {
		return nil
	}

	requests := w.sessions.AppendAndFetch(common.RemoteKey(srcHost, srcPort), data, true)
	if len(requests) == 0 {
		return nil
	}

	w.mux.RLock()
	for _, r := range requests {
		total := 0
		for _, a := range r.([]interface{}) {
			total += len(a.(string))
		}
		err := w.histogram.Current.RecordValue(int64(total))
		if err != nil {
			log.Errorf("stat req size fail, err:%s", err.Error())
		}
	}
	w.mux.RUnlock()

	oldTime := atomic.LoadInt64(&w.mtime)
	if time.Now().Unix()-oldTime < statTime {
		return nil
	}

	ok := atomic.CompareAndSwapInt64(&w.mtime, oldTime, time.Now().Unix())
	if !ok {
		return nil
	}

	w.mux.RLock()
	results := w.histogram.Merge().ValueAtPercentiles([]float64{90, 95, 99})
	w.mux.RUnlock()

	w.mux.Lock()
	w.histogram.Rotate()
	w.mux.Unlock()

	for p, r := range results {
		fmt.Printf("[%d]req.size %f%%:%d\n", oldTime, p, r)
	}
	return nil
}
