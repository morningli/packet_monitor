package main

import (
	"flag"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/morningli/packet_monitor/pkg/raw"
	"github.com/morningli/packet_monitor/pkg/redis"
	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

var (
	localHost = flag.String("h", "", "monitor listened ip")
	localPort = flag.Int("p", 8003, "monitor listened port")
	protocol  = flag.String("P", "redis", "protocol, eg:redis/raw")
	output    = flag.String("o", "default", `output target, The format is <type>:<params>.
type: default/file/single/cluster...
- default: output to stdout
- file: output to file, params is file name, eg: file:out.txt
- single: output to single redis, params is redis address, eg: single:127.0.0.1:8003
- cluster： output to redis cluster, params is cluster address, eg: cluster:127.0.0.1:8003,127.0.0.2:8003`)
	workerNum = flag.Int("worker-num", 10, "worker number")
	interf    = flag.String("i", "any", "network interface")
	buffSize  = flag.Int("B", 256<<20, "buffer size")
	logLevel  = flag.String("log-level", "info", "log level,trace/debug/info/warn/error/fatal/panic")
)

func main() {
	debug.SetGCPercent(400)

	flag.Parse()

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(lvl)

	// Open device
	inactive, err := pcap.NewInactiveHandle(*interf)
	if err != nil {
		log.Fatal(err)
	}
	defer inactive.CleanUp()

	err = inactive.SetBufferSize(*buffSize)
	if err != nil {
		log.Fatal(err)
	}
	err = inactive.SetImmediateMode(true)
	if err != nil {
		log.Fatal(err)
	}
	err = inactive.SetPromisc(false)
	if err != nil {
		log.Fatal(err)
	}
	err = inactive.SetTimeout(-1)
	if err != nil {
		log.Fatal(err)
	}
	err = inactive.SetSnapLen(256 * 1024)
	if err != nil {
		log.Fatal(err)
	}
	handle, err := inactive.Activate()
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	filter := fmt.Sprintf("tcp and host %s and port %d", *localHost, *localPort)
	err = handle.SetBPFFilter(filter)
	if err != nil {
		log.Fatal(err)
	}

	var wr common.Writer
	switch *protocol {
	case "redis":
		var outputType string
		var outputParams string
		pos := strings.Index(*output, ":")
		if pos == -1 {
			outputType = *output
		} else {
			outputType = (*output)[:pos]
			if pos != len(*output)-1 {
				outputParams = (*output)[pos+1:]
			}
		}
		switch outputType {
		case "single":
			if len(outputParams) == 0 {
				log.Fatalf("No address specified")
			}
			wr = redis.NewNetworkWriter(outputParams, false)
		case "cluster":
			if len(outputParams) == 0 {
				log.Fatalf("No address specified")
			}
			wr = redis.NewNetworkWriter(outputParams, true)
		case "default":
			wr = redis.NewFileWriter(os.Stdout)
		case "file":
			if len(outputParams) == 0 {
				log.Fatalf("No file name specified")
			}
			f, err := os.OpenFile(outputParams, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0544)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			wr = redis.NewFileWriter(f)
		}
	}

	// Use the handle as a packet source to process all packets
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()

	sessions := NewSessionMgr(net.ParseIP(*localHost), layers.TCPPort(*localPort))

	go func() {
		for {
			time.Sleep(time.Second * 300)
			log.Infof("[Stats]process:%d,miss:%d", atomic.LoadUint64(&packetsProcess), atomic.LoadUint64(&packetsMiss))
		}
	}()

	eg := errgroup.Group{}
	for i := 0; i < *workerNum; i++ {
		eg.Go(func() error {
			var monitor common.Monitor
			switch *protocol {
			case "redis":
				monitor = redis.NewMonitor(net.ParseIP(*localHost), layers.TCPPort(*localPort))
				monitor.SetWriter(wr)
			case "raw":
				monitor = &raw.Monitor{}
			default:
				log.Fatalf("no protocol found")
			}
			sessions.SetMonitor(monitor)

			for {
				select {
				case packet, ok := <-packets:
					if !ok {
						return nil
					}
					sessions.PacketArrive(packet)
				}
			}
		})
	}
	_ = eg.Wait()
}
