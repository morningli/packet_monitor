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
	"github.com/morningli/packet_monitor/pkg/reorder"
	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
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
	- cluster： output to redis cluster, params is cluster address, eg: cluster:127.0.0.1:8003,127.0.0.2:8003
	- count: count key frequency every 5 minute, ignore commands occur only 1 time `)
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
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

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

	filter := fmt.Sprintf("tcp and dst host %s and dst port %d", *localHost, *localPort)
	err = handle.SetBPFFilter(filter)
	if err != nil {
		log.Fatal(err)
	}
	err = handle.SetDirection(pcap.DirectionIn)
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
		case "count":
			threshold := 1
			if len(outputParams) > 0 {
				threshold, _ = strconv.Atoi(outputParams)
			}
			wr = redis.NewCountWriter(threshold)
		}
	}

	// Use the handle as a packet source to process all packets
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()

	var monitor common.Monitor
	switch *protocol {
	case "redis":
		monitor = reorder.NewMonitor(net.ParseIP(*localHost), layers.TCPPort(*localPort))
		monitor.SetWriter(wr)
	case "raw":
		monitor = &raw.Monitor{}
	default:
		log.Fatalf("no protocol found")
	}

	eg := errgroup.Group{}
	for i := 0; i < *workerNum; i++ {
		eg.Go(func() error {
			for {
				select {
				case packet, ok := <-packets:
					if !ok {
						return nil
					}
					monitor.Feed(packet)
				}
			}
		})
	}
	_ = eg.Wait()
}
