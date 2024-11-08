package main

import (
	"flag"
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/morningli/packet_monitor/pkg/redis"
	"golang.org/x/sync/errgroup"
	"log"
	"net"
	"os"
	"reflect"
	"strings"
	"time"
)

var (
	localHost = flag.String("h", "", "monitor listened ip")
	localPort = flag.Int("p", 8003, "monitor listened port")
	protocol  = flag.String("P", "redis", "protocol, eg:redis")
	output    = flag.String("o", "default", `output target, The format is <type>:<params>.
type: default/file/single/cluster...
- default: output to stdout
- file: output to file, params is file name, eg: file:out.txt
- single: output to single redis, params is redis address, eg: single:127.0.0.1:8003
- cluster： output to redis cluster, params is cluster address, eg: cluster:127.0.0.1:8003,127.0.0.2:8003`)
)

func main() {
	flag.Parse()

	//tcp and host 10.177.26.250 and port 8003
	filter := fmt.Sprintf("tcp and host %s and port %d", *localHost, *localPort)

	// 得到所有的(网络)设备
	devices, err := pcap.FindAllDevs()
	if err != nil {
		log.Fatal(err)
	}

	var cases []reflect.SelectCase

	for _, device := range devices {
		// Open device
		handle, err := pcap.OpenLive(device.Name, 1500, false, -1*time.Second)
		if err != nil {
			log.Fatal(err)
		}
		err = handle.SetBPFFilter(filter)
		if err != nil {
			log.Fatal(err)
		}
		defer handle.Close()

		// Use the handle as a packet source to process all packets
		packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
		packets := packetSource.Packets()
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(packets)})
	}

	tmp := strings.Split(*output, ":")
	outputType := tmp[0]
	var wr common.Writer
	switch *protocol {
	case "redis":
		switch outputType {
		case "single":
			if len(tmp) != 2 {
				log.Fatalf("No address specified")
			}
			wr = redis.NewNetworkWriter(tmp[1], false)
		case "cluster":
			if len(tmp) != 2 {
				log.Fatalf("No address specified")
			}
			wr = redis.NewNetworkWriter(tmp[1], true)
		case "default":
			wr = redis.NewFileWriter(os.Stdout)
		case "file":
			if len(tmp) != 2 {
				log.Fatalf("No file name specified")
			}
			f, err := os.OpenFile(tmp[1], os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0544)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			wr = redis.NewFileWriter(f)
		}
	}

	eg := errgroup.Group{}
	const threads = 100 // for 20w/s
	for i := 0; i < threads; i++ {
		eg.Go(func() error {
			var monitor common.Monitor = &common.DefaultMonitor{}
			switch *protocol {
			case "redis":
				monitor = redis.NewMonitor(net.ParseIP(*localHost), layers.TCPPort(*localPort))
				monitor.SetWriter(wr)
			}
			for {
				_, packet, ok := reflect.Select(cases)
				if !ok {
					return nil
				}
				monitor.Feed(packet.Interface().(gopacket.Packet))
			}
		})
	}
	_ = eg.Wait()
}
