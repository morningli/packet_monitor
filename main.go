package packet_monitor

import (
	"flag"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/morningli/packet_monitor/pkg/common"
	"github.com/morningli/packet_monitor/pkg/redis"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	device             = flag.String("i", "eth0", "network interface")
	filter             = flag.String("F", "tcp and port 6379", "network filter, BPF format")
	protocol           = flag.String("p", "redis", "protocol, eg:redis")
	outFile            = flag.String("file", "", "save to file")
	replayTarget       = flag.String("replay-target", "", "replay target")
	snapshotLen  int32 = 1500
	promiscuous  bool  = false
	err          error
	timeout      time.Duration = -1
	handle       *pcap.Handle
)

func main() {
	// Open device
	handle, err = pcap.OpenLive(*device, snapshotLen, promiscuous, timeout)
	if err != nil {
		log.Fatal(err)
	}
	err = handle.SetBPFFilter(*filter)
	if err != nil {
		log.Fatal(err)
	}
	defer handle.Close()

	var monitor common.Monitor = &common.DefaultMonitor{}
	switch *protocol {
	case "redis":
		monitor = &redis.Monitor{}
		if len(*replayTarget) > 0 {
			tmp := strings.Split(*replayTarget, ":")
			if len(tmp) != 2 {
				log.Fatal("replay target error")
			}
			port, err := strconv.Atoi(tmp[1])
			if err != nil {
				log.Fatal(err)
			}
			monitor.SetWriter(redis.NewNetworkWriter(net.ParseIP(tmp[0]), layers.TCPPort(port)))
		} else {
			var f *os.File
			if len(*outFile) == 0 {
				f = os.Stdout
			} else {
				f, err = os.OpenFile(*outFile, os.O_TRUNC|os.O_WRONLY, 0544)
				if err != nil {
					log.Fatal(err)
				}
			}
			monitor.SetWriter(redis.NewFileWriter(f))
		}
	}

	// Use the handle as a packet source to process all packets
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		monitor.Feed(packet)
	}
}
