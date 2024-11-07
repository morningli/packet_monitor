package main

import (
	"bytes"
	"fmt"
	"github.com/google/gopacket/pcap"
	"log"
	"net"
)

func findDevice(target net.IP) (dev pcap.Interface, ok bool) {
	// 得到所有的(网络)设备
	devices, err := pcap.FindAllDevs()
	if err != nil {
		log.Fatal(err)
	}

	// 打印设备信息
	fmt.Println("Devices found:")
	for _, device := range devices {
		fmt.Println("\nName: ", device.Name)
		fmt.Println("Description: ", device.Description)
		fmt.Println("Devices addresses: ", device.Description)
		for _, address := range device.Addresses {
			fmt.Println("- IP address: ", address.IP)
			fmt.Println("- Subnet mask: ", address.Netmask)
			if bytes.Compare(address.IP, target) == 0 {
				dev = device
				ok = true
			}
		}
	}
	return
}
