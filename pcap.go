package main

import (
	"github.com/google/gopacket"
	"io"
	"log"
	"net"
	"strings"
	"syscall"
)

func packetsToChannel(p *gopacket.PacketSource, c chan gopacket.Packet) {
	defer close(c)
	for {
		packet, err := p.NextPacket()
		if err == nil {
			c <- packet
			continue
		}

		// Immediately retry for temporary network errors
		if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
			continue
		}

		// Immediately retry for EAGAIN
		if err == syscall.EAGAIN {
			continue
		}

		// Immediately break for known unrecoverable errors
		if err == io.EOF || err == io.ErrUnexpectedEOF ||
			err == io.ErrNoProgress || err == io.ErrClosedPipe || err == io.ErrShortBuffer ||
			err == syscall.EBADF ||
			strings.Contains(err.Error(), "use of closed file") {
			log.Fatal(err)
		}
	}
}
