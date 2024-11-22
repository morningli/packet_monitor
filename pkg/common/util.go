package common

import (
	"fmt"
	"github.com/google/gopacket/layers"
	"net"
	"reflect"
	"strconv"
	"unsafe"
)

func RemoteKey(srcHost net.IP, srcPort layers.TCPPort) string {
	return fmt.Sprintf("%s:%d", srcHost, srcPort)
}

func StringsToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh)) //nolint: govet
}

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func Btoi(b []byte) (int, error) {
	if len(b) == 1 {
		return int(b[0] - '0'), nil
	}
	return strconv.Atoi(BytesToString(b))
}

func IsWrite(cmd string) bool {
	return false
}

func GetFirstKey(args []interface{}) string {
	return ""
}
