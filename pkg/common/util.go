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

var isWrite = map[string]struct{}{
	//Kv
	"set":         {},
	"del":         {},
	"incr":        {},
	"incrby":      {},
	"incrbyfloat": {},
	"decr":        {},
	"decrby":      {},
	"getset":      {},
	"append":      {},
	"setnx":       {},
	"setex":       {},
	"psetex":      {},
	"mset":        {},
	"msetnx":      {},
	"setrange":    {},
	"expire":      {},
	"pexpire":     {},
	"expireat":    {},
	"pexpireat":   {},
	"persist":     {},

	//Hash
	"hdel":         {},
	"hset":         {},
	"hincrby":      {},
	"hincrbyfloat": {},
	"hmset":        {},
	"hsetnx":       {},

	//List
	"linsert":   {},
	"lpop":      {},
	"lpush":     {},
	"lpushx":    {},
	"lrange":    {},
	"lrem":      {},
	"lset":      {},
	"ltrim":     {},
	"rpop":      {},
	"rpoplpush": {},
	"rpush":     {},
	"rpushx":    {},

	//BitMap
	"setbit": {},
	"bitop":  {},

	//Zset
	"zadd":             {},
	"zincrby":          {},
	"zrem":             {},
	"zunionstore":      {},
	"zinterstore":      {},
	"zremrangebyrank":  {},
	"zremrangebylex":   {},
	"zremrangebyscore": {},
	"zpopmax":          {},
	"zpopmin":          {},

	//Set
	"sadd":        {},
	"spop":        {},
	"srem":        {},
	"sunionstore": {},
	"sinterstore": {},
	"sdiffstore":  {},
	"smove":       {},
}

func IsWrite(cmd string) bool {
	_, ok := isWrite[cmd]
	return ok
}

func GetFirstKey(cmd string, args []interface{}) string {
	if len(args) >= 2 {
		return args[1].(string)
	}
	return ""
}
