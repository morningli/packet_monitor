package redis

import (
	"github.com/morningli/packet_monitor/pkg/common"
	log "github.com/sirupsen/logrus"
	"io"
	"sync/atomic"
)

type stat int

const (
	stateType = iota
	stateBulkSize
	stateBulkLenPre
	stateBulkLen
	stateBulkData
	stateSimpleString
)

type NoCopyBuffer struct {
	buf []byte
	off int // read at &buf[off], write at &buf[len(buf)]
}

func (b *NoCopyBuffer) Write(p []byte) (n int, err error) {
	if !b.empty() {
		log.Fatalf("write to not empty buffer, buf len:%d, off:%d, left:%s", len(b.buf), b.off, common.BytesToString(b.buf[b.off:]))
	}
	b.buf = p
	b.off = 0
	return len(p), nil
}

func (b *NoCopyBuffer) empty() bool { return len(b.buf) <= b.off }

func (b *NoCopyBuffer) ReadByte() (byte, error) {
	if b.empty() {
		return 0, io.EOF
	}
	c := b.buf[b.off]
	b.off++
	return c, nil
}

func (b *NoCopyBuffer) Read(p []byte) (n int, err error) {
	if b.empty() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return n, nil
}

var id int32

type Decoder struct {
	id    int32
	state stat
	data  NoCopyBuffer

	size  int
	len   int
	token []byte
	ret   []interface{}
	t     byte
}

func NewDecoder() *Decoder {
	return &Decoder{id: atomic.AddInt32(&id, 1)}
}

func (b *Decoder) Append(data []byte) {
	_, err := b.data.Write(data)
	if err != nil {
		log.Fatalf("[%d]feed data fail:%s", b.id, err)
	}
	log.Debugf("[%d]append %s", b.id, common.BytesToString(data))
}

func (b *Decoder) readLine(line []byte) (n int, err error) {
	for i := 0; i < len(line); i++ {
		d, err := b.data.ReadByte()
		if err != nil {
			return i, err
		}
		line[i] = d
		if d == '\n' {
			return i + 1, nil
		}
	}
	return len(line), io.ErrShortBuffer
}

func (b *Decoder) TryDecode() (ret interface{}) {
	bytesInt := make([]byte, 13)
	bytesString := make([]byte, 128)
	for {
		switch b.state {
		case stateType:
			t, err := b.data.ReadByte()
			if err == io.EOF {
				return nil
			}
			b.t = t
			switch t {
			case '*':
				b.state = stateBulkSize
				b.size = 0
				b.len = 0
				b.token = bytesInt
				b.ret = make([]interface{}, 0, 4)
			case '+':
				b.state = stateSimpleString
				b.token = nil
			case '-':
				b.state = stateSimpleString
				b.token = nil
			case ':':
				b.state = stateSimpleString
				b.token = nil
			case '$':
				b.state = stateBulkLen
				b.token = bytesInt
				b.len = 0
			}
		case stateSimpleString:
			n, err := b.readLine(bytesString)
			b.token = append(b.token, bytesString[:n]...)
			if err == io.ErrShortBuffer {
				break
			}
			if err != nil {
				return nil
			}
			if len(b.token) < 2 || b.token[len(b.token)-2] != '\r' || b.token[len(b.token)-1] != '\n' {
				log.Errorf("[%d]parse simple string fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}
			ret = b.token[:len(b.token)-2]
			b.state = stateType
			b.token = nil
			return
		case stateBulkSize:
			n, err := b.readLine(b.token[b.len:])
			b.len += n
			if err == io.ErrShortBuffer {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}
			if err != nil {
				return nil
			}

			size, err := common.Btoi(b.token[:b.len-2])
			if err != nil {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}
			b.size = size
			b.state = stateBulkLenPre
		case stateBulkLenPre:
			t, err := b.data.ReadByte()
			if err != nil {
				return nil
			}
			if t != '$' {
				log.Errorf("[%d]parse bulk len pre fail:%s", b.id, string(t))
				b.state = stateType
				break
			}
			b.state = stateBulkLen
			b.token = bytesInt
			b.len = 0
		case stateBulkLen:
			n, err := b.readLine(b.token[b.len:])
			b.len += n
			if err == io.ErrShortBuffer {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}
			if err != nil {
				return nil
			}

			if len(b.token) < 2 || b.token[len(b.token)-2] != '\r' || b.token[len(b.token)-1] != '\n' {
				log.Errorf("[%d]parse bulk len fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}

			size, err := common.Btoi(b.token[:b.len-2])
			if err != nil {
				log.Errorf("[%d]parse bulk len fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}
			b.len = size + 2
			b.token = make([]byte, size+2)
			b.state = stateBulkData
		case stateBulkData:
			n, err := b.data.Read(b.token[len(b.token)-b.len:])
			b.len -= n
			if err != nil {
				return nil
			}

			if b.len != 0 {
				break
			}

			if len(b.token) < 2 || b.token[len(b.token)-2] != '\r' || b.token[len(b.token)-1] != '\n' {
				log.Errorf("[%d]parse bulk data fail:%s", b.id, common.BytesToString(b.token))
				b.state = stateType
				break
			}

			if b.t == '$' {
				b.state = stateType
				ret = b.token[:len(b.token)-2]
				b.token = nil
				return
			}

			b.size--
			b.ret = append(b.ret, common.BytesToString(b.token[:len(b.token)-2])) //must string
			b.token = nil

			if b.size != 0 {
				b.state = stateBulkLenPre
			} else {
				b.state = stateType
				ret = b.ret
				b.ret = nil
				return
			}
		}
	}
}
