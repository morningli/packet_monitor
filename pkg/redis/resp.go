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
	stateDone
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

type Resp struct {
	t     byte
	state stat
	size  int
	len   int
	token []byte
	array []interface{}
	total int //raw data len
}

func (r *Resp) Value() interface{} {
	if r.t == '*' {
		return r.array
	}
	return r.token[:len(r.token)-2]
}

func (r *Resp) Size() int {
	return r.total
}

func (r *Resp) Valid() bool {
	return r.t != 0 && r.state == stateDone
}

type Decoder struct {
	id   int32
	in   bool
	data NoCopyBuffer

	cur   Resp
	stack *common.Stack
}

func NewDecoder(in bool) *Decoder {
	return &Decoder{id: atomic.AddInt32(&id, 1), in: in, stack: common.NewStack()}
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

func (b *Decoder) ResetCurrent() {
	b.cur = Resp{}
	b.cur.state = stateType
}

func (b *Decoder) ArrayItemDone() bool {
	v := b.stack.Pop()
	last := v.(Resp)
	last.array = append(last.array, b.cur)
	last.len--
	last.total += b.cur.total
	if last.len == 0 {
		b.cur = last
		b.cur.state = stateDone
		return true
	}
	b.stack.Push(last)
	b.ResetCurrent()
	return false
}

func (b *Decoder) TryDecodeRespond() (ret Resp) {
	bytesInt := make([]byte, 13)
	bytesString := make([]byte, 128)
	for {
		switch b.cur.state {
		case stateType:
			t, err := b.data.ReadByte()
			if err == io.EOF {
				return Resp{}
			}
			switch t {
			case '*':
				b.cur.state = stateBulkSize
				b.cur.token = bytesInt
				b.cur.array = make([]interface{}, 0, 4)
			case '+':
				b.cur.state = stateSimpleString
			case '-':
				b.cur.state = stateSimpleString
			case ':':
				b.cur.state = stateSimpleString
			case '$':
				b.cur.state = stateBulkLen
				b.cur.token = bytesInt
			default:
				break
			}
			b.cur.t = t
			b.cur.total++
		case stateSimpleString:
			n, err := b.readLine(bytesString)
			b.cur.token = append(b.cur.token, bytesString[:n]...)
			b.cur.total += n
			if err == io.ErrShortBuffer {
				break
			}
			if err != nil {
				return Resp{}
			}
			if len(b.cur.token) < 2 || b.cur.token[len(b.cur.token)-2] != '\r' || b.cur.token[len(b.cur.token)-1] != '\n' {
				log.Errorf("[%d]parse simple string fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			ret.state = stateDone
		case stateBulkSize:
			n, err := b.readLine(b.cur.token[b.cur.len:])
			b.cur.len += n
			b.cur.total += n
			if err == io.ErrShortBuffer {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			if err != nil {
				return Resp{}
			}

			size, err := common.Btoi(b.cur.token[:b.cur.len-2])
			if err != nil {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			b.cur.size = size
			b.stack.Push(b.cur)
			b.ResetCurrent()
		case stateBulkLen:
			n, err := b.readLine(b.cur.token[b.cur.len:])
			b.cur.len += n
			b.cur.total += n
			if err == io.ErrShortBuffer {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			if err != nil {
				return Resp{}
			}

			if len(b.cur.token) < 2 || b.cur.token[len(b.cur.token)-2] != '\r' || b.cur.token[len(b.cur.token)-1] != '\n' {
				log.Errorf("[%d]parse bulk len fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}

			size, err := common.Btoi(b.cur.token[:b.cur.len-2])
			if err != nil {
				log.Errorf("[%d]parse bulk len fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			b.cur.len = size + 2
			b.cur.token = make([]byte, size+2)
			b.cur.state = stateBulkData
		case stateBulkData:
			n, err := b.data.Read(b.cur.token[len(b.cur.token)-b.cur.len:])
			b.cur.len -= n
			b.cur.total += n
			if err != nil {
				return Resp{}
			}

			if b.cur.len != 0 {
				break
			}

			if len(b.cur.token) < 2 || b.cur.token[len(b.cur.token)-2] != '\r' || b.cur.token[len(b.cur.token)-1] != '\n' {
				log.Errorf("[%d]parse bulk data fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			b.cur.state = stateDone
		case stateDone:
			if b.stack.Size() != 0 && !b.ArrayItemDone() {
				break
			}
			ret = b.cur
			b.ResetCurrent()
			return
		}
	}
}

func (b *Decoder) TryDecodeRequest() (ret Resp) {
	bytesInt := make([]byte, 13)
	for {
		switch b.cur.state {
		case stateType:
			b.cur = Resp{}
			t, err := b.data.ReadByte()
			if err == io.EOF {
				return Resp{}
			}
			if t != '*' {
				break
			}
			b.cur.t = t
			b.cur.state = stateBulkSize
			b.cur.token = bytesInt
			b.cur.array = make([]interface{}, 0, 4)
			b.cur.total++
		case stateBulkSize:
			n, err := b.readLine(b.cur.token[b.cur.len:])
			b.cur.len += n
			b.cur.total += n
			if err == io.ErrShortBuffer {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			if err != nil {
				return Resp{}
			}

			size, err := common.Btoi(b.cur.token[:b.cur.len-2])
			if err != nil {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			b.cur.size = size
			b.cur.state = stateBulkLenPre
		case stateBulkLenPre:
			t, err := b.data.ReadByte()
			if err != nil {
				return Resp{}
			}
			if t != '$' {
				log.Errorf("[%d]parse bulk len pre fail:%s", b.id, string(t))
				b.ResetCurrent()
				break
			}
			b.cur.state = stateBulkLen
			b.cur.token = bytesInt
			b.cur.len = 0
			b.cur.total++
		case stateBulkLen:
			n, err := b.readLine(b.cur.token[b.cur.len:])
			b.cur.len += n
			b.cur.total += n
			if err == io.ErrShortBuffer {
				log.Errorf("[%d]parse bulk size fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			if err != nil {
				return Resp{}
			}

			if len(b.cur.token) < 2 || b.cur.token[len(b.cur.token)-2] != '\r' || b.cur.token[len(b.cur.token)-1] != '\n' {
				log.Errorf("[%d]parse bulk len fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}

			size, err := common.Btoi(b.cur.token[:b.cur.len-2])
			if err != nil {
				log.Errorf("[%d]parse bulk len fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}
			b.cur.len = size + 2
			b.cur.token = make([]byte, int(size)+2)
			b.cur.state = stateBulkData
		case stateBulkData:
			n, err := b.data.Read(b.cur.token[len(b.cur.token)-b.cur.len:])
			b.cur.len -= n
			b.cur.total += n
			if err != nil {
				return Resp{}
			}

			if b.cur.len != 0 {
				break
			}

			if len(b.cur.token) < 2 || b.cur.token[len(b.cur.token)-2] != '\r' || b.cur.token[len(b.cur.token)-1] != '\n' {
				log.Errorf("[%d]parse bulk data fail:%s", b.id, common.BytesToString(b.cur.token))
				b.ResetCurrent()
				break
			}

			b.cur.size--
			b.cur.array = append(b.cur.array, common.BytesToString(b.cur.token[:len(b.cur.token)-2])) //must string
			b.cur.token = nil

			if b.cur.size != 0 {
				b.cur.state = stateBulkLenPre
			} else {
				b.cur.state = stateDone
				ret = b.cur
				b.ResetCurrent()
				return
			}
		}
	}
}

func (b *Decoder) TryDecode() Resp {
	if b.in {
		return b.TryDecodeRequest()
	}
	return b.TryDecodeRespond()
}
