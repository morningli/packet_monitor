package redis

import (
	"bytes"
	"github.com/morningli/packet_monitor/pkg/common"
	log "github.com/sirupsen/logrus"
	"io"
)

type stat int

const (
	stateType = iota
	stateBulkSize
	stateBulkLenPre
	stateBulkLen
	stateBulkData
)

type RespBuffer struct {
	state stat
	data  bytes.Buffer

	size  int
	len   int
	token []byte
	ret   []interface{}
}

func (b *RespBuffer) Feed(data []byte) {
	b.data.Write(data)
}

func (b *RespBuffer) ReadBytes(line []byte, delim byte) (n int, err error) {
	for i := 0; i < len(line); i++ {
		d, err := b.data.ReadByte()
		if err != nil {
			return i, err
		}
		line[i] = d
		if d == delim {
			return i + 1, nil
		}
	}
	return len(line), io.ErrShortBuffer
}

func (b *RespBuffer) TryFetch() (ret []interface{}) {
	bytesInt := make([]byte, 13)
	for {
		switch b.state {
		case stateType:
			t, err := b.data.ReadByte()
			if err == io.EOF {
				return nil
			}
			if t != '*' {
				break
			}
			b.state = stateBulkSize
			b.size = 0
			b.len = 0
			b.token = bytesInt
			b.ret = make([]interface{}, 0, 4)
		case stateBulkSize:
			n, err := b.ReadBytes(b.token[b.len:], '\n')
			b.len += n
			if err == io.ErrShortBuffer {
				log.Errorf("parse bulk size fail:%s", common.BytesToString(b.token))
				b.state = stateType
				break
			}
			if err != nil {
				return nil
			}

			size, err := common.Btoi(b.token[:b.len-2])
			if err != nil {
				log.Errorf("parse bulk size fail:%s", common.BytesToString(b.token[:b.len-2]))
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
				log.Errorf("parse bulk len pre fail:%s", string(t))
				b.state = stateType
				break
			}
			b.state = stateBulkLen
			b.token = bytesInt
			b.len = 0
		case stateBulkLen:
			n, err := b.ReadBytes(b.token[b.len:], '\n')
			b.len += n
			if err == io.ErrShortBuffer {
				log.Errorf("parse bulk size fail:%s", common.BytesToString(b.token))
				b.state = stateType
				break
			}
			if err != nil {
				return nil
			}

			size, err := common.Btoi(b.token[:b.len-2])
			if err != nil {
				log.Errorf("parse bulk len fail:%s", common.BytesToString(b.token[:b.len-2]))
				b.state = stateType
				break
			}
			b.len = int(size) + 2
			b.token = make([]byte, int(size)+2)
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

			if b.token[len(b.token)-2] != '\r' || b.token[len(b.token)-1] != '\n' {
				log.Errorf("parse bulk data fail:%s", common.BytesToString(b.token[len(b.token)-2:]))
				b.state = stateType
				break
			}

			b.size--
			b.ret = append(b.ret, b.token[:len(b.token)-2])
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
