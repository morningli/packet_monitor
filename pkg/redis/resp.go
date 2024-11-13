package redis

import (
	"bytes"
	"io"
	"strconv"
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

func (b *RespBuffer) TryFetch() (ret []interface{}) {
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
		case stateBulkSize:
			line, err := b.data.ReadBytes('\n')
			b.token = append(b.token, line...)
			if err != nil {
				return nil
			}
			size, err := strconv.ParseInt(string(b.token[:len(b.token)-2]), 10, 64)
			if err != nil {
				b.state = stateType
				break
			}
			b.size = int(size)
			b.token = nil
			b.state = stateBulkLenPre
		case stateBulkLenPre:
			t, err := b.data.ReadByte()
			if err != nil {
				return nil
			}
			if t != '$' {
				b.state = stateType
				break
			}
			b.state = stateBulkLen
		case stateBulkLen:
			line, err := b.data.ReadBytes('\n')
			b.token = append(b.token, line...)
			if err != nil {
				return nil
			}
			size, err := strconv.ParseInt(string(b.token[:len(b.token)-2]), 10, 64)
			if err != nil {
				b.state = stateType
				break
			}
			b.len = int(size) + 2
			b.state = stateBulkData
		case stateBulkData:
			tmp := make([]byte, 128)
			n, err := b.data.Read(tmp)
			if err != nil {
				return nil
			}
			b.token = append(b.token, tmp[:n]...)
			b.len -= n
			if b.len != 0 {
				break
			}
			if b.token[len(b.token)-2] != '\r' || b.token[len(b.token)-1] != '\n' {
				b.state = stateType
				break
			}

			b.size--
			b.ret = append(b.ret, b.token)
			b.token = nil

			if b.size != 0 {
				b.state = stateBulkLen
			} else {
				b.state = stateType
				ret = b.ret
				b.ret = nil
				return
			}
		}
	}
}
