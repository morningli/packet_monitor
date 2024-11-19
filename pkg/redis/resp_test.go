package redis

import (
	"github.com/stretchr/testify/require"
	"runtime/debug"
	"testing"
)

func TestRespBuffer_Feed(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := &Decoder{}
		b.Append(data)
		args := b.TryDecode()
		require.Equal(t, []interface{}{[]byte("get"), []byte("aa")}, args)
	})

	t.Run("mutil", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := &Decoder{}
		b.Append(data)
		args := b.TryDecode()
		require.Equal(t, []interface{}{[]byte("get"), []byte("aa")}, args)
		args = b.TryDecode()
		require.Equal(t, []interface{}{[]byte("get"), []byte("aa")}, args)
	})

	t.Run("interrupt", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$4\r\naaaa\r\n")
		b := &Decoder{}
		for i := range data {
			b.Append([]byte{data[i]})
			args := b.TryDecode()
			if i < len(data)-1 {
				require.Nil(t, args)
			} else {
				require.Equal(t, []interface{}{[]byte("get"), []byte("aaaa")}, args)
			}
		}
	})
}

func BenchmarkRespBuffer_TryFetch(b *testing.B) {
	debug.SetGCPercent(400)
	data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
	buff := &Decoder{}
	for i := 0; i < b.N; i++ {
		buff.Append(data)
		_ = buff.TryDecode()
	}
}
