package redis

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRespBuffer_Feed(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := &RespBuffer{}
		b.Feed(data)
		args := b.TryFetch()
		require.Equal(t, []interface{}{[]byte("get"), []byte("aa")}, args)
	})

	t.Run("mutil", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := &RespBuffer{}
		b.Feed(data)
		args := b.TryFetch()
		require.Equal(t, []interface{}{[]byte("get"), []byte("aa")}, args)
		args = b.TryFetch()
		require.Equal(t, []interface{}{[]byte("get"), []byte("aa")}, args)
	})

	t.Run("interrupt", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$4\r\naaaa\r\n")
		b := &RespBuffer{}
		for i := range data {
			b.Feed([]byte{data[i]})
			args := b.TryFetch()
			if i < len(data)-1 {
				require.Nil(t, args)
			} else {
				require.Equal(t, []interface{}{[]byte("get"), []byte("aaaa")}, args)
			}
		}
	})
}
