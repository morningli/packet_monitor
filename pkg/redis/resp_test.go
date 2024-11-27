package redis

import (
	"github.com/stretchr/testify/require"
	"runtime/debug"
	"testing"
)

func TestRespBuffer_Feed(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := NewDecoder(true)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok := args.Value().([]interface{})
		require.True(t, ok)
		require.Equal(t, []interface{}{"get", "aa"}, bulk)
		require.Equal(t, 21, args.Size())
	})

	t.Run("mutil", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := NewDecoder(true)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok := args.Value().([]interface{})
		require.True(t, ok)
		require.Equal(t, []interface{}{"get", "aa"}, bulk)
		args = b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok = args.Value().([]interface{})
		require.True(t, ok)
		require.Equal(t, []interface{}{"get", "aa"}, bulk)
	})

	t.Run("interrupt", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$4\r\naaaa\r\n")
		b := NewDecoder(true)
		for i := range data {
			b.Append([]byte{data[i]})
			args := b.TryDecode()
			if i < len(data)-1 {
				require.False(t, args.Valid())
			} else {
				require.True(t, args.Valid())
				bulk, ok := args.Value().([]interface{})
				require.True(t, ok)
				require.Equal(t, []interface{}{"get", "aaaa"}, bulk)
			}
		}
	})
}

func TestRespBuffer_Feed2(t *testing.T) {
	t.Run("array", func(t *testing.T) {
		data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
		b := NewDecoder(false)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		array, ok := args.Value().([]interface{})
		require.True(t, ok)
		require.Len(t, array, 2)
		array0 := array[0].(Resp)
		require.True(t, array0.Valid())
		require.Equal(t, array0.Value(), []byte("get"))
		array1 := array[1].(Resp)
		require.True(t, array1.Valid())
		require.Equal(t, array1.Value(), []byte("aa"))
		require.Equal(t, 21, args.Size())
	})

	t.Run("bulk", func(t *testing.T) {
		data := []byte("$3\r\nget\r\n")
		b := NewDecoder(false)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok := args.Value().([]byte)
		require.True(t, ok)
		require.Equal(t, []byte("get"), bulk)
		require.Equal(t, 9, args.Size())
	})

	t.Run("null", func(t *testing.T) {
		data := []byte("$-1\r\n")
		b := NewDecoder(false)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		require.Nil(t, args.Value())
		require.Equal(t, 5, args.Size())
	})

	t.Run("string", func(t *testing.T) {
		data := []byte("+OK\r\n")
		b := NewDecoder(false)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok := args.Value().([]byte)
		require.True(t, ok)
		require.Equal(t, []byte("OK"), bulk)
		require.Equal(t, 5, args.Size())
	})

	t.Run("int", func(t *testing.T) {
		data := []byte(":100\r\n")
		b := NewDecoder(false)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok := args.Value().([]byte)
		require.True(t, ok)
		require.Equal(t, []byte("100"), bulk)
		require.Equal(t, 6, args.Size())
	})

	t.Run("error", func(t *testing.T) {
		data := []byte("-Err\r\n")
		b := NewDecoder(false)
		b.Append(data)
		args := b.TryDecode()
		require.True(t, args.Valid())
		bulk, ok := args.Value().([]byte)
		require.True(t, ok)
		require.Equal(t, []byte("Err"), bulk)
		require.Equal(t, 6, args.Size())
	})
}

func BenchmarkRespBuffer_TryFetch(b *testing.B) {
	debug.SetGCPercent(400)
	data := []byte("*2\r\n$3\r\nget\r\n$2\r\naa\r\n")
	buff := NewDecoder(true)
	for i := 0; i < b.N; i++ {
		buff.Append(data)
		_ = buff.TryDecode()
	}
}
