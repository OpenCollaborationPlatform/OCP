package p2p

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p-net"
)

var (
	errSmallBuffer = errors.New("Buffer Too Small")
	errLargeValue  = errors.New("Value is Larger than 64 bits")
)

func newStreamWriter(s net.Stream) *streamWriter {
	return &streamWriter{s, make([]byte, 10)}
}

type streamWriter struct {
	Stream net.Stream
	lenBuf []byte
}

func (w *streamWriter) WriteMsg(msg Message) error {

	//TODO: this is a allocation for each msg. Use buffer to make this more elegant
	data, err := Serialize(msg)
	if err != nil {
		return err
	}

	length := uint64(len(data))
	n := binary.PutUvarint(w.lenBuf, length)
	_, err = w.Stream.Write(w.lenBuf[:n])
	if err != nil {
		return err
	}
	_, err = w.Stream.Write(data)
	return err
}

func (w *streamWriter) Close() error {
	w.Stream.Close()
	return nil
}

func newStreamReader(s net.Stream, maxSize int) *streamReader {
	return &streamReader{s, bufio.NewReader(s), nil, maxSize, nil}
}

type streamReader struct {
	Stream  net.Stream
	reader  *bufio.Reader
	buf     []byte
	maxSize int
	forward chan Message
}

func (r *streamReader) ReadMsg() (Message, error) {
	length64, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return Error{err.Error()}, err
	}
	length := int(length64)
	if length < 0 || length > r.maxSize {
		return Error{"Stream read failed: too short buffer"}, io.ErrShortBuffer
	}
	if len(r.buf) < length {
		r.buf = make([]byte, length)
	}
	buf := r.buf[:length]
	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return Error{err.Error()}, err
	}
	return Deserialize(buf)
}

func (r *streamReader) Close() error {
	r.Stream.Close()
	return nil
}

func (r *streamReader) ForwardMsg(channel chan Message) error {

	if r.forward != nil {
		return fmt.Errorf("Messages already forwarded")
	}
	r.forward = channel
	go func() {
		for {
			msg, err := r.ReadMsg()
			if err != nil {
				r.Close()
				break
			}
			channel <- msg
		}
	}()
	return nil
}

func newStreamMessenger(s net.Stream) streamMessenger {
	reader := newStreamReader(s, 2048)
	writer := newStreamWriter(s)

	return streamMessenger{reader, writer}
}

type streamMessenger struct {
	reader *streamReader
	writer *streamWriter
}

func (sm *streamMessenger) WriteMsg(msg Message) error {
	return sm.writer.WriteMsg(msg)
}

func (sm *streamMessenger) ReadMsg() (Message, error) {
	return sm.reader.ReadMsg()
}

func (sm *streamMessenger) Close() error {
	//reader and writer have the same stream, one close is enough
	sm.reader.Close()
	return nil
}
