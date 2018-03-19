package commitlog

import (
	"encoding/binary"
	"io"
)

type message struct {
	offset    int32
	timestamp int32
	data      []byte
}

func (m *message) getLength() int {
	return 8 + len(m.data)
}

func (m *message) writeTo(wr io.Writer) (int, error) {
	length := m.getLength()
	_ = binary.Write(wr, binary.BigEndian, uint32(length))
	_ = binary.Write(wr, binary.BigEndian, uint32(m.offset))
	_ = binary.Write(wr, binary.BigEndian, uint32(m.timestamp))
	_, _ = wr.Write(m.data)
	return length + 4, nil
}
