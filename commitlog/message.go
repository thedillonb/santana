package commitlog

import (
	"encoding/binary"
	"io"
)

type message struct {
	offset int32
	data   []byte
}

func (m *message) getLength() int {
	return 4 + len(m.data)
}

func (m *message) serialize(wr io.Writer) (int, error) {
	length := m.getLength()
	_ = binary.Write(wr, binary.LittleEndian, uint32(length))
	_ = binary.Write(wr, binary.LittleEndian, uint32(m.offset))
	_, _ = wr.Write(m.data)
	return length + 4, nil
}
