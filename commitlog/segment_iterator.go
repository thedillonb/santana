package commitlog

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type segmentIterator struct {
	segment *segment
	pos     int64
}

type segmentMessage struct {
	length   int32
	offset   int64
	position int64
}

func newSegmentIterator(segment *segment, pos int64) *segmentIterator {
	return &segmentIterator{segment, pos}
}

func (s *segmentIterator) next() (msg *segmentMessage, ok bool, err error) {
	lengthAndOffset := make([]byte, 8)
	n, err := s.segment.log.ReadAt(lengthAndOffset, s.pos)

	if err != nil {
		ok = false

		if err == io.EOF {
			return nil, false, nil
		}

		return
	}

	if n != len(lengthAndOffset) {
		ok = false
		err = errors.New("not enough data was read")
		return
	}

	msgLen := int32(binary.LittleEndian.Uint32(lengthAndOffset[:4]))
	offset := int64(binary.LittleEndian.Uint32(lengthAndOffset[4:8])) + s.segment.baseOffset

	ok = true
	msg = &segmentMessage{msgLen, offset, s.pos}
	s.pos += 4 + int64(msgLen)
	return
}
