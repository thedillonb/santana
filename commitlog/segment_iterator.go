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
	metadata := make([]byte, 12)
	n, err := s.segment.log.ReadAt(metadata, s.pos)

	if err != nil {
		ok = false

		if err == io.EOF {
			return nil, false, nil
		}

		return
	}

	if n != len(metadata) {
		ok = false
		err = errors.New("not enough data was read")
		return
	}

	offset := int64(binary.BigEndian.Uint64(metadata[:8]))
	msgLen := int32(binary.BigEndian.Uint32(metadata[8:12]))

	ok = true
	msg = &segmentMessage{msgLen, offset, s.pos}
	s.pos += 4 + int64(msgLen)
	return
}
