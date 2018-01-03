package commitlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

const (
	logNameFormat   = "%020d.log"
	indexNameFormat = "%020d.index"
)

type segment struct {
	segmentOptions
	log      *os.File
	index    *index
	mutex    sync.Mutex
	position int64
}

type segmentOptions struct {
	path       string
	baseOffset int64
	maxSize    uint32
}

func newSegment(opts segmentOptions) (seg *segment, err error) {
	if opts.maxSize == 0 {
		opts.maxSize = 1024 * 1024 * 1024
	}

	rseg := &segment{segmentOptions: opts}

	logPath := filepath.Join(opts.path, fmt.Sprintf(logNameFormat, opts.baseOffset))
	if rseg.log, err = os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
		return nil, errors.Wrap(err, "unable to open segment file")
	}

	logFileInfo, err := rseg.log.Stat()
	if err != nil {
		return nil, err
	}

	rseg.position = logFileInfo.Size()

	indexPath := filepath.Join(opts.path, fmt.Sprintf(indexNameFormat, opts.baseOffset))
	indexOpts := indexOptions{
		path:       indexPath,
		baseOffset: opts.baseOffset,
	}

	if rseg.index, err = newIndex(indexOpts); err != nil {
		return nil, errors.Wrap(err, "unable to open index")
	}

	rebuildIndex := false

	if rseg.index.position == 0 && rseg.position > 0 {
		fmt.Printf("Index missing data... rebuilding\n")
		rebuildIndex = true
	}

	if err = rseg.index.valid(); err != nil {
		fmt.Printf("Index is not valid... rebuilding\n")
		rebuildIndex = true
	}

	if rebuildIndex == true {
		if err = rseg.rebuildIndex(); err != nil {
			fmt.Printf("Error rebuilding index: %v", err.Error())
		}
	}

	seg = rseg
	return
}

func (s *segment) append(offset int64, b []byte) error {
	if offset < s.baseOffset {
		return errors.New("invalid offset")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	msg := message{
		data:   b,
		offset: int32(offset - s.baseOffset),
	}

	n, err := msg.serialize(s.log)
	if err != nil {
		return err
	}

	currentPosition := s.position
	logger.Debug("Appended %v bytes with offset %v at position %v\n", len(b), offset, currentPosition)
	s.position += int64(n)
	return s.index.append(offset, currentPosition)
}

func (s *segment) readAt(b []byte, offset int64) (n int, err error) {
	position, err := s.index.lookup(offset)
	if err != nil {
		return 0, err
	}

	fmt.Printf("found index at %v\n", position)

	length := make([]byte, 4)
	n, err = s.log.ReadAt(length, int64(position))
	if err != nil {
		return n, err
	}
	if n != 4 {
		return n, errors.New("not enough data was read")
	}

	msgLen := binary.LittleEndian.Uint32(length)
	if int(msgLen) > len(b) {
		return n, errors.New("not enough buffer space for message")
	}

	n, err = s.log.ReadAt(b[:msgLen], int64(position)+4)
	return
}

func (s *segment) rebuildIndex() error {
	if err := s.index.delete(); err != nil {
		return err
	}

	index, err := newIndex(s.index.indexOptions)
	if err != nil {
		return err
	}

	s.index = index
	iter := newSegmentIterator(s, 0)

	for {
		msg, ok, err := iter.next()
		if err != nil {
			return err
		}

		if ok == false {
			return nil
		}

		if err = s.index.append(msg.offset, msg.position); err != nil {
			return err
		}
	}
}

func (s *segment) getNextOffset() (offset int64, err error) {
	pos, err := s.index.lookup(s.index.lastOffset)
	logger.Debug("Looking up last offset with index offset = %v\n", s.index.lastOffset)

	iter := newSegmentIterator(s, pos)

	for {
		msg, ok, err := iter.next()
		if err != nil {
			return 0, err
		}

		if ok == false {
			return offset, nil
		}

		offset = msg.offset + 1
	}
}

func (s *segment) close() error {
	if err := s.log.Close(); err != nil {
		if err := s.index.close(); err != nil {
			logger.Warn("Unable to close index while segment was being closed: %v\n", s.index.name())
		}

		return err
	}

	return s.index.close()
}

func (s *segment) name() string {
	return s.log.Name()
}

func (s *segment) delete() error {
	_ = s.log.Close()

	if err := os.Remove(s.log.Name()); err != nil {
		_ = s.index.delete()
		return err
	}

	return s.index.delete()
}

func (s *segment) flush() error {
	if err := s.log.Sync(); err != nil {
		return err
	}

	return s.index.sync()
}