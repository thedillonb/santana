package commitlog

import (
	"encoding/binary"
	"os"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

type indexOptions struct {
	path       string
	baseOffset int64
	maxBytes   int
}

type index struct {
	indexOptions
	file       *os.File
	mutex      sync.RWMutex
	position   int64
	lastOffset int64
	mmap       mmap.MMap
}

const fileEntrySize = 8

func newIndex(opts indexOptions) (idx *index, err error) {
	if opts.maxBytes == 0 {
		return nil, errors.New("invalid index max bytes")
	}

	ridx := &index{
		indexOptions: opts,
		lastOffset:   opts.baseOffset,
	}

	ridx.file, err = os.OpenFile(opts.path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create index file")
	}

	fileInfo, err := ridx.file.Stat()
	if err != nil {
		ridx.file.Close()
		return nil, errors.Wrap(err, "unable to retrieve index file information")
	}

	ridx.position = fileInfo.Size()

	if err = ridx.file.Truncate(int64(opts.maxBytes + (opts.maxBytes % fileEntrySize))); err != nil {
		ridx.file.Close()
		return nil, errors.Wrap(err, "unable to truncate file size")
	}

	ridx.mmap, err = mmap.Map(ridx.file, mmap.RDWR, 0)
	if err != nil {
		ridx.file.Close()
		return nil, errors.Wrap(err, "unable to map file into memory")
	}

	if ridx.position > 0 {
		if ridx.lastOffset, _, err = ridx.read(ridx.position - fileEntrySize); err != nil {
			ridx.file.Close()
			return
		}
	}

	idx = ridx
	return
}

func (idx *index) append(offset int64, position int64) error {
	if idx.position > 0 && offset <= idx.lastOffset {
		return errors.New("offset must be greater than previous offsets")
	}
	if idx.isFull() {
		return errors.New("index is full")
	}

	data := make([]byte, fileEntrySize)
	binary.BigEndian.PutUint32(data[:4], uint32(offset-idx.baseOffset))
	binary.BigEndian.PutUint32(data[4:8], uint32(position))

	idx.mutex.Lock()
	copy(idx.mmap[idx.position:idx.position+fileEntrySize], data)
	idx.position += fileEntrySize
	idx.lastOffset = offset
	idx.mutex.Unlock()

	logger.Debug("Appended index entry mapping %v => %v\n", offset, position)
	return nil
}

func (idx *index) lookup(offset int64) (position int64, err error) {
	if offset < idx.baseOffset || offset > idx.lastOffset {
		err = errors.Errorf("offset outside of range: %v < %v < %v ", idx.baseOffset, offset, idx.lastOffset)
		return
	}

	_, position, err = idx.read((offset - idx.baseOffset) * fileEntrySize)
	return
}

func (idx *index) read(n int64) (offset int64, position int64, err error) {
	if n+fileEntrySize > int64(len(idx.mmap)) {
		err = errors.New("read out of range")
		return
	}

	buff := make([]byte, fileEntrySize)
	copy(buff, idx.mmap[n:n+fileEntrySize])
	offset = int64(binary.BigEndian.Uint32(buff[0:4])) + idx.baseOffset
	position = int64(binary.BigEndian.Uint32(buff[4:8]))
	return
}

func (idx *index) getEntries() int64 {
	return idx.position / fileEntrySize
}

func (idx *index) isFull() bool {
	return idx.position >= int64(idx.maxBytes)
}

func (idx *index) name() string {
	return idx.file.Name()
}

func (idx *index) sync() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if err := idx.mmap.Flush(); err != nil {
		return errors.Wrap(err, "error syncing index mmap copy")
	}

	return nil
}

func (idx *index) close() error {
	if err := idx.sync(); err != nil {
		return err
	}

	if err := idx.file.Truncate(idx.position); err != nil {
		return errors.Wrap(err, "failed to truncate file")
	}

	return idx.file.Close()
}

func (idx *index) valid() error {
	idx.mutex.RLock()
	defer idx.mutex.RUnlock()

	if idx.position == 0 {
		return nil
	}

	if idx.position%fileEntrySize != 0 {
		return errors.Errorf(
			"index position %v does not align with entry size %v",
			idx.position, fileEntrySize)
	}

	// Need something to catch the case where a log has been released
	// without being closed propertly. The size is then MAX so it seems as
	// if there are entries but they're invalid.
	if idx.position > fileEntrySize*2 {
		off1, _, err := idx.read(idx.position - fileEntrySize)
		if err != nil {
			return err
		}

		off2, _, err := idx.read(idx.position - fileEntrySize*2)
		if err != nil {
			return err
		}

		if off1 == off2 {
			return errors.Errorf("index contains non-incrementing offsets")
		}
	}

	return nil
}

func (idx *index) delete() error {
	if err := idx.mmap.Unmap(); err != nil {
		logger.Warn("Unable to unmap index file while deleting it: %v\n", idx.name())
	}

	if err := idx.file.Close(); err != nil {
		return err
	}

	return os.Remove(idx.path)
}
