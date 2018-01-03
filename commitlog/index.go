package commitlog

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

type index struct {
	indexOptions
	file       *os.File
	mutex      sync.RWMutex
	position   int64
	lastOffset int64
	mmap       mmap.MMap
}

type indexOptions struct {
	path       string
	baseOffset int64
	maxBytes   int
}

const fileEntrySize = 8

func newIndex(opts indexOptions) (idx *index, err error) {
	if opts.maxBytes == 0 {
		opts.maxBytes = 1024 * 1024 * 8
	}

	ridx := &index{indexOptions: opts}

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

	data := make([]byte, fileEntrySize)
	binary.LittleEndian.PutUint32(data[:4], uint32(offset-idx.baseOffset))
	binary.LittleEndian.PutUint32(data[4:8], uint32(position))

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
		err = errors.New("offset outside of range")
		return
	}

	_, position, err = idx.read((offset - idx.baseOffset) * fileEntrySize)
	return
}

func (idx *index) read(n int64) (offset int64, position int64, err error) {
	if n+fileEntrySize >= int64(len(idx.mmap)) {
		err = errors.New("read out of range")
		return
	}

	buff := make([]byte, fileEntrySize)
	copy(buff, idx.mmap[n:n+fileEntrySize])
	offset = int64(binary.LittleEndian.Uint32(buff[0:4])) + idx.baseOffset
	position = int64(binary.LittleEndian.Uint32(buff[4:8]))
	return
}

func (idx *index) getEntries() int64 {
	return idx.position / fileEntrySize
}

func (idx *index) isFull() bool {
	return idx.getEntries() >= int64(len(idx.mmap)/fileEntrySize)
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
		return errors.New("corrupt index")
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
