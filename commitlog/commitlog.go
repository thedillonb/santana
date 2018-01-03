package commitlog

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"

	l "santana/logger"
)

const (
	logFileSuffix   = ".log"
	indexFileSuffix = ".index"
)

var logger = l.NewLogger("commitlog")

type CommitLog struct {
	CommitLogOptions
	segments      []*segment
	activeSegment *segment
	nextOffset    int64
	mutex         sync.Mutex
	recoveryPoint int64
}

type CommitLogOptions struct {
	Path          string
	FlushInterval int
}

func NewCommitLog(opts CommitLogOptions) (*CommitLog, error) {
	if opts.FlushInterval == 0 {
		opts.FlushInterval = 1
	}

	err := os.MkdirAll(opts.Path, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create log directory")
	}

	files, err := ioutil.ReadDir(opts.Path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read log directory")
	}

	commitLog := &CommitLog{
		CommitLogOptions: opts,
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), indexFileSuffix) {
			logFileName := strings.TrimSuffix(file.Name(), indexFileSuffix) + logFileSuffix
			logFilePath := path.Join(opts.Path, logFileName)

			if _, err = os.Stat(logFilePath); os.IsNotExist(err) {
				indexFilePath := path.Join(opts.Path, file.Name())
				logger.Warn("Found orphaned index file %v. Deleting...\n", indexFilePath)

				if err := os.Remove(indexFilePath); err != nil {
					logger.Warn("Unable to remove orphaned index file %v!\n", indexFilePath)
				}
			}
		} else if strings.HasSuffix(file.Name(), logFileSuffix) {
			baseOffsetStr := strings.TrimSuffix(file.Name(), logFileSuffix)
			baseOffset, err := strconv.ParseInt(baseOffsetStr, 10, 64)
			if err != nil {
				// Unwind open stuff
				return nil, errors.Wrap(err, "")
			}

			segOpts := segmentOptions{
				baseOffset: baseOffset,
				path:       opts.Path,
			}

			segment, err := newSegment(segOpts)
			if err != nil {
				// Unwind open stuff
				return nil, errors.Wrap(err, "")
			}

			commitLog.segments = append(commitLog.segments, segment)
		}
	}

	if len(commitLog.segments) == 0 {
		segOps := segmentOptions{
			path: opts.Path,
		}

		segment, err := newSegment(segOps)
		if err != nil {
			return nil, err
		}

		commitLog.segments = append(commitLog.segments, segment)
	}

	commitLog.activeSegment = commitLog.segments[len(commitLog.segments)-1]
	commitLog.nextOffset, err = commitLog.activeSegment.getNextOffset()
	if err != nil {
		return nil, err
	}

	commitLog.recoveryPoint = commitLog.nextOffset

	return commitLog, nil
}

func (c *CommitLog) Append(b []byte) (offset int64, err error) {
	c.mutex.Lock()
	offset = c.nextOffset
	c.nextOffset++
	err = c.activeSegment.append(offset, b)
	c.mutex.Unlock()

	if err != nil {
		return
	}

	if c.getUnflushedMessages() >= int64(c.FlushInterval) {
		if err = c.flush(); err != nil {
			return
		}
	}

	return
}

func (c *CommitLog) ReadAt(b []byte, offset int64) (n int, err error) {
	for i := len(c.segments) - 1; i >= 0; i-- {
		if offset >= c.segments[i].baseOffset {
			n, err = c.segments[i].readAt(b, offset)
			return
		}
	}

	err = errors.New("offset does not exist")
	return
}

func (c *CommitLog) Close() error {
	for _, s := range c.segments {
		if err := s.close(); err != nil {
			logger.Warn("Error closing segment: %v\n", err.Error())
		}
	}

	return nil
}

func (c *CommitLog) getUnflushedMessages() int64 {
	return c.nextOffset - c.recoveryPoint
}

func (c *CommitLog) flush() error {
	return c.flushFromOffset(c.nextOffset)
}

func (c *CommitLog) flushFromOffset(offset int64) error {
	if offset <= c.recoveryPoint {
		return nil
	}

	c.mutex.Lock()
	var segs []*segment
	for i := len(c.segments) - 1; i >= 0; i-- {
		if offset >= c.segments[i].baseOffset {
			segs = append(segs, c.segments[i])
		}
	}
	c.mutex.Unlock()

	for _, s := range segs {
		logger.Debug("Flushing log segment %v\n", s.name())
		if err := s.flush(); err != nil {
			return err
		}
	}

	c.mutex.Lock()
	if offset > c.recoveryPoint {
		c.recoveryPoint = offset
	}
	c.mutex.Unlock()

	return nil
}
