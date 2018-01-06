package commitlog

import (
	"encoding/json"
	"io/ioutil"
	"math"
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

	configFileName = "config.cfg"
)

var logger = l.NewLogger("commitlog")

type CommitLogOptions struct {
	Path          string `json:"-"`
	FlushInterval int    `json:"flushInterval"`
	IndexMaxBytes int    `json:"indexMaxBytes"`
	LogMaxBytes   int    `json:"logMaxBytes"`
	TTL           int    `json:"ttl"`
}

type CommitLog struct {
	CommitLogOptions
	segments      []*segment
	activeSegment *segment
	nextOffset    int64
	mutex         sync.Mutex
	recoveryPoint int64
}

func OpenCommitLog(dir string) (*CommitLog, error) {
	configFile := path.Join(dir, configFileName)

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return nil, errors.Errorf("config file missing for %s", dir)
	}

	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	var opts CommitLogOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		return nil, err
	}

	opts.Path = dir

	return NewCommitLog(opts)
}

func NewCommitLog(opts CommitLogOptions) (*CommitLog, error) {
	if opts.FlushInterval == 0 {
		opts.FlushInterval = 1
	}
	if opts.LogMaxBytes == 0 {
		opts.LogMaxBytes = 1024 * 1024 * 1024
	}
	if opts.IndexMaxBytes == 0 {
		opts.IndexMaxBytes = 1024 * 1024 * 8
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

	if err := commitLog.saveConfig(); err != nil {
		return nil, errors.Wrap(err, "failed to save log configuration")
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

			if _, err = commitLog.appendNewSegment(baseOffset); err != nil {
				return nil, errors.Wrap(err, "failed to append new segment")
			}
		}
	}

	if len(commitLog.segments) == 0 {
		if _, err := commitLog.appendNewSegment(0); err != nil {
			return nil, err
		}
	}

	commitLog.activeSegment = commitLog.segments[len(commitLog.segments)-1]

	if commitLog.nextOffset, err = commitLog.activeSegment.getNextOffset(); err != nil {
		return nil, err
	}

	commitLog.recoveryPoint = commitLog.nextOffset

	return commitLog, nil
}

func (c *CommitLog) saveConfig() error {
	data, err := json.Marshal(c.CommitLogOptions)
	if err != nil {
		return err
	}

	filename := path.Join(c.Path, configFileName)
	return ioutil.WriteFile(filename, data, 0755)
}

func (c *CommitLog) appendNewSegment(baseOffset int64) (*segment, error) {
	opts := segmentOptions{
		path:         c.Path,
		baseOffset:   baseOffset,
		maxIndexSize: c.IndexMaxBytes,
		maxLogSize:   c.LogMaxBytes,
	}

	segment, err := newSegment(opts)
	if err != nil {
		return nil, err
	}

	c.segments = append(c.segments, segment)
	return segment, nil
}

func (c *CommitLog) Append(b []byte) (offset int64, err error) {
	c.mutex.Lock()

	if c.activeSegment.isFull() {
		logger.Info("%s is full; rotating...", c.Name())

		var newSeg *segment
		if newSeg, err = c.appendNewSegment(c.nextOffset); err != nil {
			return
		}

		c.activeSegment = newSeg
		if c.nextOffset, err = newSeg.getNextOffset(); err != nil {
			return
		}
	}

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
	var i int
	for i = 0; i < len(c.segments); i++ {
		offsetCeiling := int64(math.MaxInt64)
		if i+1 < len(c.segments) {
			offsetCeiling = c.segments[i+1].baseOffset
		}

		if offset >= c.segments[i].baseOffset && offset < offsetCeiling {
			break
		}
	}
	segs := c.segments[i:]
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

func (c *CommitLog) Name() string {
	return path.Base(c.Path)
}
