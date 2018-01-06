package commitlog

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
)

var (
	ErrLogNotFound   = errors.New("log not found")
	ErrAlreadyExists = errors.New("log already exists")
)

type LogManagerOptions struct {
	Dir string
}

type LogManager struct {
	LogManagerOptions
	logs map[string]*CommitLog
}

func NewLogManager(opts LogManagerOptions) (*LogManager, error) {
	files, err := ioutil.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}

	lm := &LogManager{
		LogManagerOptions: opts,
		logs:              make(map[string]*CommitLog),
	}

	for _, f := range files {
		if f.IsDir() {
			l, err := OpenCommitLog(path.Join(opts.Dir, f.Name()))
			if err != nil {
				return nil, err
			}

			lm.logs[f.Name()] = l
		}
	}

	return lm, nil
}

func (s *LogManager) CreateLog(name string, opts CommitLogOptions) (*CommitLog, error) {
	if _, ok := s.logs[name]; ok {
		return nil, ErrAlreadyExists
	}

	opts.Path = path.Join(s.Dir, name)
	l, err := NewCommitLog(opts)
	if err != nil {
		return nil, err
	}

	s.logs[name] = l
	return l, nil
}

func (s *LogManager) DeleteLog(name string) error {
	l, ok := s.logs[name]
	if !ok {
		return ErrLogNotFound
	}

	if err := l.Close(); err != nil {
		return err
	}

	if err := os.RemoveAll(path.Join(s.Dir, name)); err != nil {
		return err
	}

	delete(s.logs, name)
	return nil
}

func (s *LogManager) GetLog(name string) (*CommitLog, error) {
	if l, ok := s.logs[name]; ok {
		return l, nil
	}

	return nil, ErrLogNotFound
}

func (s *LogManager) GetLogs() []string {
	var logs []string

	for k := range s.logs {
		logs = append(logs, k)
	}

	return logs
}

func (s *LogManager) Close() {
	for _, v := range s.logs {
		_ = v.Close()
	}

	s.logs = map[string]*CommitLog{}
}
