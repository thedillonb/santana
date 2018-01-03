package logger

import (
	"log"
)

type Logger struct {
	name string
}

func NewLogger(name string) Logger {
	return Logger{name}
}

func (l Logger) Debug(format string, args ...interface{}) {
	log.Printf("DEBG "+format, args...)
}

func (l Logger) Info(format string, args ...interface{}) {
	log.Printf("INFO "+format, args...)
}

func (l Logger) Warn(format string, args ...interface{}) {
	log.Printf("WARN "+format, args...)
}
