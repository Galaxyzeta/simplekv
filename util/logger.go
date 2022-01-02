package util

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"time"
)

// TODO let's implement an async log writer.

type Logger struct {
	instance *log.Logger
}

func NewLogger(prefix string, writer io.Writer) *Logger {
	return &Logger{
		instance: log.New(writer, prefix, 0),
	}
}

func (l *Logger) Debugf(format string, elems ...interface{}) {
	l.commonPrint("Debug", format, elems...)
}

func (l *Logger) Infof(format string, elems ...interface{}) {
	l.commonPrint("Info", format, elems...)
}

func (l *Logger) Errorf(format string, elems ...interface{}) {
	l.commonPrint("Error", format, elems...)
}

func (l *Logger) Warnf(format string, elems ...interface{}) {
	l.commonPrint("Warn", format, elems...)
}

func (l *Logger) commonPrint(level string, format string, elems ...interface{}) {
	l.instance.Printf(fmt.Sprintf("[%s] %s | %s | %s\n", level, getCaller(), time.Now().Format("2006-01-02 15:04:05.000"), format), elems...)
}

func getCaller() string {
	// Get caller
	fpcs := make([]uintptr, 1)
	// Skip runtime.Caller() <-- getCaller() <-- commonPrint() <-- Infof()
	n := runtime.Callers(4, fpcs)
	if n == 0 {
		return ""
	}
	caller := runtime.FuncForPC(fpcs[0] - 1)
	if caller == nil {
		return ""
	}
	return caller.Name()
}
