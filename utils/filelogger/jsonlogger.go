package filelogger

import (
	"encoding/json"
	"os"
	"sync"
)

type JsonFileLogger struct {
	mu   sync.Mutex
	file *os.File
}

func NewJsonFileLogger(path string) (*JsonFileLogger, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &JsonFileLogger{
		file: file,
	}, nil
}

func (l *JsonFileLogger) Append(v any) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	if _, err := l.file.Write(append(data, '\n')); err != nil {
		return err
	}

	return l.file.Sync()
}

func (l *JsonFileLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.file.Close()
}
