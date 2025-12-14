package filelogger

type FileLogger interface {
	Append(v any) error
	Close() error
}
