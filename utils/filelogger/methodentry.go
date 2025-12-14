package filelogger

type MethodLogEntry struct {
	Timestamp string   `json:"timestamp"`
	Method    string   `json:"method"`
	Params    []string `json:"params"`
}
