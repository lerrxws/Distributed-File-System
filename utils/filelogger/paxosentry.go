package filelogger

type PaxosLogEntry struct {
	Timestamp  string   `json:"timestamp"`
	Instance   int64    `json:"instance"`

	NH         int64    `json:"n_h"`
	NA         int64    `json:"n_a"`
	VA         []string `json:"v_a,omitempty"`
}
