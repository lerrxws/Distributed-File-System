package paxos

type Data struct {
	N_H int64    `json:"n_h"`
	N_A int64    `json:"n_a"`
	V_A []string `json:"v_a"`
}

type NodeCollection struct {
	Nodes map[string][]Data `json:"nodes"`
}
