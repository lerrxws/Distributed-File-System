package paxos

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type PaxosLogger struct {
	filename string
	perm     os.FileMode
}

func CreateNewPaxosLogger(filename string) (*PaxosLogger, error) {
	if !checkFileType(filename) {
		return nil, fmt.Errorf("the file is not a JSON")
	}

	l := &PaxosLogger{
		filename: filename,
		perm:     0644,
	}

	return l, nil
}

func (l *PaxosLogger) GetViewInfo(nodeId string, vId int64) (Data, error) {
	dataJson, err := os.ReadFile(l.filename)
	if err != nil {
		return Data{}, err
	}

	nodes, err := unpackJsonData(dataJson)
	if err != nil{
		return Data{}, err
	}

	dataCollection := nodes.Nodes[nodeId]

	for _, data := range dataCollection{
		if data.N_A == vId {
			return data, nil
		}
	}

	return Data{}, nil
}

func (l *PaxosLogger) SaveViewInfo(nodeId string, data Data) error{
	dataJson, err := os.ReadFile(l.filename)
	if err != nil {
		return err
	}

	nodes, err := unpackJsonData(dataJson)
	if err != nil{
		return err
	}

	dataCollection := nodes.Nodes[nodeId]
	dataCollection = append(dataCollection, data)
	nodes.Nodes[nodeId] = dataCollection

	dataJson, err = packJsonData(nodes)
	if err != nil {
		return err
	}

	err = os.WriteFile(l.filename, dataJson, l.perm)
	if err != nil {
		return err
	}

	return nil
}

// region private methods

func checkFileType(filename string) bool{
	return strings.HasSuffix(filename, ".json")
}

func packJsonData(nodeCollection NodeCollection) ([]byte, error) {
	return json.Marshal(nodeCollection)
}

func unpackJsonData(data []byte) (NodeCollection, error) {
	var nodeCollection NodeCollection
	err := json.Unmarshal(data, &nodeCollection)
	if err != nil {
		return NodeCollection{}, err
	}

	return nodeCollection, nil
}
// endregion
