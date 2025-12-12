package paxos

type CommitViewFunc func(viewId int64, view []string)