package lockcache

import (
	"sync"
)

type Lock struct {
	State LockState
	Cond  *sync.Cond
}

func NewLock(state LockState) (*Lock) {
	lock := &Lock{
		State: state,
	}
	lock.Cond = sync.NewCond(&sync.Mutex{})
	return lock
}

func (l *Lock) GetState() LockState {
	return l.State
}

func (l *Lock) SetState(state LockState) {
	l.State = state
}