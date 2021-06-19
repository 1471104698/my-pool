package gpool

import "sync"

// worker
type worker struct {
	cond *sync.Cond
	task interface{}	// 如何传入待定
}
