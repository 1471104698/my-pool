package gpool

import "sync/atomic"

const (
	WorkerInit = iota
	WorkerRunning
	WorkerFree
	WorkerStop
)

// worker
type worker struct {
	p      *pool
	task   chan func()
	status int32
}

// NewWorker
func NewWorker(p *pool) *worker {
	return &worker{
		p:      p,
		task:   make(chan func(), 1),
		status: Init,
	}
}

// run 执行任务
func (w *worker) run() {
	// 开启一个 goroutine 执行任务
	go func() {
		// 阻塞接收任务
		for t := range w.task {
			// 如果当前 worker 被要求停止运行，那么停止阻塞
			if w.isStop() {
				// 判断 pool 是否还在运行，如果是的话那么将它的 runningSize-1
				if w.p.IsRunning() {
					w.p.incrRunning(-1)
				}
				return
			}
			if t == nil {
				continue
			}
			// 如果 pool 已经关闭，那么没必要继续执行了
			if w.p.IsClosed() {
				return
			}
			// 执行任务
			t()
			// 入队 workers，继续等待任务调度
			w.p.addWorker(w)
		}
	}()
}

// setStatus
func (w *worker) setStatus(status int32) {
	atomic.StoreInt32(&w.status, status)
}

// isStop
func (w *worker) isStop() bool {
	return atomic.LoadInt32(&w.status) >= WorkerStop
}

// isStop
func (w *worker) isRunning() bool {
	return atomic.LoadInt32(&w.status) <= WorkerRunning
}
