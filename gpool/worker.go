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
		// 阻塞接收任务，chan 阻塞的是 G，不会影响到 M，M 仍然可以继续去跟其他的 G 进行绑定
		for t := range w.task {

			// 当前 worker 是否需要停止继续执行
			if w.isNeedStop() {
				return
			}
			if t == nil {
				continue
			}
			if w.isNeedStop() {
				return
			}
			// 执行任务
			t()
			if w.isNeedStop() {
				return
			}
			// 从任务队列中获取任务，如果队列中有任务那么一直执行
			for t = w.p.deTaskQueue(); t != nil; t = w.p.deTaskQueue() {
				t()
				if w.isNeedStop() {
					return
				}
			}
			// 没有任务执行，入队 workers，继续等待任务调度
			w.p.addWorker(w)
		}
	}()
}

// setStatus
func (w *worker) setStatus(status int32) {
	atomic.StoreInt32(&w.status, status)
}

// isNeedStop
func (w *worker) isNeedStop() bool {
	// pool 已经关闭，停止运行
	if w.p.IsClosed() || atomic.LoadInt32(&w.status) >= WorkerStop {
		// pool 的 runningSize-1
		w.p.incrRunning(-1)
		return true
	}
	return false
}

// isNeedStop
func (w *worker) isRunning() bool {
	return atomic.LoadInt32(&w.status) <= WorkerRunning
}
