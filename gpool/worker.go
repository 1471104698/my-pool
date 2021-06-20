package gpool

// worker
type worker struct {
	p    *pool
	task chan func()
}

// NewWorker
func NewWorker(p *pool) *worker {
	return &worker{
		p:    p,
		task: make(chan func(), 1),
	}
}

// run 执行任务
func (w *worker) run() {
	// len+1，表示当前存在的 worker 数+1
	w.p.incrLen(1)
	// 开启一个 goroutine 执行任务
	go func() {
		// 阻塞接收任务
		for t := range w.task {
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
