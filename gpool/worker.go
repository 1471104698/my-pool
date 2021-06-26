package gpool

import (
	"sync/atomic"
	"time"
)

// worker 状态
const (
	WorkerRunning = iota
	WorkerFree
	WorkerStop
)

// worker 类型
const (
	WorkerWithQueue = iota
	WorkerWithChan
)

// Worker worker 接口
type Worker interface {
	run(Worker)
	getTask() (t TaskFunc)
	needExit() bool
	doAfter()
	setStatus(status int32)
	IsRunning() bool
	IsStop() bool
}

// worker 抽取封装的公共 worker
type worker struct {
	Worker
	p      *Pool
	status int32
}

// run 执行任务
// 这里传入 wi Worker 的原因是因为我们外部创建的是 内嵌了 worker 的 workerWithQueue 或者 workerWithChan
// 但也只是相当于它们拥有了 worker 的变量和方法，这里我们使用 worker 来抽取出公共的逻辑，但是对于一些特殊的需要由 workerWithQueue 和 workerWithChan 的方法调用
// 由于 Go 的设计问题，我们不能直接使用 w 来进行调用，因为它并不会去识别这是 workerWithQueue 还是 workerWithChan，它会直接调用 worker 的方法
// 所以我们这里需要再传入对应的实现，然后使用该实现去调用这些特殊的方法
func (w *worker) run(wi Worker) {
	w.setStatus(WorkerRunning)
	// 开启一个 goroutine 执行任务
	go func() {

		// 对任务执行过程中发生的 panic 处理
		defer w.p.handlePanic()

		// 阻塞接收任务，chan 阻塞的是 G，不会影响到 M，M 仍然可以继续去跟其他的 G 进行绑定
		for {
			t := wi.getTask()
			if t == nil {
				// 如果当前 worker 需要回收，那么结束运行
				if wi.needExit() {
					w.p.decrRunning(1)
					return
				}
			} else {
				// 执行任务
				t()
				// 唤醒阻塞等待中的 Submit() goroutine
				wi.doAfter()
			}
		}
	}()
}

// setStatus 设置 worker 状态
func (w *worker) setStatus(status int32) {
	atomic.StoreInt32(&w.status, status)
}

// IsRunning 判断 worker 是否正在运行
func (w *worker) IsRunning() bool {
	return atomic.LoadInt32(&w.status) <= WorkerRunning
}

// IsStop 判断 worker 已经停止运行
func (w *worker) IsStop() bool {
	return atomic.LoadInt32(&w.status) >= WorkerStop
}

// workerWithQueue 从任务队列中获取任务的 worker
type workerWithQueue struct {
	*worker
	task TaskFunc
}

// newWorkerWithQueue 创建一个 worker queue
func newWorkerWithQueue(p *Pool, task TaskFunc) *workerWithQueue {
	return &workerWithQueue{
		worker: &worker{
			p:      p,
			status: WorkerRunning,
		},
		task: task,
	}
}

// getTask 获取任务
func (w *workerWithQueue) getTask() (t TaskFunc) {
	if w.task != nil {
		t = w.task
		w.task = nil
		return t
	}
	// 尝试从任务队列中获取任务
	t = w.p.deTaskQueueTimeout(w.p.freeTime)
	if t != nil {
		return t
	}
	return nil
}

// doNotTask 没有获取任务后的操作
func (w *workerWithQueue) needExit() bool {
	if w.isNeedRecycle() {
		w.setStatus(WorkerStop)
		return true
	}
	return false
}

// doAfter 任务执行完成后进行的操作
func (w *workerWithQueue) doAfter() {
	w.signal()
}

// isNeedRecycle 判断是否需要进行回收
func (w *workerWithQueue) isNeedRecycle() bool {
	return w.isNeedStop() || w.p.RunningSize() > w.p.CoreSize()
}

// isNeedStop 判断当前 worker 是否需要结束运行
func (w *workerWithQueue) isNeedStop() bool {
	// Pool 已经关闭，停止运行
	if w.p.IsClosed() || w.IsStop() {
		// Pool 的 runningSize-1
		w.p.incrRunning(-1)
		return true
	}
	return false
}

// signal 唤醒 Submit 等待的 goroutine
func (w *workerWithQueue) signal() {
	if w.p.opts.isBlocking && w.p.IsRunning() {
		select {
		case w.p.ch <- struct{}{}:
		case <-time.After(time.Nanosecond):
		}
	}
}

// workerWithChan 从 chan 中获取任务的 worker
type workerWithChan struct {
	*worker
	task chan TaskFunc
}

// newWorkerWithChan 创建一个 worker chan
func newWorkerWithChan(p *Pool) *workerWithChan {
	return &workerWithChan{
		worker: &worker{
			p:      p,
			status: WorkerRunning,
		},
		task: make(chan TaskFunc),
	}
}

// getTask 获取任务
func (w *workerWithChan) getTask() TaskFunc {
	for t := range w.task {
		if t == nil {
			return nil
		}
		return t
	}
	return nil
}

// needExit 没有获取任务后的操作
func (w *workerWithChan) needExit() bool {
	return true
}

// doAfter 任务执行完成后进行的操作
func (w *workerWithChan) doAfter() {
	//将 worker 添加到 workers 队列中
	w.p.addWorker(w)
}
