package gpool

import (
	"fmt"
	"sync/atomic"
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

// chan 关闭异常处理
var chanClosePanicHandler = func() {
	if err := recover(); err != nil {
		fmt.Println("ch is closed")
	}
}

// Worker worker 接口
type Worker interface {
	IsRunning() bool        // 当前 worker 是否在运行
	IsStop() bool           // 当前 worker 是否停止运行
	Close()                 // 关闭当前 worker
	run(Worker)             // 执行任务
	setTask(t TaskFunc)     // 设置任务，pool 不关心怎么将任务交给 worker，由 worker 自己去决定
	getTask() (t TaskFunc)  // 获取任务
	needExit() bool         // 当前 worker 是否需要退出
	notifyExit()            // 通知退出函数，因为不同类型的 worker 的退出逻辑是不一样的
	doAfter()               // doAfter 任务执行完成后进行的操作
	setStatus(status int32) // 设置状态
}

// worker 抽取封装的公共 worker
type worker struct {
	Worker
	p      GoroutinePool
	status int32
}

// newWorker
func newWorker(p GoroutinePool) *worker {
	return &worker{
		p:      p,
		status: WorkerRunning,
	}
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
					wi.Close()
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

// Close 关闭该 worker 的逻辑，对该类型来说不需要任何处理
func (w *workerWithQueue) Close() {}

// newWorkerWithQueue 创建一个 worker queue
func newWorkerWithQueue(p GoroutinePool) *workerWithQueue {
	return &workerWithQueue{
		worker: newWorker(p),
	}
}

// getTask 获取任务
func (w *workerWithQueue) getTask() (t TaskFunc) {
	if w.task != nil {
		t = w.task
		w.task = nil
		return t
	}
	// 如果 worker 已经停止，那么不获取任务
	if w.IsStop() {
		return nil
	}
	// 尝试从任务队列中获取任务
	t = w.p.deTaskQueueTimeout()
	if t != nil {
		return t
	}
	return nil
}

// setTask 设置任务，pool 不关心怎么将任务交给 worker，由 worker 自己去决定
func (w *workerWithQueue) setTask(t TaskFunc) {
	if w.IsStop() {
		return
	}
	w.task = t
}

// needExit 当前 worker 是否需要退出，对于该类型的 worker，如果没有获取到任务那么判断是否需要回收
// ✳✳✳✳✳✳✳✳✳✳✳✳✳这里有个想法，可以将任务的获取逻辑和退出逻辑做成两个组件，这样的话可以自由拼接 ✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳✳
func (w *workerWithQueue) needExit() bool {
	if w.isNeedRecycle() {
		w.setStatus(WorkerStop)
		return true
	}
	return false
}

// notifyExit 通知 worker 退出，该类型的 worker 只需要设置状态，当获取任务的时候发现已经退出了那么就不会再执行任务
func (w *workerWithQueue) notifyExit() {
	w.setStatus(WorkerStop)
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
	// pool 已经关闭，停止运行
	if w.p.IsClosed() || w.IsStop() {
		// pool 的 runningSize-1
		w.p.incrRunning(-1)
		return true
	}
	return false
}

// signal 唤醒 Submit 等待的 goroutine
func (w *workerWithQueue) signal() {
	// 处理 chan 已经关闭时产生的异常，因为这里 判断 IsRunning() 以及下面的操作并不是原子性的
	// 所以可能前一时刻 IsRunning() return true，下一时刻 pool 已经关闭了 chan
	defer chanClosePanicHandler()
	if w.p.isBlocking() && w.p.IsRunning() {
		w.p.signal()
	}
}

// workerWithChan 从 chan 中获取任务的 worker
type workerWithChan struct {
	*worker
	task chan TaskFunc
}

// newWorkerWithChan 创建一个 worker chan
func newWorkerWithChan(p GoroutinePool) *workerWithChan {
	return &workerWithChan{
		worker: newWorker(p),
		task:   make(chan TaskFunc),
	}
}

// Close 关闭该 worker 的逻辑，需要将 ch 关闭
func (w *workerWithChan) Close() {
	close(w.task)
}

// getTask 获取任务
func (w *workerWithChan) getTask() TaskFunc {
	// 从 chan 中读取任务，如果没有任务那么进行阻塞
	for t := range w.task {
		if t == nil {
			return nil
		}
		return t
	}
	return nil
}

// setTask 设置任务，pool 不关心怎么将任务交给 worker，由 worker 自己去决定
func (w *workerWithChan) setTask(t TaskFunc) {
	if w.IsStop() {
		return
	}
	w.task <- t
}

// needExit 当前 worker 是否需要退出
func (w *workerWithChan) needExit() bool {
	return w.IsStop()
}

// notifyExit 通知退出逻辑，往 task 中传 nil，一旦读取到 nil，那么该 worker 会退出
func (w *workerWithChan) notifyExit() {
	// 处理 ch 已经关闭导致的 panic
	defer chanClosePanicHandler()
	w.setStatus(WorkerStop)
	w.task <- nil
}

// doAfter 任务执行完成后进行的操作
func (w *workerWithChan) doAfter() {
	//将 worker 添加到 workers 队列中，继续等待获取任务
	w.p.addWorker(w)
	// 唤醒在等待的 Submit goroutine
	w.p.signal()
}
