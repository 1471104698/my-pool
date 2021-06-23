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

// worker
type worker struct {
	p      *pool
	task   taskFunc
	status int32
}

// NewWorker 创建一个 worker
func NewWorker(p *pool, task taskFunc) *worker {
	return &worker{
		p:      p,
		task:   task,
		status: WorkerRunning,
	}
}

// run 执行任务
func (w *worker) run() {
	w.setStatus(WorkerRunning)
	// 开启一个 goroutine 执行任务
	go func() {

		// 对任务执行过程中发生的 panic 处理
		defer w.p.handlePanic()

		// 阻塞接收任务，chan 阻塞的是 G，不会影响到 M，M 仍然可以继续去跟其他的 G 进行绑定
		for {
			t := w.getTask()
			if t == nil {
				// 如果当前 worker 需要回收，那么结束运行
				if w.isRecycle() {
					w.setStatus(WorkerStop)
					return
				}

			} else {
				// 执行任务
				t()
				// 唤醒阻塞等待中的 Submit() goroutine
				w.signal()

				// 入队 workers，继续等待任务调度
				//w.p.addWorker(w)
			}
		}
	}()
}

// getTask 获取任务
func (w *worker) getTask() (t taskFunc) {
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

// getTask2 获取任务
func (w *worker) getTask2() (t taskFunc) {
	// 利用 select 来完成超时控制
	//select {
	//case t = <- w.task:
	//	return t
	//case <- time.After(time.Duration(w.freeTime) * time.Second):
	//
	//}
	//// 尝试从任务队列中获取任务
	//t = w.p.deTaskQueueTimeout(w.freeTime)
	//if t != nil {
	//	return t
	//}
	return nil
}

// isRecycle 判断是否需要进行回收
func (w *worker) isRecycle() bool {
	return w.isNeedStop() || w.p.RunningSize() > w.p.CoreSize()
}

// setStatus 设置 worker 状态
func (w *worker) setStatus(status int32) {
	atomic.StoreInt32(&w.status, status)
}

// isNeedStop 判断当前 worker 是否需要结束运行
func (w *worker) isNeedStop() bool {
	// pool 已经关闭，停止运行
	if w.p.IsClosed() || w.IsStop() {
		// pool 的 runningSize-1
		w.p.incrRunning(-1)
		return true
	}
	return false
}

// IsRunning 判断 worker 是否正在运行
func (w *worker) IsRunning() bool {
	return atomic.LoadInt32(&w.status) <= WorkerRunning
}

// IsStop 判断 worker 已经停止运行
func (w *worker) IsStop() bool {
	return atomic.LoadInt32(&w.status) >= WorkerStop
}

// signal 唤醒 Submit 等待的 goroutine
func (w *worker) signal() {
	if w.p.opts.isBlocking {
		select {
		case w.p.ch <- struct{}{}:
		case <-time.After(time.Nanosecond):
		}
	}
}
