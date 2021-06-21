package gpool

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// pool 默认容量
	DefaultMaxSize = 64
	// pool 默认容量
	DefaultCoreSize = 16
	// 清理无效 worker 时间周期
	DefaultCleanStopWorkerTime = time.Second
)

// pool 的状态
const (
	// pool 正在运行
	Running = iota
	// pool 已经关闭
	Closed
)

// 常用错误
var (
	poolClosedErr = fmt.Errorf("pool is closed")
	poolFullErr   = fmt.Errorf("pool is full")
)

// isFullFunc
type isFullFunc = func() bool

// pool
type pool struct {
	maxSize     int32
	coreSize    int32
	runningSize int32
	status      int32
	freeTime    int32

	lock sync.Locker
	cond *sync.Cond

	// 这里实际上应该将 workers 做成一个接口，这样可以接收不同实现的任务队列
	workers   *workers
	taskQueue *taskQueue

	opts *Options
}

// NewPool
func NewPool(core, max, freeTime int32, opts ...Option) *pool {
	p := &pool{
		maxSize:     max,
		coreSize:    core,
		freeTime:    freeTime,
		runningSize: 0,
		status:      Running,
		lock:        newLocker(),
		cond:        sync.NewCond(newLocker()),
		opts:        setOptions(opts),
		workers:     NewWorkers(-1),
		taskQueue:   NewTaskQueue(-1),
	}
	p.init()
	// 开启一个线程定时清除 无效 worker
	go p.cleanStopWorker()
	return p
}

// init
func (p *pool) init() {
	if p.maxSize < 0 {
		p.maxSize = DefaultMaxSize
	}
	if p.coreSize < 0 {
		p.coreSize = DefaultCoreSize
	}
	if p.coreSize > p.maxSize {
		p.coreSize = p.maxSize
	}

	if p.opts.cleanTime <= 0 {
		p.opts.cleanTime = DefaultCleanStopWorkerTime
	}

	if p.opts.rejectHandler == nil {
		p.opts.rejectHandler = defaultRejectHandler
	}
	if p.opts.panicHandler == nil {
		p.opts.panicHandler = defaultPanicHandler
	}
	if p.opts.logger == nil {
		p.opts.logger = defaultLogger
	}
}

// cleanStopWorker
func (p *pool) cleanStopWorker() {
	// NewTimer(d) (*Timer) 创建一个 Timer，内部维护了一个 chan Time 类型的 C 字段，它会在过去时间段 d 后，向其自身的 C 字段发送当时的时间，只有一次触发机会
	// NewTicker 返回一个新的 Ticker，该 Ticker 内部维护了一个 chan Time 类型的 C 字段，并会每隔时间段 d 就向该通道发送当时的时间。即有多次触发机会
	ticker := time.NewTicker(p.opts.cleanTime)
	// 注意停止该 ticker
	defer ticker.Stop()
	// 坑点：这里是为了随机数处理，用来初始化随机变量，如果没有初始化，那么 rand.Intn() 得到的都是固定的值，而非一个随机值
	rand.Seed(time.Now().UnixNano())
	for range ticker.C {
		if p.IsClosed() {
			return
		}
		// 每次随机扫描 len/4 个随机位置的 worker，如果过期了那么进行移除
		l := int(p.workers.len)
		for i := 0; i < l/4; i++ {
			idx := rand.Intn(l)
			p.workers.checkWorker(int32(idx))
		}
	}
}

// Submit 任务提交
func (p *pool) Submit(task taskFunc) error {
	// 接收到一个任务，此时应该怎么做？
	// 判断 pool 是否已经关闭
	if p.IsClosed() {
		return poolClosedErr
	}

	// 对任务执行过程中发生的 panic 处理
	defer func() {
		if err := recover(); err != nil {
			if h := p.opts.panicHandler; h != nil {
				h(err.(error))
			} else {
				panic(err)
			}
		}
	}()

	// 获取 worker 来执行任务
	w := p.getWorker(p.isCoreFull, task)
	// worker 数量达到了 core
	if w == nil {
		// 将任务放到任务队列中
		if !p.enTaskQueue(task) {
			// 任务队列已满，那么创建 非 core worker
			w = p.getWorker(p.isMaxFull, task)
			if w == nil {
				// 执行拒绝策略
				if r := p.opts.rejectHandler; r != nil {
					return r(task)
				}
				return poolFullErr
			}
		}
		return nil
	}

	// 这里有个问题，当塞任务的时候可能 worker 已经结束运行了，导致 deadlock
	// 这里拿到的 w 已经运行了 run()，直接塞任务即可
	//w.task <- task
	return nil
}

// IsRunning
func (p *pool) IsRunning() bool {
	return atomic.LoadInt32(&p.status) <= Running
}

// IsRunning
func (p *pool) IsClosed() bool {
	// atomic.LoadInt32() 原子性的获取某个值
	return atomic.LoadInt32(&p.status) >= Closed
}

// RunningSize
func (p *pool) RunningSize() int32 {
	return atomic.LoadInt32(&p.runningSize)
}

// MaxSize
func (p *pool) MaxSize() int32 {
	return p.maxSize
}

// CoreSize
func (p *pool) CoreSize() int32 {
	return p.coreSize
}

func (p *pool) FreeSize() int32 {
	return p.MaxSize() - p.RunningSize()
}

// Close
func (p *pool) Close() {
	// 获取锁，使得其他 goroutine 无法创建新的 worker
	p.lock.Lock()
	defer p.lock.Unlock()
	// 扫描所有的 workers 中所有的 worker，对于不在这里的 worker 在执行完任务后会自动退出
	p.setStatus(Closed)
	p.workers.reset()
	// 清空任务队列的任务
	p.taskQueue.reset()
}

// Reboot 重启被关闭的 pool
func (p *pool) Reboot() {
	// 因为这里可能存在 goroutine 同时修改状态，而我们需要启动一个清除 stop worker 的 goroutine
	// 因此为了避免启动多余的线程，这里使用 CAS，如果修改成功，那么由当前 goroutine 去开启 clean goroutine，失败的话表示已经有其他的 goroutine 开启了
	if atomic.CompareAndSwapInt32(&p.status, Closed, Running) {
		go p.cleanStopWorker()
	}
}

// ---------------------------------------------------------------------------------------------------

// newLocker
func newLocker() sync.Locker {
	return &sync.Mutex{}
}

// isMaxFull
func (p *pool) isMaxFull() bool {
	return p.RunningSize() >= p.maxSize
}

// isCoreFull
func (p *pool) isCoreFull() bool {
	return p.RunningSize() >= p.coreSize
}

// incrRunning
func (p *pool) incrRunning(i int32) {
	atomic.AddInt32(&p.runningSize, i)
}

// setStatus
func (p *pool) setStatus(status int32) {
	atomic.StoreInt32(&p.status, status)
}

// getWorker
// 获取 workder 的逻辑，比如目前的 worker 数是否已经超过了容量，获取成功了怎么做，获取失败了怎么做
func (p *pool) getWorker(isFull isFullFunc, task taskFunc) (w *worker) {
	// 这里由 workers 自己保证并发安全
	//w, _ = p.workers.Remove()
	//if w != nil {
	//	return w
	//}

	// 这里需要加锁，因为 isFullFunc() 的判断虽然是原子性的，但是它跟下面的创建 worker 的操作合在一起并不是原子性的
	// 比如当前 pool 还有一个空余位置，同时来了两个 goroutine
	// isFullFunc() 的判断是原子性的，此时的判断不涉及到 worker 添加，因此对于这两个 goroutine 来说返回的就是 false
	// 那么它们都会同时执行下面的创建 worker 的逻辑，导致创建的 worker 数超过了限制范围
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.IsClosed() {
		return nil
	}

	// 当前是否能够创建新的 worker
	if isFull() {
		return nil
	}

	// 创建一个新的 worker
	w = NewWorker(p, task)
	p.addWorker(w)
	// 让 worker 先开始运行等待任务
	w.run()
	// runningSize+1，表示当前存在的 worker 数+1
	p.incrRunning(1)
	// 这里不能将 worker 入队，因为 workers 内部的 worker 存储的是空闲等待任务的，而这里新创建的是需要去执行任务的
	return w
}

// addWorker
func (p *pool) addWorker(w *worker) {
	if p.IsClosed() || w.IsStop() {
		return
	}
	p.workers.Put(w)
}

// enTaskQueue
func (p *pool) enTaskQueue(task taskFunc) bool {
	if p.IsClosed() {
		return false
	}
	return p.taskQueue.Add(task)
}

// enTaskQueue
func (p *pool) deTaskQueue(timeout int32) (task taskFunc) {
	if p.IsClosed() {
		return nil
	}
	return p.taskQueue.PollWithTimeout(timeout, time.Nanosecond)
}
