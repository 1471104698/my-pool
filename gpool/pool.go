package gpool

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultMaxSize Pool 默认容量
	DefaultMaxSize = 2000
	// DefaultCoreSize Pool 默认容量
	DefaultCoreSize = 1000
	// DefaultCleanStopWorkerTime 清理无效 worker 时间周期
	DefaultCleanStopWorkerTime = time.Second
	// worker 默认空闲时间
	DefaultFreeTime = 2

	// DefaultBlockingTime 默认最大的 Submit() 阻塞 goroutine 时长
	DefaultBlockingTime = 10 * time.Nanosecond
	// DefaultMaxBlockNum 默认最大阻塞数
	DefaultMaxBlockNum = 1000
)

// Pool 的状态
const (
	// Pool 正在运行
	Running = iota
	// Pool 已经关闭
	Closed
)

// 常用错误
var (
	poolClosedErr = fmt.Errorf("pool is closed")
	poolFullErr   = fmt.Errorf("pool is full")
)

// TaskFunc 任务类型
type TaskFunc = func()

// isFullFunc
type isFullFunc = func() bool

// Pool
type Pool struct {
	// 最大 worker 数
	maxSize int32
	// core worker 数
	coreSize int32
	// 正在运行的 worker 数
	runningSize int32
	// Pool 状态
	status int32
	// worker 空闲时间，单位为 time.Nanosecond
	freeTime int32
	// 允许阻塞的 Submit() 数
	blockSize int32

	// 全局锁
	lock sync.Locker
	// 用于控制 Pool 的阻塞和等待，目前尚未使用
	cond *sync.Cond

	// 控制 Submit() 唤醒
	ch chan struct{}

	// 这里实际上应该将 workers 做成一个接口，这样可以接收不同实现的任务队列
	workers PoolWorkers
	// 任务队列
	taskQueue *taskQueue

	// Pool 可选参数
	opts *Options
}

// NewPool 创建一个 Pool
func NewPool(core, max, freeTime int32, opts ...Option) *Pool {
	lock := newLocker()
	p := &Pool{
		maxSize:     max,
		coreSize:    core,
		freeTime:    freeTime,
		runningSize: 0,
		status:      Running,
		lock:        lock,
		cond:        sync.NewCond(lock),
		opts:        setOptions(opts),
		workers:     NewPoolWorkers(WorkerQueue, math.MaxInt32),
		taskQueue:   NewTaskQueue(-1),
	}
	p.taskQueue.p = p
	p.init()
	// 开启一个线程定时清除 无效 worker
	go p.cleanStopWorker()
	return p
}

// init 初始化 Pool 参数
func (p *Pool) init() {
	// 设置 maxSize
	if p.maxSize < 0 {
		p.maxSize = DefaultMaxSize
	}
	// 设置 coreSize
	if p.coreSize < 0 {
		p.coreSize = DefaultCoreSize
	}
	// 维护 core 和 max 的关系
	if p.coreSize > p.maxSize {
		p.coreSize = p.maxSize
	}
	// 设置 freeTime
	if p.freeTime <= 0 {
		p.freeTime = DefaultFreeTime
	}
	// 设置 blocking 相关参数
	if p.opts.isBlocking {
		if p.opts.blockingTime <= 0 {
			p.opts.blockingTime = DefaultBlockingTime
		}
		if p.opts.blockMaxNum <= 0 {
			p.opts.blockMaxNum = DefaultMaxBlockNum
		}
		p.ch = make(chan struct{}, p.opts.blockMaxNum)
	}
	// 设置 cleanTime
	if p.opts.cleanTime <= 0 {
		p.opts.cleanTime = DefaultCleanStopWorkerTime
	}
	// 设置拒绝策略
	if p.opts.rejectHandler == nil {
		p.opts.rejectHandler = defaultRejectHandler
	}
	// 设置 panic 处理器
	if p.opts.panicHandler == nil {
		p.opts.panicHandler = defaultPanicHandler
	}
	// 预分配处理
	if p.opts.isPreAllocation {
		if p.opts.allocationNum <= 0 || p.opts.allocationNum > p.coreSize {
			p.opts.allocationNum = p.coreSize
		}
		p.preAllocate()
	}
}

// Submit 任务提交
func (p *Pool) Submit(task TaskFunc) error {
	// 判断 Pool 是否已经关闭
	if p.IsClosed() {
		return poolClosedErr
	}

	// 尝试创建 core worker
	if !p.getWorker(p.isCoreFull, task) {
		// worker 数量达到了 core
		// 尝试将将任务放到任务队列中
		//if !p.enTaskQueue(task) {
		// 任务队列已满，尝试创建 非 core worker
		if !p.getWorker(p.isMaxFull, task) {
			// 创建失败，判断是否需要阻塞等待
			if p.isNeedBlocking() && p.blockWait(task) {
				return nil
			}
			// 执行拒绝策略
			if r := p.opts.rejectHandler; r != nil {
				return r(task)
			}
			// 没有拒绝策略，直接返回指定错误
			return poolFullErr
		}
		//}
		// 成功放入任务队列，直接返回
		return nil
	}

	// 这里有个问题，当塞任务的时候可能 worker 已经结束运行了，导致 deadlock
	// 这里拿到的 w 已经运行了 run()，直接塞任务即可
	//w.task <- task
	return nil
}

// IsRunning Pool 是否正在运行
func (p *Pool) IsRunning() bool {
	return atomic.LoadInt32(&p.status) <= Running
}

// IsClosed Pool 是否已经关闭
func (p *Pool) IsClosed() bool {
	// atomic.LoadInt32() 原子性的获取某个值
	return atomic.LoadInt32(&p.status) >= Closed
}

// RunningSize 获取已经存在的 worker 数
func (p *Pool) RunningSize() int32 {
	return atomic.LoadInt32(&p.runningSize)
}

// MaxSize 获取最大 worker 数
func (p *Pool) MaxSize() int32 {
	return atomic.LoadInt32(&p.maxSize)
}

// CoreSize 获取最大 core worker 数
func (p *Pool) CoreSize() int32 {
	return atomic.LoadInt32(&p.coreSize)
}

// BlockSize 获取 Submit() 阻塞 goroutine 数
func (p *Pool) BlockSize() int32 {
	return atomic.LoadInt32(&p.blockSize)
}

// SetCoreSize 动态设置 core worker 数
func (p *Pool) SetCoreSize(coreSize int32) {
	// 这里并不需要使用 CAS，多个 goroutine 同时设置最终也会有一个确定的值
	if coreSize > p.MaxSize() {
		return
	}
	atomic.StoreInt32(&p.coreSize, coreSize)
}

// SetMaxSize 动态设置 max worker 数
func (p *Pool) SetMaxSize(maxSize int32) {
	if maxSize < p.CoreSize() {
		return
	}
	atomic.StoreInt32(&p.maxSize, maxSize)
}

// Close 关闭 Pool
func (p *Pool) Close() {
	// 获取锁，保证并发安全，使得其他 goroutine 无法创建新的 worker
	p.lock.Lock()
	defer p.lock.Unlock()
	// 设置 Pool 为 关闭状态
	p.setStatus(Closed)
	// 扫描所有的 WorkersQueue 中所有的 worker，对于不在这里的 worker 在执行完任务后会自动退出
	p.workers.reset()
	// 清空任务队列的任务
	p.taskQueue.reset()
	// 关闭 chan
	if p.opts.isBlocking {
		close(p.ch)
	}
}

// Reboot 重启被关闭的 Pool
func (p *Pool) Reboot() {
	// 因为这里可能存在 goroutine 同时修改状态，而我们需要启动一个清除 stop worker 的 goroutine
	// 因此为了避免启动多余的 goroutine 以及多次初始化 ch
	// 使用 CAS，如果修改成功，那么由当前 goroutine 去开启 clean goroutine 以及关闭 ch ，失败的话表示已经有其他的 goroutine 开启了
	if atomic.CompareAndSwapInt32(&p.status, Closed, Running) {
		go p.cleanStopWorker()
		if p.opts.isBlocking {
			p.ch = make(chan struct{}, p.opts.blockMaxNum)
		}
	}
}

// preAllocate 预创建 worker
func (p *Pool) preAllocate() {
	for i := 0; i < int(p.opts.allocationNum); i++ {
		p.newWorkerWithQueue(nil)
	}
}

// handlePanic
func (p *Pool) handlePanic() {
	if err := recover(); err != nil {
		if h := p.opts.panicHandler; h != nil {
			h(err.(error))
		} else {
			panic(err)
		}
	}
}

// cleanStopWorker
func (p *Pool) cleanStopWorker() {
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
		l := int(p.workers.Len())
		for i := 0; i < l/4; i++ {
			idx := rand.Intn(l)
			p.workers.checkWorker(int32(idx))
		}
	}
}

// newLocker 获取一把锁
func newLocker() sync.Locker {
	return &sync.Mutex{}
}

// isMaxFull 判断是否已经存在 maxSize 个 worker
func (p *Pool) isMaxFull() bool {
	return p.RunningSize() >= p.maxSize
}

// isCoreFull 判断是否已经存在 coreSize 个 worker
func (p *Pool) isCoreFull() bool {
	return p.RunningSize() >= p.coreSize
}

// incrRunning runningSize+i
func (p *Pool) incrRunning(i int32) {
	atomic.AddInt32(&p.runningSize, i)
}

// decrRunning runningSize-i
func (p *Pool) decrRunning(i int32) {
	atomic.AddInt32(&p.runningSize, -i)
}

// isNeedBlocking 判断当前 Submit() goroutine 是否需要阻塞
func (p *Pool) isNeedBlocking() bool {
	return p.opts.isBlocking && p.BlockSize() < p.opts.blockMaxNum
}

// setStatus 设置 Pool 状态
func (p *Pool) setStatus(status int32) {
	atomic.StoreInt32(&p.status, status)
}

// getWorker 获取 workder 的逻辑
func (p *Pool) getWorker(isFull isFullFunc, task TaskFunc) bool {
	// 这里由 WorkersQueue 自己保证并发安全
	//w, _ = p.WorkersQueue.Remove()
	//if w != nil {
	//	return w
	//}

	// 这里需要加锁，因为 isFullFunc() 的判断虽然是原子性的，但是它跟下面的创建 worker 的操作合在一起并不是原子性的
	// 比如当前 Pool 还有一个空余位置，同时来了两个 goroutine
	// isFullFunc() 的判断是原子性的，此时的判断不涉及到 worker 添加，因此对于这两个 goroutine 来说返回的就是 false
	// 那么它们都会同时执行下面的创建 worker 的逻辑，导致创建的 worker 数超过了限制范围
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.IsClosed() {
		return false
	}
	// 当前是否能够创建新的 worker
	if isFull() {
		return false
	}
	// 创建一个新的 worker，并将任务交给该 worker
	// 这里不能将 worker 入队，因为 WorkersQueue 内部的 worker 存储的是空闲等待任务的，而这里新创建的是需要去执行任务的
	p.newWorkerWithQueue(task)
	return true
}

// addWorker 添加 worker 到 WorkersQueue 队列
func (p *Pool) addWorker(w Worker) {
	if p.IsClosed() || w.IsStop() {
		return
	}
	p.workers.Put(w)
}

// newWorkerWithQueue 创建一个新的 WorkerWithQueue 类型的 worker
func (p *Pool) newWorkerWithQueue(task TaskFunc) {
	// 让 worker 先开始运行等待任务
	w := newWorkerWithQueue(p, task)
	w.run(w)
	p.addWorker(w)
	// runningSize+1，表示当前存在的 worker 数+1
	p.incrRunning(1)
}

// enTaskQueue 将任务存储到任务队列
func (p *Pool) enTaskQueue(task TaskFunc) bool {
	if p.IsClosed() {
		return false
	}
	return p.taskQueue.Add(task)
}

// deTaskQueueTimeout 从任务队列中取任务，超时等待
func (p *Pool) deTaskQueueTimeout(timeout int32) (task TaskFunc) {
	if p.IsClosed() {
		return nil
	}
	// 超时获取任务
	return p.taskQueue.PollWithTimeout(timeout, time.Nanosecond)
}

// deTaskQueue 从任务队列中取任务，拿不到直接返回 nil
func (p *Pool) deTaskQueue() (task TaskFunc) {
	if p.IsClosed() {
		return nil
	}
	return p.taskQueue.Poll()
}

// blockWait 阻塞 Submit() goroutine，直到超时或者任务提交到任务队列成功
func (p *Pool) blockWait(task TaskFunc) bool {
	// 加锁
	p.lock.Lock()
	defer p.lock.Unlock()
	// 阻塞数+1
	p.blockSize++
	// 计算超时到期时间
	endTime := time.Now().Add(p.opts.blockingTime)
	// 生产者-消费者，任务入队失败并且 Pool 还没有关闭，那么进行阻塞
	for !p.enTaskQueue(task) && p.IsRunning() {
		// 获取剩余超时时间
		remaining := endTime.Sub(time.Now())
		// 没有剩余超时时间，直接返回
		if remaining <= 0 {
			// 阻塞数-1
			p.blockSize--
			return false
		}
		// 利用 chan+select 进行超时等待
		select {
		case <-p.ch:
		case <-time.After(remaining):
		}
	}
	// 阻塞数-1
	p.blockSize--
	return true
}
