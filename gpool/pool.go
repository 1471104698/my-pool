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
	// DefaultMaxSize pool 默认 max 容量
	DefaultMaxSize = 1000
	// DefaultCoreSize pool 默认 core 容量
	DefaultCoreSize = 500
	// DefaultCleanStopWorkerTime 清理无效 worker 时间周期
	DefaultCleanStopWorkerTime = time.Second
	// DefaultFreeTime worker 默认空闲时间
	DefaultFreeTime = 2

	// DefaultBlockingTime 默认最大的 Submit 阻塞 goroutine 时长
	DefaultBlockingTime = 10 * time.Nanosecond
	// DefaultMaxBlockNum 默认最大阻塞数
	DefaultMaxBlockNum = 1000
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

// TaskFunc 任务类型
type TaskFunc = func()

// isFullFunc
type isFullFunc = func() bool

//  GoroutinePool
type GoroutinePool interface {
	Submit(p GoroutinePool, task TaskFunc) error // 任务提交
	Do(task TaskFunc) bool                       // 每个 pool 任务处理逻辑
	Close()                                      // 关闭 pool
	IsRunning() bool                             // 判断 pool 是否运行中
	IsClosed() bool                              // 判断 pool 是否已经关闭
	Reboot()                                     // 重启 pool
	signal()                                     // 唤醒等待的 Submit goroutine
	RunningSize() int32                          // 获取已经存在的 worker 数
	MaxSize() int32                              // 获取最大 worker 数
	CoreSize() int32                             // 获取最大 core worker 数
	BlockSize() int32                            // 获取 Submit 阻塞 goroutine 数
	SetCoreSize(coreSize int32)                  // 动态设置 core worker 数
	SetMaxSize(maxSize int32)                    // 动态设置 max worker 数
	isMaxFull() bool                             // 判断是否已经存在 maxSize 个 worker
	isCoreFull() bool                            // 判断是否已经存在 coreSize 个 worker
	incrRunning(i int32)                         // runningSize+i
	decrRunning(i int32)                         // runningSize-i
	isBlocking() bool                            // 是否开启了阻塞
	isNeedBlocking() bool                        // 判断当前 Submit goroutine 是否需要阻塞
	setStatus(status int32)                      // 设置 pool 状态
	preAllocate(p1 GoroutinePool)                // 预创建 worker 逻辑
	handlePanic()                                // panic 处理逻辑
	handleReject(task TaskFunc) error            // 执行拒绝测试
	addWorker(t Worker)                          // 添加 worker 到 workers
	enTaskQueue(task TaskFunc) bool              // 任务入队
	deTaskQueue() (task TaskFunc)                // 任务获取
	deTaskQueueTimeout() (task TaskFunc)         // 任务超时获取
}

// pool
type pool struct {
	GoroutinePool

	// 最大 worker 数
	maxSize int32
	// core worker 数
	coreSize int32
	// 正在运行的 worker 数
	runningSize int32
	// pool 状态
	status int32
	// worker 空闲时间，单位为 time.Nanosecond
	freeTime int32
	// 允许阻塞的 Submit 数
	blockSize int32

	// worker 类型
	workerType int32

	// 全局锁
	lock sync.Locker
	// 用于控制 pool 的阻塞和等待
	cond *sync.Cond
	// 用于超时控制
	ch chan struct{}

	// worker 列表
	workers PoolWorkers
	// 任务队列
	taskQueue *taskQueue

	// pool 可选参数
	opts *Options
}

// newPool 创建一个 pool
func newPool(core, max, freeTime int32, opts ...Option) *pool {
	lock := newLocker()
	p := &pool{
		maxSize:     max,
		coreSize:    core,
		freeTime:    freeTime,
		runningSize: 0,
		status:      Running,
		lock:        lock,
		cond:        sync.NewCond(lock),
		opts:        setOptions(opts),
		workers:     NewPoolWorkers(WorkerQueue, math.MaxInt32),
		taskQueue:   NewTaskQueue(DefaultTaskQueueCap),
	}
	p.init()
	return p
}

// NewPool1
func NewPool1(core, max, freeTime int32, opts ...Option) GoroutinePool {
	p := &pool1{
		pool: newPool(core, max, freeTime, opts...),
	}
	p.preAllocate(p)
	p.pool.workerType = WorkerWithQueue
	// 开启一个线程定时清除 无效 worker
	go p.cleanStopWorker()
	return p
}

// NewPool2
func NewPool2(core, max, freeTime int32, opts ...Option) GoroutinePool {
	p := &pool2{
		pool: newPool(core, max, freeTime, opts...),
	}
	p.preAllocate(p)
	p.pool.workerType = WorkerWithChan
	return p
}

// init 初始化 pool 参数
func (p *pool) init() {
	// 设置 maxSize
	if p.maxSize <= 0 {
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
	}
}

// Submit 任务提交
func (*pool) Submit(p GoroutinePool, task TaskFunc) error {
	// 判断 pool 是否已经关闭
	if p.IsClosed() {
		return poolClosedErr
	}
	if !p.Do(task) {
		// 执行拒绝策略
		return p.handleReject(task)
	}
	return nil
}

// IsRunning pool 是否正在运行
func (p *pool) IsRunning() bool {
	return atomic.LoadInt32(&p.status) <= Running
}

// IsClosed pool 是否已经关闭
func (p *pool) IsClosed() bool {
	// atomic.LoadInt32() 原子性的获取某个值
	return atomic.LoadInt32(&p.status) >= Closed
}

// RunningSize 获取已经存在的 worker 数
func (p *pool) RunningSize() int32 {
	return atomic.LoadInt32(&p.runningSize)
}

// MaxSize 获取最大 worker 数
func (p *pool) MaxSize() int32 {
	return atomic.LoadInt32(&p.maxSize)
}

// CoreSize 获取最大 core worker 数
func (p *pool) CoreSize() int32 {
	return atomic.LoadInt32(&p.coreSize)
}

// BlockSize 获取 Submit 阻塞 goroutine 数
func (p *pool) BlockSize() int32 {
	return atomic.LoadInt32(&p.blockSize)
}

// SetCoreSize 动态设置 core worker 数
func (p *pool) SetCoreSize(coreSize int32) {
	// 这里并不需要使用 CAS，多个 goroutine 同时设置最终也会有一个确定的值
	if coreSize > p.MaxSize() {
		return
	}
	atomic.StoreInt32(&p.coreSize, coreSize)
}

// SetMaxSize 动态设置 max worker 数
func (p *pool) SetMaxSize(maxSize int32) {
	if maxSize < p.CoreSize() {
		return
	}
	atomic.StoreInt32(&p.maxSize, maxSize)
}

// newLocker 获取一把锁
func newLocker() sync.Locker {
	return &sync.Mutex{}
}

// isMaxFull 判断是否已经存在 maxSize 个 worker
func (p *pool) isMaxFull() bool {
	return p.RunningSize() >= p.maxSize
}

// isCoreFull 判断是否已经存在 coreSize 个 worker
func (p *pool) isCoreFull() bool {
	return p.RunningSize() >= p.coreSize
}

// incrRunning runningSize+i
func (p *pool) incrRunning(i int32) {
	atomic.AddInt32(&p.runningSize, i)
}

// decrRunning runningSize-i
func (p *pool) decrRunning(i int32) {
	atomic.AddInt32(&p.runningSize, -i)
}

// isBlocking 是否开启了阻塞
func (p *pool) isBlocking() bool {
	return p.opts.isBlocking
}

// isNeedBlocking 判断当前 Submit goroutine 是否需要阻塞
func (p *pool) isNeedBlocking() bool {
	return p.isBlocking() && p.BlockSize() < p.opts.blockMaxNum
}

// setStatus 设置 pool 状态
func (p *pool) setStatus(status int32) {
	atomic.StoreInt32(&p.status, status)
}

// giveTaskToWorker 将任务交给 worker 去处理
func (p *pool) giveTaskToWorker(p1 GoroutinePool, isFull isFullFunc, task TaskFunc, getInWorkers bool) bool {
	// 从 workers 中获取一个空闲 worker，这里由 workers 自己保证并发安全
	var w Worker
	if getInWorkers {
		w, _ = p.workers.Remove()
		if w != nil {
			w.setTask(task)
			return true
		}
	}
	// 这里需要加锁，因为 isFullFunc() 的判断虽然是原子性的，但是它跟下面的创建 worker 的操作合在一起并不是原子性的
	// 比如当前 pool 还有一个空余位置，同时来了两个 goroutine
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
	w = p.newWorker(p1)
	if w == nil {
		return false
	}
	// 将任务交给 worekr
	w.setTask(task)
	p.incrRunning(1)
	return true
}

// preAllocate 预创建 worker
func (p *pool) preAllocate(p1 GoroutinePool) {
	for i := 0; i < int(p.opts.allocationNum); i++ {
		p.newWorker(p1)
	}
}

// handlePanic panic 处理
func (p *pool) handlePanic() {
	if err := recover(); err != nil {
		if h := p.opts.panicHandler; h != nil {
			h(err.(error))
		} else {
			panic(err)
		}
	}
}

// handleReject 执行拒绝策略
func (p *pool) handleReject(task TaskFunc) error {
	if h := p.opts.rejectHandler; h != nil {
		return h(task)
	}
	return poolFullErr
}

// addWorker 添加 worker 到 WorkersQueue 队列
func (p *pool) addWorker(w Worker) {
	if p.IsClosed() || w.IsStop() {
		return
	}
	p.workers.Put(w)
}

// newWorkerWithQueue 创建一个新的 WorkerWithQueue 类型的 worker
func (p *pool) newWorker(p1 GoroutinePool) Worker {
	var w Worker
	switch p.workerType {
	case WorkerWithQueue:
		w = newWorkerWithQueue(p1)
		p.addWorker(w)
	case WorkerWithChan:
		w = newWorkerWithChan(p1)
	default:

	}
	if w != nil {
		// 让 worker 先开始运行等待任务
		w.run(w)
	}
	return w
}

// enTaskQueue 将任务存储到任务队列
func (p *pool) enTaskQueue(task TaskFunc) bool {
	if p.IsClosed() {
		return false
	}
	return p.taskQueue.Add(task)
}

// deTaskQueueTimeout 从任务队列中取任务，超时等待
func (p *pool) deTaskQueueTimeout() (task TaskFunc) {
	if p.IsClosed() {
		return nil
	}
	// 超时获取任务
	return p.taskQueue.PollWithTimeout(p.freeTime, time.Nanosecond)
}

// deTaskQueue 从任务队列中取任务，拿不到直接返回 nil
func (p *pool) deTaskQueue() (task TaskFunc) {
	if p.IsClosed() {
		return nil
	}
	return p.taskQueue.Poll()
}

// pool1
type pool1 struct {
	*pool
}

// Do 任务处理逻辑
func (p *pool1) Do(task TaskFunc) bool {
	// 判断 pool 是否已经关闭
	if p.IsClosed() {
		return false
	}
	// 尝试创建 core worker
	if !p.giveTaskToWorker(p, p.isCoreFull, task, false) {
		// worker 数量达到了 core
		// 尝试将将任务放到任务队列中
		if !p.enTaskQueue(task) {
			// 任务队列已满，尝试创建 非 core worker
			if !p.giveTaskToWorker(p, p.isMaxFull, task, false) {
				// 创建失败，判断是否需要阻塞等待
				if p.isNeedBlocking() {
					return p.block(task)
				}
				return false
			}
		}
		return true
	}
	return true
}

// block 阻塞 Submit goroutine，直到超时或者任务提交到任务队列成功
func (p *pool1) block(task TaskFunc) bool {
	// 加锁
	p.lock.Lock()
	defer p.lock.Unlock()
	// 阻塞数+1
	p.blockSize++
	// 计算超时到期时间
	endTime := time.Now().Add(p.opts.blockingTime)
	// 生产者-消费者，任务入队失败并且 pool 还没有关闭，那么进行阻塞
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

// signal 唤醒 Submit 等待的 goroutine
func (p *pool1) signal() {
	select {
	case p.ch <- struct{}{}:
	case <-time.After(time.Nanosecond):
	}
}

// Close 关闭 pool
func (p *pool1) Close() {
	// 获取锁，保证并发安全，使得其他 goroutine 无法创建新的 worker
	p.lock.Lock()
	defer p.lock.Unlock()
	// 设置 pool 为 关闭状态
	p.setStatus(Closed)
	// 清空 workers，通知所有 worker 停止运行
	p.workers.reset()
	// 清空任务队列的任务
	p.taskQueue.reset()
	// 关闭 chan
	if p.opts.isBlocking {
		close(p.ch)
	}
}

// Reboot 重启被关闭的 pool
func (p *pool1) Reboot() {
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
		l := int(p.workers.Len())
		for i := 0; i < l/4; i++ {
			idx := rand.Intn(l)
			p.workers.checkWorker(int32(idx))
		}
	}
}

// pool2
type pool2 struct {
	*pool
}

// Do 将任务交给 worker 或者 存放到任务队列中
func (p *pool2) Do(task TaskFunc) bool {
	// 尝试创建 worker
	ok := p.giveTaskToWorker(p, p.isMaxFull, task, true)
	if !ok {
		if !p.isNeedBlocking() {
			return false
		}
		for !ok {
			// 阻塞，等待唤醒
			p.block()
			// 尝试获取 w
			ok = p.giveTaskToWorker(p, p.isMaxFull, task, true)
		}
	}
	return true
}

// Close 关闭 pool
func (p *pool2) Close() {
	// 获取锁，保证并发安全，使得其他 goroutine 无法创建新的 worker
	p.lock.Lock()
	defer p.lock.Unlock()
	// 设置 pool 为 关闭状态
	p.setStatus(Closed)
	// 清空 workers，通知所有 worker 停止运行
	p.workers.reset()
	// 唤醒所有在等待的 Submit 线程
	p.cond.Broadcast()
}

// Reboot 重启被关闭的 pool
func (p *pool2) Reboot() {
	p.setStatus(Running)
}

// block 阻塞当前 Submit goroutine
func (p *pool2) block() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.blockSize++
	p.cond.Wait()
	p.blockSize--
}

// signal 唤醒 Submit 等待的 goroutine
func (p *pool2) signal() {
	if p.IsClosed() {
		return
	}
	p.cond.Signal()
}
