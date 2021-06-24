package main

import (
	"fmt"
	"gpool/gpool"
	"sync"
	"sync/atomic"
	"time"
)

var sum int32

func add(i int32) {
	time.Sleep(100 * time.Millisecond)
	atomic.AddInt32(&sum, i)
}

var wg sync.WaitGroup

func main() {
	wg = sync.WaitGroup{}

	var p = gpool.NewPool(100, 100, 2, gpool.WithIsBlocking(false),
		gpool.WithIsPreAllocation(true))
	times := 50000
	//times := 5000
	for i := 0; i < 1; i++ {
		p.Reboot()
		startTime := time.Now()
		sum = 0
		for i := 0; i < times; i++ {
			wg.Add(1)
			i := i
			p.Submit(func() {
				fmt.Println(i)
				add(int32(i))
				wg.Done()
			})
		}
		fmt.Println("wait...")
		wg.Wait()
		fmt.Println("耗时：", time.Now().Sub(startTime))

		except := getSum(times)
		fmt.Println("预期 sum 值：", except)
		fmt.Println("实际 sum 值:", sum)
		if sum != except {
			panic(fmt.Errorf("expect:%v, actually:%v", sum, except))
		}
	}
}

func getSum(j int) int32 {
	var sum int32
	for i := 0; i < j; i++ {
		sum += int32(i)
	}
	return sum
}
