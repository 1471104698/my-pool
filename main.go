package main

import (
	"fmt"
	"gpool/gpool"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"
)

func add(sum *int32, i int32) {
	time.Sleep(100 * time.Millisecond)
	atomic.AddInt32(sum, i)
	fmt.Println(i)
}

var errs int32

func addErr(i int32) {
	atomic.AddInt32(&errs, i)
}

var wg = sync.WaitGroup{}

func main() {
	var sum int32

	go seeStack()

	var p = gpool.NewPool2(0, 10, 10, gpool.WithIsBlocking(true),
		gpool.WithIsPreAllocation(false))
	//times := 3000
	times := 10000
	for j := 0; j < 1; j++ {
		p.Reboot()
		startTime := time.Now()
		sum = 0
		for i := 0; i < times; i++ {
			wg.Add(1)
			j := i
			err := p.Submit(p, func() {
				add(&sum, int32(j))
				wg.Done()
			})
			if err != nil {
				fmt.Printf("err：%v, 第 %v 个任务\n", err, i)
				// 这里直接报错，避免 wg.Wait() 阻塞
				panic(err)
			}
		}
		wg.Wait()
		fmt.Println("耗时：", time.Now().Sub(startTime))
		p.Close()
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

const (
	pprofAddr string = ":7890"
)

func seeStack() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}
