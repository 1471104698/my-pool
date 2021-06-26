package gpool

import (
	"sync/atomic"
	"testing"
)

var sum int32 = 0

func add(i int32) {
	atomic.AddInt32(&sum, i)
}

// TestWorkers
func TestWorkers(t *testing.T) {
	//ws := NewWorkersQueue(10)
	//wg := sync.WaitGroup{}
	//
	//times := 1000
	//consumer := func() {
	//	wg.Add(1)
	//	for i := 0; i < times; i++ {
	//		go func() {
	//			res := *ws.Take()
	//			add(res.task.(int32))
	//		}()
	//	}
	//	wg.Done()
	//}
	//producer := func() {
	//	wg.Add(1)
	//	for i := 0; i < times; i++ {
	//		var j int32 = int32(i)
	//		go func() {
	//			ws.Put(&worker{
	//				task: j,
	//			})
	//		}()
	//	}
	//	wg.Done()
	//}
	//go consumer()
	//go producer()
	//time.Sleep(1 * time.Second)
	//wg.Wait()
	//fmt.Println("sum:", sum)
	//if sum != getSum(times) {
	//	panic("the final result is wrong!!!")
	//}
}

func getSum(j int) int32 {
	var sum int32
	for i := 0; i < j; i++ {
		sum += int32(i)
	}
	return sum
}
