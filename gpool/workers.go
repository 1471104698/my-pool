package gpool

// workers
type workers struct {
	cap int32
	len int32

	workers []*worker
}
