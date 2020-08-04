package gopool

import (
	"fmt"
	"time"
)

// ErrTimeout returned by Pool to indicate that there no free
// goroutines during some period of time.
var ErrTimeout = fmt.Errorf("timeout")

// GoPool contains logic of goroutine reuse.
type GoPool struct {
	sem   chan struct{}
	queue chan func()
}

// New creates new goroutine pool with maxWorkers size. It also creates a work
// queue of sizeQueue size. Finally, it spawns given amount of goroutines
// immediately.
//   maxWorkers - maximum amount of goroutines
//   sizeQueue - size of waiters for goroutines. If 0 - then no queue.
//   spawn - amount of goroutines will start at immediately
func New(maxWorkers, sizeQueue, spawn int) *GoPool {
	if spawn <= 0 && sizeQueue > 0 {
		panic("dead queue configuration detected")
	}
	if spawn > maxWorkers {
		panic("spawn > workers")
	}
	p := &GoPool{
		sem:   make(chan struct{}, maxWorkers),
		queue: make(chan func(), sizeQueue),
	}
	for i := 0; i < spawn; i++ {
		p.sem <- struct{}{}
		go p.worker(func() {})
	}

	return p
}

// Schedule schedules task to be executed over pool's workers.
func (p *GoPool) Schedule(task func()) {
	_ = p.schedule(task, nil)
}

// ScheduleTimeout schedules task to be executed over pool's workers.
// It returns ErrScheduleTimeout when no free workers met during given timeout.
func (p *GoPool) ScheduleTimeout(timeout time.Duration, task func()) error {
	return p.schedule(task, time.After(timeout))
}

func (p *GoPool) schedule(task func(), timeout <-chan time.Time) error {
	select {
	case <-timeout:
		return ErrTimeout
	case p.queue <- task:
		return nil
	case p.sem <- struct{}{}:
		go p.worker(task)
		return nil
	}
}

func (p *GoPool) worker(task func()) {
	defer func() { <-p.sem }()

	task()

	for task := range p.queue {
		task()
	}
}

func (p *GoPool) Close() {
	close(p.queue)
	for len(p.sem) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	close(p.sem)
}
