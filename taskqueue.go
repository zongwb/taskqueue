package taskqueue

import (
	"errors"
	"time"
)

var (
	//EnqueueTimeout = time.Millisecond * 20

	// ErrTaskExpired ...
	ErrTaskExpired = errors.New("task expired")
	// ErrQueueFull ...
	ErrQueueFull = errors.New("processing queue is full")
)

// Task defines the callback interface for the real task.
type Task interface {
	Do() error
}

// TaskQueue implements a FIFO task queue with a pool of workers.
type TaskQueue struct {
	threads        int
	queueSize      int
	taskTimeout    time.Duration
	enqueueTimeout time.Duration
	taskQueue      chan Task
	quit           chan bool
}

// NewTaskQueue creates a new instance.
// taskTimeout: the maximum waiting time before the task is processed.
// enqueueTimeout: the waiting time before the task can be queued when the queue is full.
func NewTaskQueue(threads, queueSize int, taskTimeout, enqueueTimeout time.Duration) *TaskQueue {
	m := &TaskQueue{
		threads:        threads,
		queueSize:      queueSize,
		taskTimeout:    taskTimeout,
		enqueueTimeout: enqueueTimeout,
	}
	return m
}

// Start starts the workers.
func (m *TaskQueue) Start() {
	m.taskQueue = make(chan Task, m.queueSize)
	m.quit = make(chan bool, m.threads)
	for i := 0; i < m.threads; i++ {
		go run(m.taskQueue, m.quit)
	}
}

// Stop stops all workers.
func (m *TaskQueue) Stop() {
	for i := 0; i < cap(m.quit); i++ {
		m.quit <- true
	}
}

// Dispatch enqueues a task to the task queue and waits for the result.
func (m *TaskQueue) Dispatch(t Task) error {
	newT := &TimedTask{
		start:   time.Now(),
		timeout: m.taskTimeout,
		t:       t,
		notify:  make(chan error, 1),
	}
	timer := time.NewTimer(m.enqueueTimeout)
	var err error
	select {
	case m.taskQueue <- newT:
		timer.Stop()
		// wait for the processing to finish
		err = <-newT.notify
	case <-timer.C:
		err = ErrQueueFull
	}
	return err
}

func run(q chan Task, quit chan bool) {
loop:
	for {
		select {
		case t := <-q:
			t.Do()
		case <-quit:
			break loop
		}
	}
}

// TimedTask implements the task interface and wraps another task.
type TimedTask struct {
	start   time.Time
	timeout time.Duration
	t       Task // the real task
	notify  chan error
}

// IsExpired returns true if the task has expired.
func (t *TimedTask) IsExpired() bool {
	span := time.Now().Sub(t.start)
	if span >= t.timeout {
		return true
	}
	return false
}

// Do executes the actual task.
func (t *TimedTask) Do() error {
	if t.IsExpired() {
		err := ErrTaskExpired
		t.notify <- err
		return err
	}
	err := t.t.Do()
	t.notify <- err
	return err
}
