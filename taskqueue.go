package taskqueue

import (
	"errors"
	"sync"
	"time"
)

var (
	//EnqueueTimeout = time.Millisecond * 20

	// ErrQueueCancelled ...
	ErrQueueCancelled = errors.New("queue cancelled")
	// ErrQueueClosed ...
	ErrQueueClosed = errors.New("queue closed")
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
	closed         bool
	quit           chan bool
	closeQueue     sync.Once
	cancelQueue    sync.Once
	wg             sync.WaitGroup
	sync.RWMutex   // guard the 'closed' flag
}

// New creates a new instance.
// taskTimeout: the maximum waiting time before the task is processed.
// enqueueTimeout: the waiting time before the task can be queued when the queue is full.
//                 if enqueueTimeout = 0, the enqueue operation will block.
// Note that if enqueueTimeout = 0, make sure taskTimeout is long enough to have a chance to be executed.
func New(threads, queueSize int, taskTimeout, enqueueTimeout time.Duration) *TaskQueue {
	m := &TaskQueue{
		threads:        threads,
		queueSize:      queueSize,
		taskTimeout:    taskTimeout,
		enqueueTimeout: enqueueTimeout,
	}
	m.start()
	return m
}

func (m *TaskQueue) start() {
	m.taskQueue = make(chan Task, m.queueSize)
	m.quit = make(chan bool)
	for i := 0; i < m.threads; i++ {
		m.wg.Add(1)
		go m.run()
	}
}

func (m *TaskQueue) run() {
	defer m.wg.Done()
	for {
		select {
		case <-m.quit:
			return
		default:
		}
		select {
		case t, ok := <-m.taskQueue:
			if !ok {
				return
			}
			t.Do()
		case <-m.quit:
			return
		}
	}
}

// Cancel cancels all pending tasks, so some tasks may never be executed.
func (m *TaskQueue) Cancel() {
	m.cancelQueue.Do(func() {
		close(m.quit) // Tasks in the queue will not be processed
	})
}

// Wait waits till all pending tasks are completed.
func (m *TaskQueue) Wait() {
	m.wg.Wait()
}

// Close stops all workers.
func (m *TaskQueue) Close() {
	m.closeQueue.Do(func() {
		m.Lock()
		defer m.Unlock()
		m.closed = true
		close(m.taskQueue) // Tasks in the queue will still be processed
	})
}

// CloseAndWait is a convenient wrapper for Close() and Wait().
func (m *TaskQueue) CloseAndWait() {
	m.Close()
	m.Wait()
}

// PostAndExec sends a task to the task queue and waits for the task's execution to complete.
// This is usually used in a fan-in pattern.
// The enqueue operation is either blocking or time-out depending on enqueueTimeout.
// This is a convenient wrapper for waiting for the execution result. Client can implement
// the same behaviour with Post() by waiting on a signal.
func (m *TaskQueue) PostAndExec(t Task) error {
	return m.postCore(t, true)
}

// Post sends a task to the task queue and returns immediately without waiting for
// the task's execution. This is usually used in a fan-out pattern.
// The enqueue operation is either blocking or time-out depending on enqueueTimeout.
func (m *TaskQueue) Post(t Task) error {
	return m.postCore(t, false)
}

func (m *TaskQueue) postCore(t Task, sync bool) error {
	// check if queue is cancelled
	select {
	case <-m.quit:
		return ErrQueueCancelled
	default:
	}

	// check if queue is closed
	m.RLock()
	defer m.RUnlock()
	if m.closed {
		return ErrQueueClosed
	}

	newT := &TimedTask{
		start:   time.Now(),
		timeout: m.taskTimeout,
		t:       t,
		notify:  make(chan error, 1),
	}
	var err error
	if m.enqueueTimeout == 0 {
		select {
		case m.taskQueue <- newT:
			// This is a blocking enqueue.
		case <-m.quit:
			return ErrQueueCancelled
		}
	} else {
		timer := time.NewTimer(m.enqueueTimeout)
		select {
		case m.taskQueue <- newT:
			timer.Stop()
		case <-m.quit:
			timer.Stop()
			return ErrQueueCancelled
		case <-timer.C:
			return ErrQueueFull
		}
	}

	if !sync {
		return nil
	}

	select {
	case err = <-newT.notify:
	// wait for the processing to finish
	case <-m.quit:
		return ErrQueueCancelled
	}
	return err
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
