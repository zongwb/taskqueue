// The simple task queue implementation caters to two concurrency patterns: fan-in and fan-out.
//
// In a fan-in pattern, the client normally has many goroutines, and there is a need to
// control the concurrency to the (backend) computing resource. Use PostAndExec() in this case.
//
// In a fan-out pattern, the client is often single-threaded (e.g. a task dispatcher),
// and there is a need to execute the tasks in parallel. Use Post() in this case.

package taskqueue

import (
	"sync"
	"time"
)

var (
	// ErrQueueCancelled ...
	ErrQueueCancelled = &TQError{Reason: "queue is cancelled"}
	// ErrQueueClosed ...
	ErrQueueClosed = &TQError{Reason: "queue is closed"}
	// ErrTaskExpired ...
	ErrTaskExpired = &TQError{Reason: "task expired"}
	// ErrQueueFull ...
	ErrQueueFull = &TQError{Reason: "queue is full"}
)

// TQError defines a concrete error type so that client can determine
// if the error returned by Post() is from the application or the task queue.
type TQError struct {
	Reason string
}

func (e *TQError) Error() string {
	return e.Reason
}

// Task defines the callback interface for the real task.
type Task interface {
	Do() error
}

// TaskQueue implements a FIFO task queue with a pool of workers.
type TaskQueue struct {
	threads      int
	queueSize    int
	taskTimeout  time.Duration
	taskQueue    chan Task
	closed       bool
	quit         chan bool
	closeQueue   sync.Once
	cancelQueue  sync.Once
	wg           sync.WaitGroup
	sync.RWMutex // guard the 'closed' flag
}

// New creates a new instance.
// taskTimeout: the maximum waiting time before the task is processed.
func New(threads, queueSize int, taskTimeout time.Duration) *TaskQueue {
	m := &TaskQueue{
		threads:     threads,
		queueSize:   queueSize,
		taskTimeout: taskTimeout,
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

// Close stops all workers. Due to locking,
// calling Close may block until it has a chance to close the channel.
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
// enqueueTimeout: the waiting time before the task can be enqueued when the queue is full.
//                 if enqueueTimeout = 0, the enqueue operation will block.
// Note that if enqueueTimeout = 0, make sure taskTimeout is long enough to have a chance to be executed.
//
// This is a convenient wrapper for waiting for the execution result. Client can implement
// the same behaviour with Post() by waiting on a signal.
//
// This is usually used in a fan-in pattern.
func (m *TaskQueue) PostAndExec(t Task, enqueueTimeout time.Duration) error {
	return m.postCore(t, enqueueTimeout, true)
}

// Post sends a task to the task queue and returns immediately without waiting for
// the task's execution.
// enqueueTimeout: the waiting time before the task can be enqueued when the queue is full.
//                 if enqueueTimeout = 0, the enqueue operation will block.
// Note that if enqueueTimeout = 0, make sure taskTimeout is long enough to have a chance to be executed.
//
// This is usually used in a fan-out pattern.
func (m *TaskQueue) Post(t Task, enqueueTimeout time.Duration) error {
	return m.postCore(t, enqueueTimeout, false)
}

func (m *TaskQueue) postCore(t Task, enqueueTimeout time.Duration, sync bool) error {
	m.RLock()
	defer m.RUnlock()
	newT, err := m.prepareTask(t)
	if err != nil {
		return err
	}

	if enqueueTimeout == 0 {
		select {
		case m.taskQueue <- newT:
			// This is a blocking enqueue.
		case <-m.quit:
			return ErrQueueCancelled
		}
	} else {
		timer := time.NewTimer(enqueueTimeout)
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

// Read lock must be acquired before calling prepareTask.
func (m *TaskQueue) prepareTask(t Task) (*TimedTask, error) {
	// check if queue is cancelled
	select {
	case <-m.quit:
		return nil, ErrQueueCancelled
	default:
	}

	// check if queue is closed
	if m.closed {
		return nil, ErrQueueClosed
	}

	newT := &TimedTask{
		start:   time.Now(),
		timeout: m.taskTimeout,
		t:       t,
		notify:  make(chan error, 1),
	}
	return newT, nil
}

// TryPost sends a task to the task queue and returns immediately without waiting for
// the task's execution. If the queue is full, it returns immediately with an error
//
// This is usually used in a fan-out pattern.
func (m *TaskQueue) TryPost(t Task) error {
	return m.tryPostCore(t)
}

func (m *TaskQueue) tryPostCore(t Task) error {
	m.RLock()
	defer m.RUnlock()
	newT, err := m.prepareTask(t)
	if err != nil {
		return err
	}

	select {
	case m.taskQueue <- newT:
	default:
		return ErrQueueFull
	}
	return nil
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
