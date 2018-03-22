package main

import (
	"fmt"

	"github.com/zongwb/taskqueue"
	"time"
)

func main() {
	fmt.Println("vim-go")

	q := taskqueue.NewTaskQueue(2, 100, time.Second, time.Millisecond*50)
	q.Start()

	job := &Job{
		name: "dummy job",
	}
	q.Dispatch(job)
	job2 := &Job{
		name: "dummy job 2",
	}
	q.Dispatch(job2)

	q.Stop()
}

type Job struct {
	name string
}

func (j *Job) Do() error {
	fmt.Printf("Processing job: %s\n", j.name)
	return nil
}
