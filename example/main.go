package main

import (
	"fmt"
	"log"

	"github.com/zongwb/taskqueue"
	"time"
)

func main() {
	log.Println("vim-go")

	q := taskqueue.New(5, 10, time.Second*1000)

	n := 20
	for i := 0; i < n; i++ {
		job := &Job{
			name: fmt.Sprintf("dummy job %d", i),
		}
		// Whether to use Post or PostAndExec depends on the application.
		err := q.Post(job, 0) // 0 means blocking
		if err != nil {
			log.Println(err)
		}
		// q.PostAndExec(job, 0)
		if i == 10 {
			q.Close()
			//q.Cancel()
		}
	}

	log.Println("Waiting to complete...")
	q.CloseAndWait()
	//q.Wait()
}

type Job struct {
	name string
}

func (j *Job) Do() error {
	log.Printf("Processing job: %s\n", j.name)
	time.Sleep(time.Second)
	return nil
}
