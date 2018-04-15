package que

import (
	"testing"
	"time"
	"os"
)

func TestWakeInterval(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultInterval := NewWorker(c, wm)
	if workerWithDefaultInterval.Interval != defaultWakeInterval {
		t.Fatal("Worker interval has value other than the default one")
	}

	customInterval := 12345 * time.Millisecond
	workerWithCustomInterval := NewWorker(c, wm, WakeInterval(customInterval))
	if workerWithCustomInterval.Interval != customInterval {
		t.Fatal("Worker interval has value other than custom one")
	}
}

func TestWorkerQueue(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerWithDefaultQueue := NewWorker(c, wm)
	if workerWithDefaultQueue.Queue != os.Getenv("QUE_QUEUE") {
		t.Fatal("Worker queue has value other than the default one")
	}

	customQueue := "fooBarBaz"
	workerWithCustomQueue := NewWorker(c, wm, WorkerQueue(customQueue))
	if workerWithCustomQueue.Queue != customQueue {
		t.Fatal("Worker interval has value other than custom one")
	}
}

func TestPoolWakeInterval(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultInterval := NewWorkerPool(c, wm, 2)
	if workerPoolWithDefaultInterval.Interval != defaultWakeInterval {
		t.Fatal("WorkerPool interval has value other than the default one")
	}

	customInterval := 12345 * time.Millisecond
	workerPoolWithCustomInterval := NewWorkerPool(c, wm, 2, PoolWakeInterval(customInterval))
	if workerPoolWithCustomInterval.Interval != customInterval {
		t.Fatal("WorkerPool interval has value other than custom one")
	}
}

func TestPoolWorkerQueue(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	wm := WorkMap{
		"MyJob": func(j *Job) error {
			return nil
		},
	}

	workerPoolWithDefaultQueue := NewWorkerPool(c, wm, 2)
	if workerPoolWithDefaultQueue.Queue != "" {
		t.Fatal("WorkerPool queue has value other than the default one")
	}

	customQueue := "fooBarBaz"
	workerPoolWithCustomQueue := NewWorkerPool(c, wm, 2, PoolWorkerQueue(customQueue))
	if workerPoolWithCustomQueue.Queue != customQueue {
		t.Fatal("WorkerPool interval has value other than custom one")
	}
}
