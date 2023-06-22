package que

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"weavelab.xyz/monorail/shared/wlib/wlog"
)

// WorkFunc is a function that performs a Job. If an error is returned, the job
// is reenqueued with exponential backoff.
type WorkFunc func(j *Job) error

// WorkMap is a map of Job names to WorkFuncs that are used to perform Jobs of a
// given type.
type WorkMap map[string]WorkFunc

// Worker is a single worker that pulls jobs off the specified Queue. If no Job
// is found, the Worker will sleep for Interval seconds.
type Worker struct {
	// Interval is the amount of time that this Worker should sleep before trying
	// to find another Job.
	Interval time.Duration

	// Queue is the name of the queue to pull Jobs off of. The default value, "",
	// is usable and is the default for both que-go and the ruby que library.
	Queue string

	// ID is a unique id for this worker.
	ID int

	c *Client
	m WorkMap

	mu   sync.Mutex
	done bool
	ch   chan struct{}
}

var defaultWakeInterval = 5 * time.Second

func init() {
	if v := os.Getenv("QUE_WAKE_INTERVAL"); v != "" {
		if newInt, err := strconv.Atoi(v); err == nil {
			defaultWakeInterval = time.Duration(newInt) * time.Second
		}
	}
}

// NewWorker returns a Worker that fetches Jobs from the Client and executes
// them using WorkMap. If the type of Job is not registered in the WorkMap, it's
// considered an error and the job is re-enqueued with a backoff.
//
// Workers default to an Interval of 5 seconds, which can be overridden by
// setting the environment variable QUE_WAKE_INTERVAL. The default Queue is the
// nameless queue "", which can be overridden by setting QUE_QUEUE. Either of
// these settings can be changed on the returned Worker before it is started
// with Work().
func NewWorker(c *Client, m WorkMap) *Worker {
	return &Worker{
		Interval: defaultWakeInterval,
		Queue:    os.Getenv("QUE_QUEUE"),
		c:        c,
		m:        m,
		ch:       make(chan struct{}),
	}
}

// Work pulls jobs off the Worker's Queue at its Interval. This function only
// returns after Shutdown() is called, so it should be run in its own goroutine.
func (w *Worker) Work(ctx context.Context, n int) {

	for {
		select {
		case <-w.ch:
			log.Println("worker done")
			return
		case <-time.After(w.Interval):
			for {
				if didWork := w.WorkOne(ctx, n); !didWork {
					break // didn't do any work, go back to sleep
				}
			}
		}
	}
}

type Tx struct {
	tx pgx.Tx
	mu sync.Mutex
}

func (ct *Tx) Rollback(ctx context.Context) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	return ct.tx.Rollback(ctx)
}

func (ct *Tx) Commit(ctx context.Context) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	return ct.tx.Commit(ctx)
}
func (ct *Tx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	return ct.tx.QueryRow(ctx, sql, args...)
}
func (ct *Tx) Exec(ctx context.Context, sql string, args ...interface{}) error {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	_, err := ct.tx.Exec(ctx, sql, args...)
	return err
}
func (w *Worker) printAvailableDBCons(n int) {
	for {
		select {
		case <-w.ch:
			log.Println("worker done")
			return
		case <-time.After(time.Second * 5):
			log.Printf("wroker %v acquired db connections %v, totol %v ,max %v", n, w.c.pool.Stat().AcquiredConns(), w.c.pool.Stat().TotalConns(),
				w.c.pool.Stat().MaxConns())
		}
	}
}

func (w *Worker) WorkOne(ctx context.Context, n int) (didWork bool) {

	for i := 0; i < maxLockJobAttempts; i++ {

		tx, err := w.c.pool.Begin(ctx)
		if err != nil {
			wlog.InfoC(ctx, fmt.Sprintf("unable to begin transaction: %v", err))
			return
		}
		transaction := Tx{
			tx: tx,
		}
		j := Job{}

		err = transaction.QueryRow(ctx, sqlGlobalLockJob, w.Queue).Scan(
			&j.Queue,
			&j.Priority,
			&j.RunAt,
			&j.ID,
			&j.Type,
			&j.Args,
			&j.ErrorCount,
			&j.ShardID,
			&j.LastError,
		)

		if err != nil {
			err2 := transaction.Rollback(ctx)
			if err2 != nil {
				log.Printf("error while rolling back %v", err)
			}
			if strings.Contains(err.Error(), "no rows in result set") {
				log.Printf("attempting to lock the job from wroker %v : %v", n, err)
				return
			} else {
				log.Printf("received error from wroker %v.... retrying : %v", n, err)
				continue
			}
		} else {
			job := &j

			if job == nil || (job != nil && job.ID == 0) {
				err2 := transaction.Rollback(ctx)
				if err2 != nil {
					log.Printf("error while rolling back %v", err)
				}
				return // no job was available
			}
			defer recoverPanic(ctx, job)
			didWork = true
			job.WorkerID = w.ID
			job.Client = w.c

			wf, ok := w.m[job.Type]
			if !ok {
				msg := fmt.Sprintf("unknown job type: %q", j.Type)
				log.Println(msg)
				if err = j.Error(ctx, msg); err != nil {
					log.Printf("attempting to save error on job %d: %v", j.ID, err)
				}
				return
			}

			if err = wf(job); err != nil {
				job.Error(ctx, err.Error())
				return
			}

			err = transaction.Exec(ctx, sqlDeleteJob, j.Queue, j.Priority, j.RunAt, j.ID)
			if err != nil {
				err2 := transaction.Rollback(ctx)
				if err2 != nil {
					log.Printf("error while rolling back %v", err)
				}

				log.Printf("attempting to delete job %d: %v", j.ID, err)
				return
			}
			err = transaction.Commit(ctx)
			if err != nil {
				err2 := transaction.Rollback(ctx)
				if err2 != nil {
					log.Printf("error while rolling back %v", err)
				}
				log.Printf("error while Committing changes  %v", err)

			}
			wlog.InfoC(ctx, fmt.Sprintf("wroker %v event is done =job_worked job_id=%d job_type=%s", n, j.ID, j.Type))
			break
		}

	}

	//job := &j
	//
	//if job == nil || (job != nil && job.ID == 0) {
	//	return // no job was available
	//}
	//defer recoverPanic(ctx, job)
	//didWork = true
	//job.WorkerID = w.ID
	//job.Client = w.c
	//
	//
	//wf, ok := w.m[job.Type]
	//if !ok {
	//	msg := fmt.Sprintf("unknown job type: %q", j.Type)
	//	log.Println(msg)
	//	if err = j.Error(ctx, msg); err != nil {
	//		log.Printf("attempting to save error on job %d: %v", j.ID, err)
	//	}
	//	return
	//}
	//
	//if err = wf(job); err != nil {
	//	job.Error(ctx, err.Error())
	//	return
	//}

	//err = transaction.Exec(ctx, sqlDeleteJob, j.Queue, j.Priority, j.RunAt, j.ID)
	//if err != nil {
	//	log.Printf("attempting to delete job %d: %v", j.ID, err)
	//}
	//err = transaction.Commit(ctx)
	//if err != nil {
	//	log.Printf("error while Committing changes  %v", err)
	//}

	//wlog.InfoC(ctx, fmt.Sprintf("event is done =job_worked job_id=%d job_type=%s", j.ID, j.Type))

	return
}

// Shutdown tells the worker to finish processing its current job and then stop.
// There is currently no timeout for in-progress jobs. This function blocks
// until the Worker has stopped working. It should only be called on an active
// Worker.
func (w *Worker) Shutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done {
		return
	}

	log.Println("worker shutting down gracefully...")
	w.ch <- struct{}{}
	w.done = true
	close(w.ch)
}

// recoverPanic tries to handle panics in job execution.
// A stacktrace is stored into Job last_error.
func recoverPanic(ctx context.Context, j *Job) {
	if r := recover(); r != nil {
		// record an error on the job with panic message and stacktrace
		stackBuf := make([]byte, 1024)
		n := runtime.Stack(stackBuf, false)

		buf := &bytes.Buffer{}
		fmt.Fprintf(buf, "%v\n", r)
		fmt.Fprintln(buf, string(stackBuf[:n]))
		fmt.Fprintln(buf, "[...]")
		stacktrace := buf.String()
		log.Printf("event=panic job_id=%d job_type=%s\n%s", j.ID, j.Type, stacktrace)
		if err := j.Error(ctx, stacktrace); err != nil {
			log.Printf("attempting to save error on job %d: %v", j.ID, err)
		}
	}
}

// WorkerPool is a pool of Workers, each working jobs from the queue Queue
// at the specified Interval using the WorkMap.
type WorkerPool struct {
	WorkMap  WorkMap
	Interval time.Duration
	Queue    string

	c       *Client
	workers []*Worker
	mu      sync.Mutex
	done    bool
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
func NewWorkerPool(c *Client, wm WorkMap, count int) *WorkerPool {
	return &WorkerPool{
		c:        c,
		WorkMap:  wm,
		Interval: defaultWakeInterval,
		workers:  make([]*Worker, count),
	}
}

// Start starts all of the Workers in the WorkerPool.
func (w *WorkerPool) Start(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.workers {
		w.workers[i] = NewWorker(w.c, w.WorkMap)
		w.workers[i].Interval = w.Interval
		w.workers[i].Queue = w.Queue
		w.workers[i].ID = int(i)
		go w.workers[i].Work(ctx, i)
		go w.workers[i].printAvailableDBCons(i)
	}
}

// Shutdown sends a Shutdown signal to each of the Workers in the WorkerPool and
// waits for them all to finish shutting down.
func (w *WorkerPool) Shutdown() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done {
		return
	}
	var wg sync.WaitGroup
	wg.Add(len(w.workers))

	for _, worker := range w.workers {
		go func(worker *Worker) {
			worker.Shutdown()
			wg.Done()
		}(worker)
	}
	wg.Wait()
	w.done = true
}
