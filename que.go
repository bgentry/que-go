package que

import (
	"database/sql"
	"errors"
	"time"

	"github.com/lib/pq"
)

// Job is a single unit of work for Que to perform.
type Job struct {
	// ID is the unique database ID of the Job. It is ignored on job creation.
	ID int64

	// Queue is the name of the queue. It defaults to the empty queue "".
	Queue string

	// Priority is the priority of the Job. The default priority is 100, and a
	// lower number means a higher priority. A priority of 5 would be very
	// important.
	Priority int

	// RunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	RunAt time.Time

	// Type corresponds to the Ruby job_class. If you are interoperating with
	// Ruby, you should pick suitable Ruby class names (such as MyJob).
	Type string

	// Args must be a valid JSON string
	// TODO: should this be []byte instead?
	Args string

	// ErrorCount is the number of times this job has attempted to run, but
	// failed with an error. It is ignored on job creation.
	ErrorCount int

	// LastError is the error message or stack trace from the last time the job
	// failed. It is ignored on job creation.
	LastError sql.NullString
}

// TODO: add a way to specify default queueing options
type Client struct {
	db *sql.DB
}

func NewClient(db *sql.DB) *Client {
	return &Client{db: db}
}

var ErrMissingType = errors.New("job type must be specified")

func (c *Client) Enqueue(j Job) error {
	if j.Type == "" {
		return ErrMissingType
	}

	queue := sql.NullString{
		String: j.Queue,
		Valid:  j.Queue != "",
	}
	priority := sql.NullInt64{
		Int64: int64(j.Priority),
		Valid: j.Priority != 0,
	}
	runAt := pq.NullTime{
		Time:  j.RunAt,
		Valid: !j.RunAt.IsZero(),
	}
	args := sql.NullString{
		String: j.Args,
		Valid:  j.Args != "",
	}

	_, err := c.db.Exec(sqlInsertJob, queue, priority, runAt, j.Type, args)
	return err
}

// TODO: consider an alternate Enqueue func that also returns the newly
// enqueued Job struct. The query sqlInsertJobAndReturn was already written for
// this.
