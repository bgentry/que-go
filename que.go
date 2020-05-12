package que

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/weave-lab/pgx"
	"github.com/weave-lab/pgx/pgtype"
	"weavelab.xyz/monorail/shared/go-utilities/null"
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
	Priority int16

	// RunAt is the time that this job should be executed. It defaults to now(),
	// meaning the job will execute immediately. Set it to a value in the future
	// to delay a job's execution.
	RunAt time.Time

	// Type corresponds to the Ruby job_class. If you are interoperating with
	// Ruby, you should pick suitable Ruby class names (such as MyJob).
	Type string

	// Args must be the bytes of a valid JSON string
	Args []byte

	// ErrorCount is the number of times this job has attempted to run, but
	// failed with an error. It is ignored on job creation.
	ErrorCount int32

	// LastError is the error message or stack trace from the last time the job
	// failed. It is ignored on job creation.
	LastError pgtype.Text

	// WorkerID is the id of the worker working this job. It is ignored on creation.
	WorkerID int

	ShardID null.UUID

	// Client is a reference to the Que Client which originated the request.
	// This allows jobs to enqueue additional jobs as needed.
	Client *Client

	mu      sync.Mutex
	deleted bool
	pool    *pgx.ConnPool
	conn    *pgx.Conn
}

// Conn returns the pgx connection that this job is locked to. You may initiate
// transactions on this connection or use it as you please until you call
// Done(). At that point, this conn will be returned to the pool and it is
// unsafe to keep using it. This function will return nil if the Job's
// connection has already been released with Done().
func (j *Job) Conn() *pgx.Conn {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.conn
}

// Delete marks this job as complete by deleting it form the database.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Delete() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.deleted {
		return nil
	}

	_, err := j.conn.Exec("que_destroy_job", j.Queue, j.Priority, j.RunAt, j.ID)
	if err != nil {
		return err
	}

	j.deleted = true
	return nil
}

// Done releases the Postgres advisory lock on the job and returns the database
// connection to the pool.
func (j *Job) Done() {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.conn == nil || j.pool == nil {
		// already marked as done
		return
	}

	var ok bool
	// Swallow this error because we don't want an unlock failure to cause work to
	// stop.
	_ = j.conn.QueryRow("que_unlock_job", j.ID).Scan(&ok)

	j.pool.Release(j.conn)
	j.pool = nil
	j.conn = nil
}

// Error marks the job as failed and schedules it to be reworked. An error
// message or backtrace can be provided as msg, which will be saved on the job.
// It will also increase the error count.
//
// You must also later call Done() to return this job's database connection to
// the pool.
func (j *Job) Error(msg string) error {
	errorCount := j.ErrorCount + 1
	delay := intPow(int(errorCount), 4) + 3 // TODO: configurable delay

	_, err := j.conn.Exec("que_set_error", errorCount, delay, msg, j.Queue, j.Priority, j.RunAt, j.ID)
	if err != nil {
		return err
	}
	return nil
}

// Client is a Que client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	pool    *pgx.ConnPool
	inserts map[string]string

	// TODO: add a way to specify default queueing options
}

// NewClient creates a new Client that uses the pgx pool.
func NewClient(pool *pgx.ConnPool) *Client {
	// prepare custom sql if exists
	return &Client{pool, make(map[string]string)}
}

// ErrMissingType is returned when you attempt to enqueue a job with no Type
// specified.
var ErrMissingType = errors.New("job type must be specified")
var ErrNoCustomEnqueue = errors.New("no custom sql enqueue was found")

func (c *Client) AddCustomEnqueues(enq map[string]string) error {
	for name, sql := range enq {
		if _, err := c.pool.Prepare(name, sql); err != nil {
			return err
		}
		c.inserts[name] = sql
	}
	return nil
}

// EnqueueCustom allows you to use a custom sql insert specified at queue creation time
func (c *Client) EnqueueCustom(j *Job, sql string) error {
	if _, ok := c.inserts[sql]; !ok {
		return ErrNoCustomEnqueue
	}

	return execEnqueue(j, c.pool, sql)
}

// Enqueue adds a job to the queue.
func (c *Client) Enqueue(j *Job) error {
	return execEnqueue(j, c.pool, "que_insert_job")
}

func (c *Client) BulkCustomEnqueue(jobs []*Job, sql string) error {
	if _, ok := c.inserts[sql]; !ok {
		return ErrNoCustomEnqueue
	}

	return bulkEnqueue(jobs, c.pool, sql)
}

func (c *Client) BulkEnqueue(jobs []*Job) error {
	return bulkEnqueue(jobs, c.pool, "que_insert_job")
}

// EnqueueInTx adds a job to the queue within the scope of the transaction tx.
// This allows you to guarantee that an enqueued job will either be committed or
// rolled back atomically with other changes in the course of this transaction.
//
// It is the caller's responsibility to Commit or Rollback the transaction after
// this function is called.
func (c *Client) EnqueueInTx(j *Job, tx *pgx.Tx) error {
	return execEnqueue(j, tx, "que_insert_job")
}

type PreparedJob struct {
	Type     string
	Queue    *pgtype.Text
	Priority *pgtype.Int2
	RunAt    *pgtype.Timestamptz
	Args     *pgtype.Bytea
	ShardID  null.UUID
}

func prepareJob(j *Job) (*PreparedJob, error) {
	prepped := &PreparedJob{}
	if j.Type == "" {
		return prepped, ErrMissingType
	}
	prepped.Type = j.Type

	prepped.Queue = &pgtype.Text{
		String: j.Queue,
		Status: pgtype.Null,
	}
	if j.Queue != "" {
		prepped.Queue.Status = pgtype.Present
	}

	prepped.Priority = &pgtype.Int2{
		Int:    j.Priority,
		Status: pgtype.Null,
	}
	if j.Priority != 0 {
		prepped.Priority.Status = pgtype.Present
	}

	prepped.RunAt = &pgtype.Timestamptz{
		Time:   j.RunAt,
		Status: pgtype.Null,
	}
	if !j.RunAt.IsZero() {
		prepped.RunAt.Status = pgtype.Present
	}

	prepped.Args = &pgtype.Bytea{
		Bytes:  j.Args,
		Status: pgtype.Null,
	}
	if len(j.Args) != 0 {
		prepped.Args.Status = pgtype.Present
	}

	prepped.ShardID = j.ShardID

	return prepped, nil
}

func execEnqueue(j *Job, q queryable, sql string) error {
	prepped, err := prepareJob(j)
	if err != nil {
		return err
	}

	_, err = q.Exec(sql, prepped.Queue, prepped.Priority, prepped.RunAt, prepped.Type, prepped.Args, prepped.ShardID)
	return err
}

const maxBatch = 5000 // Force a maximum batch volume to deal with high volume database deadlocks
func bulkEnqueue(jobs []*Job, connectionPool *pgx.ConnPool, sql string) error {
	ctx := context.Background()
	batches := makeBatches(jobs, maxBatch)
	for _, batchJobs := range batches {
		batch := connectionPool.BeginBatch()
		for _, job := range batchJobs {
			prepped, err := prepareJob(job)
			if err != nil {
				return err
			}
			batch.Queue(sql,
				[]interface{}{prepped.Queue, prepped.Priority, prepped.RunAt, prepped.Type, prepped.Args, prepped.ShardID},
				[]pgtype.OID{},
				[]int16{},
			)
		}
		err := batch.Send(ctx, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeBatches(jobs []*Job, batchSize int) [][]*Job {
	batches := make([][]*Job, 0, (len(jobs)+batchSize-1)/batchSize)
	for batchSize < len(jobs) {
		jobs, batches = jobs[batchSize:], append(batches, jobs[0:batchSize:batchSize])
	}
	batches = append(batches, jobs)
	return batches
}

type queryable interface {
	Exec(sql string, arguments ...interface{}) (commandTag pgx.CommandTag, err error)
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
	QueryRow(sql string, args ...interface{}) *pgx.Row
}

// Maximum number of loop iterations in LockJob before giving up.  This is to
// avoid looping forever in case something is wrong.
const maxLockJobAttempts = 10

// Returned by LockJob if a job could not be retrieved from the queue after
// several attempts because of concurrently running transactions.  This error
// should not be returned unless the queue is under extremely heavy
// concurrency.
var ErrAgain = errors.New("maximum number of LockJob attempts reached")

// TODO: consider an alternate Enqueue func that also returns the newly
// enqueued Job struct. The query sqlInsertJobAndReturn was already written for
// this.

// LockJob attempts to retrieve a Job from the database in the specified queue.
// If a job is found, a session-level Postgres advisory lock is created for the
// Job's ID. If no job is found, nil will be returned instead of an error.
//
// Because Que uses session-level advisory locks, we have to hold the
// same connection throughout the process of getting a job, working it,
// deleting it, and removing the lock.
//
// After the Job has been worked, you must call either Done() or Error() on it
// in order to return the database connection to the pool and remove the lock.
func (c *Client) LockJob(queue string) (*Job, error) {
	conn, err := c.pool.Acquire()
	if err != nil {
		return nil, err
	}

	j := Job{pool: c.pool, conn: conn}

	for i := 0; i < maxLockJobAttempts; i++ {
		err = conn.QueryRow("que_lock_job", queue).Scan(
			&j.Queue,
			&j.Priority,
			&j.RunAt,
			&j.ID,
			&j.Type,
			&j.Args,
			&j.ErrorCount,
			&j.ShardID.UUID,
			&j.LastError,
		)

		if err != nil {
			c.pool.Release(conn)
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}

		// Deal with race condition. Explanation from the Ruby Que gem:
		//
		// Edge case: It's possible for the lock_job query to have
		// grabbed a job that's already been worked, if it took its MVCC
		// snapshot while the job was processing, but didn't attempt the
		// advisory lock until it was finished. Since we have the lock, a
		// previous worker would have deleted it by now, so we just
		// double check that it still exists before working it.
		//
		// Note that there is currently no spec for this behavior, since
		// I'm not sure how to reliably commit a transaction that deletes
		// the job in a separate thread between lock_job and check_job.
		var ok bool
		err = conn.QueryRow("que_check_job", j.Queue, j.Priority, j.RunAt, j.ID).Scan(&ok)
		if err == nil {
			return &j, nil
		} else if err == pgx.ErrNoRows {
			// Encountered job race condition; start over from the beginning.
			// We're still holding the advisory lock, though, so we need to
			// release it before resuming.  Otherwise we leak the lock,
			// eventually causing the server to run out of locks.
			//
			// Also swallow the possible error, exactly like in Done.
			_ = conn.QueryRow("que_unlock_job", j.ID).Scan(&ok)
			continue
		} else {
			c.pool.Release(conn)
			return nil, err
		}
	}
	c.pool.Release(conn)
	return nil, ErrAgain
}

var preparedStatements = map[string]string{
	"que_check_job":   sqlCheckJob,
	"que_destroy_job": sqlDeleteJob,
	"que_insert_job":  sqlInsertJob,
	"que_lock_job":    sqlLockJob,
	"que_set_error":   sqlSetError,
	"que_unlock_job":  sqlUnlockJob,
}

func PrepareStatements(conn *pgx.Conn) error {
	for name, sql := range preparedStatements {
		if _, err := conn.Prepare(name, sql); err != nil {
			return err
		}
	}
	return nil
}
