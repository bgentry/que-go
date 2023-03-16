package que

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
	"time"
)

func TestLockJob(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}

	if j.conn == nil {
		t.Fatal("want non-nil conn on locked Job")
	}
	if j.pool == nil {
		t.Fatal("want non-nil pool on locked Job")
	}
	defer j.Done(context.Background())

	// check values of returned Job
	if j.ID == 0 {
		t.Errorf("want non-zero ID")
	}
	if want := ""; j.Queue != want {
		t.Errorf("want Queue=%q, got %q", want, j.Queue)
	}
	if want := int16(100); j.Priority != want {
		t.Errorf("want Priority=%d, got %d", want, j.Priority)
	}
	if j.RunAt.IsZero() {
		t.Error("want non-zero RunAt")
	}
	if want := "MyJob"; j.Type != want {
		t.Errorf("want Type=%q, got %q", want, j.Type)
	}
	if want, got := "[]", string(j.Args); got != want {
		t.Errorf("want Args=%s, got %s", want, got)
	}
	if want := int32(0); j.ErrorCount != want {
		t.Errorf("want ErrorCount=%d, got %d", want, j.ErrorCount)
	}
	if j.LastError.Valid {
		t.Errorf("want no LastError, got %v", j.LastError)
	}

	// check for advisory lock
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = j.pool.QueryRow(context.Background(), query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("want 1 advisory lock, got %d", count)
	}

	// make sure conn was checked out of pool
	stat := c.pool.Stat()
	total, idle := stat.TotalConns(), stat.IdleConns()
	if want := total - 1; idle != want {
		t.Errorf("want idle=%d, got %d", want, idle)
	}

	if err = j.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLockJobAlreadyLocked(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done(context.Background())

	j2, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j2 != nil {
		defer j2.Done(context.Background())
		t.Fatalf("wanted no job, got %+v", j2)
	}
}

func TestLockJobNoJob(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		t.Errorf("want no job, got %v", j)
	}
}

func TestLockJobCustomQueue(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob", Queue: "extra_priority"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		j.Done(context.Background())
		t.Errorf("expected no job to be found with empty queue name, got %+v", j)
	}

	j, err = c.LockJob(context.Background(), "extra_priority")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Done(context.Background())

	if j == nil {
		t.Fatal("wanted job, got none")
	}

	if err = j.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestJobConn(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done(context.Background())

	if conn := j.Conn(); conn != j.conn {
		t.Errorf("want %+v, got %+v", j.conn, conn)
	}
}

func TestJobConnRace(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)

	// call Conn and Done in different goroutines to make sure they are safe from
	// races.
	go func() {
		_ = j.Conn()
		wg.Done()
	}()
	go func() {
		j.Done(context.Background())
		wg.Done()
	}()
	wg.Wait()
}

// Test the race condition in LockJob
func TestLockJobAdvisoryRace(t *testing.T) {
	c := openTestClientMaxConns(t, 2)
	defer truncateAndClose(context.Background(), c.pool)

	// *pgx.ConnPool doesn't support pools of only one connection.  Make sure
	// the other one is busy so we know which backend will be used by LockJob
	// below.
	unusedConn, err := c.pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unusedConn.Release()

	// We use two jobs: the first one is concurrently deleted, and the second
	// one is returned by LockJob after recovering from the race condition.
	for i := 0; i < 2; i++ {
		if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
			t.Fatal(err)
		}
	}

	// helper functions
	newPool := func() *pgxpool.Pool {
		connPoolConfig := pgxpool.Config{
			ConnConfig:   testConnConfig,
			AfterConnect: PrepareStatements,
		}
		conn, err := pgxpool.NewWithConfig(context.Background(), &connPoolConfig)
		if err != nil {
			panic(err)
		}
		return conn
	}
	getBackendPID := func(conn *pgxpool.Conn) int32 {
		var backendPID int32
		err := conn.QueryRow(context.Background(), `
			SELECT pg_backend_pid()
		`).Scan(&backendPID)
		if err != nil {
			panic(err)
		}
		return backendPID
	}
	waitUntilBackendIsWaiting := func(backendPID int32, name string) {
		pool := newPool()
		i := 0
		for {
			var waiting bool
			err := pool.QueryRow(context.Background(), `SELECT wait_event is not null from pg_stat_activity where pid=$1`, backendPID).Scan(&waiting)
			if err != nil {
				panic(err)
			}

			if waiting {
				break
			} else {
				i++
				if i >= 10000/50 {
					panic(fmt.Sprintf("timed out while waiting for %s", name))
				}

				time.Sleep(50 * time.Millisecond)
			}
		}

	}

	// Reproducing the race condition is a bit tricky.  The idea is to form a
	// lock queue on the relation that looks like this:
	//
	//   AccessExclusive <- AccessShare  <- AccessExclusive ( <- AccessShare )
	//
	// where the leftmost AccessShare lock is the one implicitly taken by the
	// sqlLockJob query.  Once we release the leftmost AccessExclusive lock
	// without releasing the rightmost one, the session holding the rightmost
	// AccessExclusiveLock can run the necessary DELETE before the sqlCheckJob
	// query runs (since it'll be blocked behind the rightmost AccessExclusive
	// Lock).
	//
	deletedJobIDChan := make(chan int64, 1)
	lockJobBackendIDChan := make(chan int32)
	secondAccessExclusiveBackendIDChan := make(chan int32)

	go func() {
		conn := newPool()
		defer conn.Close()

		tx, err := conn.Begin(context.Background())
		if err != nil {
			panic(err)
		}
		_, err = tx.Exec(context.Background(), `LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		if err != nil {
			panic(err)
		}

		// first wait for LockJob to appear behind us
		backendID := <-lockJobBackendIDChan
		waitUntilBackendIsWaiting(backendID, "LockJob")

		// then for the AccessExclusive lock to appear behind that one
		backendID = <-secondAccessExclusiveBackendIDChan
		waitUntilBackendIsWaiting(backendID, "second access exclusive lock")

		err = tx.Rollback(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		pool := newPool()
		defer pool.Close()

		conn, err := pool.Acquire(context.Background())
		if err != nil {
			panic(err)
		}

		// synchronization point
		secondAccessExclusiveBackendIDChan <- getBackendPID(conn)

		tx, err := conn.Begin(context.Background())
		if err != nil {
			panic(err)
		}
		_, err = tx.Exec(context.Background(), `LOCK TABLE que_jobs IN ACCESS EXCLUSIVE MODE`)
		if err != nil {
			panic(err)
		}

		// Fake a concurrent transaction grabbing the job
		var jid int64
		err = tx.QueryRow(context.Background(), `
			DELETE FROM que_jobs
			WHERE job_id =
				(SELECT min(job_id)
				 FROM que_jobs)
			RETURNING job_id
		`).Scan(&jid)
		if err != nil {
			panic(err)
		}

		deletedJobIDChan <- jid

		err = tx.Commit(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	conn, err := c.pool.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	ourBackendID := getBackendPID(conn)
	conn.Release()

	// synchronization point
	lockJobBackendIDChan <- ourBackendID

	job, err := c.LockJob(context.Background(), "")
	if err != nil {
		panic(err)
	}
	defer job.Done(context.Background())

	deletedJobID := <-deletedJobIDChan

	t.Logf("Got id %d", job.ID)
	t.Logf("Concurrently deleted id %d", deletedJobID)

	if deletedJobID >= job.ID {
		t.Fatalf("deleted job id %d must be smaller than job.ID %d", deletedJobID, job.ID)
	}
}

func TestJobDelete(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done(context.Background())

	if err = j.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}

	// make sure job was deleted
	j2, err := findOneJob(context.Background(), c.pool)
	if err != nil {
		t.Fatal(err)
	}
	if j2 != nil {
		t.Errorf("job was not deleted: %+v", j2)
	}
}

func TestJobDone(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	j.Done(context.Background())

	// make sure conn and pool were cleared
	if j.conn != nil {
		t.Errorf("want nil conn, got %+v", j.conn)
	}
	if j.pool != nil {
		t.Errorf("want nil pool, got %+v", j.pool)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.pool.QueryRow(context.Background(), query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, idle := stat.TotalConns(), stat.IdleConns()
	if total != idle {
		t.Errorf("want idle=total, got idle=%d total=%d", idle, total)
	}
}

func TestJobDoneMultiple(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	j.Done(context.Background())
	// try calling Done() again
	j.Done(context.Background())
}

func TestJobDeleteFromTx(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	// get the job's database connection
	conn := j.Conn()
	if conn == nil {
		t.Fatal("wanted conn, got nil")
	}

	// start a transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// delete the job
	if err = j.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	// mark as done
	j.Done(context.Background())

	// make sure the job is gone
	j2, err := findOneJob(context.Background(), c.pool)
	if err != nil {
		t.Fatal(err)
	}

	if j2 != nil {
		t.Errorf("wanted no job, got %+v", j2)
	}
}

func TestJobDeleteFromTxRollback(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j1, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j1 == nil {
		t.Fatal("wanted job, got none")
	}

	// get the job's database connection
	conn := j1.Conn()
	if conn == nil {
		t.Fatal("wanted conn, got nil")
	}

	// start a transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// delete the job
	if err = j1.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	// mark as done
	j1.Done(context.Background())

	// make sure the job still exists and matches j1
	j2, err := findOneJob(context.Background(), c.pool)
	if err != nil {
		t.Fatal(err)
	}

	if j1.ID != j2.ID {
		t.Errorf("want job %d, got %d", j1.ID, j2.ID)
	}
}

func TestJobError(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done(context.Background())

	msg := "world\nended"
	if err = j.Error(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	j.Done(context.Background())

	// make sure job was not deleted
	j2, err := findOneJob(context.Background(), c.pool)
	if err != nil {
		t.Fatal(err)
	}
	if j2 == nil {
		t.Fatal("job was not found")
	}
	defer j2.Done(context.Background())

	if !j2.LastError.Valid || j2.LastError.String != msg {
		t.Errorf("want LastError=%q, got %q", msg, j2.LastError.String)
	}
	if j2.ErrorCount != 1 {
		t.Errorf("want ErrorCount=%d, got %d", 1, j2.ErrorCount)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.pool.QueryRow(context.Background(), query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, idle := stat.TotalConns(), stat.IdleConns()
	if total != idle {
		t.Errorf("want idle=total, got idle=%d total=%d", idle, total)
	}
}

func TestJobErrorWithRunAt(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(context.Background(), c.pool)

	if err := c.Enqueue(context.Background(), &Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done(context.Background())

	runAt := time.Now().Add(2 * time.Hour)

	msg := "world\nended"
	if err = j.ErrorRunAt(context.Background(), msg, runAt); err != nil {
		t.Fatal(err)
	}
	j.Done(context.Background())

	// make sure job was not deleted
	j2, err := findOneJob(context.Background(), c.pool)
	if err != nil {
		t.Fatal(err)
	}
	if j2 == nil {
		t.Fatal("job was not found")
	}
	defer j2.Done(context.Background())

	if !j2.LastError.Valid || j2.LastError.String != msg {
		t.Errorf("want LastError=%q, got %q", msg, j2.LastError.String)
	}
	if j2.ErrorCount != 1 {
		t.Errorf("want ErrorCount=%d, got %d", 1, j2.ErrorCount)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.pool.QueryRow(context.Background(), query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, idle := stat.TotalConns(), stat.IdleConns()
	if total != idle {
		t.Errorf("want idle=total, got idle=%d total=%d", idle, total)
	}
}
