package que

import "testing"

func TestLockJob(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	if err := c.Enqueue(Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}

	if j.conn == nil {
		t.Fatal("want non-nil conn on locked Job")
	}
	if j.pool == nil {
		t.Fatal("want non-nil pool on locked Job")
	}
	defer j.Done()

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
	if want := "[]"; j.Args != want {
		t.Errorf("want Args=%s, got %s", want, j.Args)
	}
	if want := int32(0); j.ErrorCount != want {
		t.Errorf("want ErrorCount=%d, got %d", want, j.ErrorCount)
	}
	if j.LastError.Valid {
		t.Errorf("want no LastError, got %s", j.LastError)
	}

	// check for advisory lock
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = j.pool.QueryRow(query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("want 1 advisory lock, got %d", count)
	}

	// make sure conn was checked out of pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	if want := total - 1; available != want {
		t.Errorf("want available=%d, got %d", want, available)
	}
}

func TestLockJobAlreadyLocked(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	if err := c.Enqueue(Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}
	defer j.Done()

	j2, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j2 != nil {
		defer j2.Done()
		t.Fatalf("wanted no job, got %+v", j2)
	}
}

func TestLockJobNoJob(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		t.Errorf("want no job, got %v", j)
	}
}

func TestLockJobCustomQueue(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	if err := c.Enqueue(Job{Type: "MyJob", Queue: "extra_priority"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j != nil {
		j.Done()
		t.Errorf("expected no job to be found with empty queue name, got %+v", j)
	}

	j, err = c.LockJob("extra_priority")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Done()

	if j == nil {
		t.Fatal("wanted job, got none")
	}
}

func TestJobDone(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	if err := c.Enqueue(Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	if err = j.Done(); err != nil {
		t.Fatal(err)
	}
	// make sure conn and pool were cleared
	if j.conn != nil {
		t.Errorf("want nil conn, got %+v", j.conn)
	}
	if j.pool != nil {
		t.Errorf("want nil pool, got %+v", j.pool)
	}

	// make sure job was deleted
	j2, err := findOneJob(c.pool)
	if err != nil {
		t.Fatal(err)
	}
	if j2 != nil {
		t.Errorf("job was not deleted: %+v", j2)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.pool.QueryRow(query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	if total != available {
		t.Errorf("want available=total, got available=%d total=%d", available, total)
	}
}

func TestJobDoneMultiple(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	if err := c.Enqueue(Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	if err = j.Done(); err != nil {
		t.Fatal(err)
	}
	// try calling Done() again
	if err = j.Done(); err != nil {
		t.Fatal(err)
	}
}

func TestJobError(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	if err := c.Enqueue(Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	j, err := c.LockJob("")
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("wanted job, got none")
	}

	msg := "world\nended"
	if err = j.Error(msg); err != nil {
		t.Fatal(err)
	}
	// make sure conn and pool were cleared
	if j.conn != nil {
		t.Errorf("want nil conn, got %+v", j.conn)
	}
	if j.pool != nil {
		t.Errorf("want nil pool, got %+v", j.pool)
	}

	// make sure job was not deleted
	j2, err := findOneJob(c.pool)
	if err != nil {
		t.Fatal(err)
	}
	if j2 == nil {
		t.Fatal("job was not found")
	}
	if !j2.LastError.Valid || j2.LastError.String != msg {
		t.Errorf("want LastError=%q, got %q", msg, j2.LastError.String)
	}
	if j2.ErrorCount != 1 {
		t.Errorf("want ErrorCount=%d, got %d", 1, j2.ErrorCount)
	}

	// make sure lock was released
	var count int64
	query := "SELECT count(*) FROM pg_locks WHERE locktype=$1 AND objid=$2::bigint"
	if err = c.pool.QueryRow(query, "advisory", j.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Error("advisory lock was not released")
	}

	// make sure conn was returned to pool
	stat := c.pool.Stat()
	total, available := stat.CurrentConnections, stat.AvailableConnections
	if total != available {
		t.Errorf("want available=total, got available=%d total=%d", available, total)
	}
}
