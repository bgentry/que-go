package que

import (
	"testing"

	"github.com/jackc/pgx"
)

func openTestClient(t testing.TB) *Client {
	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "localhost",
			Database: "que-go-test",
		},
		MaxConnections: 5,
	}
	pool, err := pgx.NewConnPool(connPoolConfig)
	if err != nil {
		t.Fatal(err)
	}
	return NewClient(pool)
}

func truncateAndClose(pool *pgx.ConnPool) {
	if _, err := pool.Exec("TRUNCATE TABLE que_jobs"); err != nil {
		panic(err)
	}
	pool.Close()
}

func findOneJob(pool *pgx.ConnPool) (*Job, error) {
	findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

	j := &Job{}
	err := pool.QueryRow(findSQL).Scan(
		&j.Priority,
		&j.RunAt,
		&j.ID,
		&j.Type,
		&j.Args,
		&j.ErrorCount,
		&j.LastError,
		&j.Queue,
	)
	return j, err
}
