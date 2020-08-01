package que

import (
    "context"
    "testing"

    "github.com/jackc/pgx/v4"
    "github.com/jackc/pgx/v4/pgxpool"
)

var testConnConfig, _ = pgx.ParseConfig("postgres://postgres:postgres@localhost/que-go-test")

func openTestClientMaxConns(t testing.TB, maxConnections int32) *Client {
    connPoolConfig, err := pgxpool.ParseConfig(testConnConfig.ConnString())
    if err != nil {
        t.Fatal(err)
    }
    connPoolConfig.MaxConns = maxConnections
    connPoolConfig.AfterConnect = PrepareStatements
    pool, err := pgxpool.ConnectConfig(context.Background(), connPoolConfig)
    if err != nil {
        t.Fatal(err)
    }
    return NewClient(pool)
}

func openTestClient(t testing.TB) *Client {
    return openTestClientMaxConns(t, 5)
}

func truncateAndClose(pool *pgxpool.Pool) {
    if _, err := pool.Exec(context.Background(), "TRUNCATE TABLE que_jobs"); err != nil {
        panic(err)
    }
    pool.Close()
}

func findOneJob(q queryable) (*Job, error) {
    findSQL := `
	SELECT priority, run_at, job_id, job_class, args, error_count, last_error, queue
	FROM que_jobs LIMIT 1`

    j := &Job{}
    err := q.QueryRow(context.Background(), findSQL).Scan(
        &j.Priority,
        &j.RunAt,
        &j.ID,
        &j.Type,
        &j.Args,
        &j.ErrorCount,
        &j.LastError,
        &j.Queue,
    )
    if err == pgx.ErrNoRows {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    return j, nil
}
