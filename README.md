# que-go

[![GoDoc](https://godoc.org/github.com/bgentry/que-go?status.svg)][godoc]

Que-go is a fully interoperable Golang port of [Chris Hanks][chanks]' [Ruby Que
queuing library][ruby-que] for PostgreSQL. Que uses PostgreSQL's advisory locks
for speed and reliability.

## WIP

This is a very early work-in-progress, so I make no guarantees about stability.

## pgx PostgreSQL driver

This package uses the [pgx][pgx] Go PostgreSQL driver rather than the more
popular [pq][pq]. Because Que uses session-level advisory locks, we have to hold
the same connection throughout the process of getting a job, working it,
deleting it, and removing the lock.

Pq and the built-in database/sql interfaces do not offer this functionality, so
we'd have to implement our own connection pool. Fortunately, pgx already has a
perfectly usable one built for us. Even better, it offers better performance
than pq due largely to its use of binary encoding.

[godoc]: https://godoc.org/github.com/bgentry/que-go
[chanks]: https://github.com/chanks
[ruby-que]: https://github.com/chanks/que
[pgx]: https://github.com/jackc/pgx
[pq]: https://github.com/lib/pq
