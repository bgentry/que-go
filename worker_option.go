package que

import "time"

// WorkerOption defines a type that allows to set worker properties during the build-time.
type WorkerOption func(*Worker)

// WorkerPoolOption defines a type that allows to set worker pool properties during the build-time.
type WorkerPoolOption func(pool *WorkerPool)

// WakeInterval overrides default wake interval with the given value.
func WakeInterval(d time.Duration) WorkerOption {
	return func(w *Worker) {
		w.Interval = d
	}
}

// WorkerQueue overrides default worker queue name with the given value.
func WorkerQueue(queue string) WorkerOption {
	return func(w *Worker) {
		w.Queue = queue
	}
}

// PoolWakeInterval overrides default wake interval with the given value.
func PoolWakeInterval(d time.Duration) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.Interval = d
	}
}

// PoolWorkerQueue overrides default worker queue name with the given value.
func PoolWorkerQueue(queue string) WorkerPoolOption {
	return func(w *WorkerPool) {
		w.Queue = queue
	}
}
