package que

import "testing"

func TestWorkerWorkOne(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	success := false
	wm := WorkMap{
		"MyJob": func(j *Job) error {
			success = true
			return nil
		},
	}
	w := NewWorker(c, wm)

	didWork := w.workOne()
	if didWork {
		t.Errorf("want didWork=false when no job was queued")
	}

	if err := c.Enqueue(&Job{Type: "MyJob"}); err != nil {
		t.Fatal(err)
	}

	didWork = w.workOne()
	if !didWork {
		t.Errorf("want didWork=true")
	}
	if !success {
		t.Errorf("want success=true")
	}
}

func TestWorkerShutdown(t *testing.T) {
	c := openTestClient(t)
	defer truncateAndClose(c.pool)

	w := NewWorker(c, WorkMap{})
	finished := false
	go func() {
		w.Work()
		finished = true
	}()
	w.Shutdown()
	if !finished {
		t.Errorf("want finished=true")
	}
	if !w.done {
		t.Errorf("want w.done=true")
	}
}
