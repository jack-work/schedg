package sched

import (
	"testing"
	"time"

	"github.com/jack-work/schedg/internal/priority"
)

func task(id string, prio int64) priority.Task {
	return priority.Task{ID: id, Priority: prio, Submitted: time.Unix(int64(len(id)), 0)}
}

func drain(s *Scheduler) []string {
	var out []string
	for {
		t, ok := s.Next()
		if !ok {
			break
		}
		out = append(out, t.ID)
	}
	return out
}

func TestPriorityOrder(t *testing.T) {
	s := New(priority.Default(), 0)
	s.Submit(task("a", 1), nil)
	s.Submit(task("b", 5), nil)
	s.Submit(task("c", 3), nil)
	got := drain(s)
	want := []string{"b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestDependencyUnblocks(t *testing.T) {
	s := New(priority.Default(), 0)
	s.Submit(task("root", 1), nil)
	if err := s.Submit(task("child", 9), []string{"root"}); err != nil {
		t.Fatal(err)
	}
	// child has higher priority but is blocked until root completes.
	first, _ := s.Next()
	if first.ID != "root" {
		t.Fatalf("first = %s, want root (child must be blocked)", first.ID)
	}
	if err := s.Complete("root"); err != nil {
		t.Fatal(err)
	}
	second, ok := s.Next()
	if !ok || second.ID != "child" {
		t.Fatalf("second = %v,%v want child", second.ID, ok)
	}
}

func TestCompletedDepIsDropped(t *testing.T) {
	s := New(priority.Default(), 0)
	s.Submit(task("a", 1), nil)
	a, _ := s.Next()
	if a.ID != "a" {
		t.Fatal("expected a")
	}
	s.Complete("a")
	// b depends on already-complete a => should be ready immediately.
	s.Submit(task("b", 1), []string{"a"})
	b, ok := s.Next()
	if !ok || b.ID != "b" {
		t.Fatalf("b not ready despite completed dep: %v,%v", b.ID, ok)
	}
}

func TestCycleRefused(t *testing.T) {
	s := New(priority.Default(), 0)
	if err := s.Submit(task("a", 1), []string{"b"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(task("b", 1), []string{"a"}); err == nil {
		t.Fatal("expected cycle error submitting b->a after a->b")
	}
}

func TestCancelRequeuesAndCounts(t *testing.T) {
	s := New(priority.Default(), 0) // unlimited cancels
	s.Submit(task("a", 1), nil)
	a, _ := s.Next()
	buried, err := s.Cancel(a.ID, "worker died")
	if err != nil {
		t.Fatal(err)
	}
	if buried {
		t.Fatal("should not bury with maxCancels=0")
	}
	// Reason and counts are set immediately after the cancel.
	if m := s.Meta("a"); m.Cancels != 1 || m.Attempts != 1 || m.Reason != "worker died" {
		t.Fatalf("meta = %+v, want cancels=1 attempts=1 reason set", m)
	}
	again, ok := s.Next()
	if !ok || again.ID != "a" {
		t.Fatalf("cancelled task not returned to ready: %v,%v", again.ID, ok)
	}
	// Re-leasing bumps the attempt and clears the stale reason.
	if m := s.Meta("a"); m.Attempts != 2 || m.Reason != "" {
		t.Fatalf("meta = %+v, want attempts=2 reason cleared", m)
	}
}

func TestCancelBuriesAtMax(t *testing.T) {
	s := New(priority.Default(), 2) // bury on 2nd cancel
	s.Submit(task("a", 1), nil)

	s.Next()
	if buried, _ := s.Cancel("a", "1"); buried {
		t.Fatal("first cancel should requeue, not bury")
	}
	s.Next()
	buried, _ := s.Cancel("a", "2")
	if !buried {
		t.Fatal("second cancel should bury")
	}
	if !s.Buried("a") {
		t.Fatal("a should be in dead-letter")
	}
	if _, ok := s.Next(); ok {
		t.Fatal("buried task must not be dispatched")
	}
}

func TestFailBuriesAndRequeueRevives(t *testing.T) {
	s := New(priority.Default(), 0)
	s.Submit(task("a", 1), nil)
	s.Next()
	if err := s.Fail("a", "broken"); err != nil {
		t.Fatal(err)
	}
	if !s.Buried("a") {
		t.Fatal("fail should bury")
	}
	if _, ok := s.Next(); ok {
		t.Fatal("buried task must not be dispatched")
	}
	if err := s.Requeue("a"); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Next()
	if !ok || got.ID != "a" {
		t.Fatalf("requeue should return task to ready: %v,%v", got.ID, ok)
	}
	if m := s.Meta("a"); m.Cancels != 0 {
		t.Fatalf("requeue should reset cancels, got %d", m.Cancels)
	}
}

func TestExpireLeases(t *testing.T) {
	clock := time.Unix(1000, 0)
	s := New(priority.Default(), 0)
	s.now = func() time.Time { return clock }
	s.Submit(task("a", 1), nil)
	s.Next() // leased at t=1000

	clock = time.Unix(1005, 0)
	if ex := s.ExpireLeases(10 * time.Second); len(ex) != 0 {
		t.Fatalf("nothing should expire yet, got %v", ex)
	}
	clock = time.Unix(1020, 0)
	ex := s.ExpireLeases(10 * time.Second)
	if len(ex) != 1 || ex[0] != "a" {
		t.Fatalf("expected a to expire, got %v", ex)
	}
	// An expired lease counts as a cancel with a synthetic reason.
	if m := s.Meta("a"); m.Cancels != 1 || m.Reason != "lease expired" {
		t.Fatalf("meta = %+v, want cancels=1 reason='lease expired'", m)
	}
	got, ok := s.Next()
	if !ok || got.ID != "a" {
		t.Fatalf("expired lease should return task to ready: %v,%v", got.ID, ok)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := New(priority.Default(), 3)
	s.Submit(task("root", 1), nil)
	s.Submit(task("child", 2), []string{"root"})
	s.Next() // root in-flight
	s.Cancel("root", "retry")
	s.Next() // root in-flight again (attempt 2, cancels 1)

	snap := s.Snapshot()
	s2 := Restore(priority.Default(), snap, 3)

	if m := s2.Meta("root"); m.Attempts != 2 || m.Cancels != 1 {
		t.Fatalf("counters lost across restore: %+v", m)
	}
	if err := s2.Complete("root"); err != nil {
		t.Fatalf("complete after restore: %v", err)
	}
	c, ok := s2.Next()
	if !ok || c.ID != "child" {
		t.Fatalf("child not unblocked after restore+complete: %v,%v", c.ID, ok)
	}
}
