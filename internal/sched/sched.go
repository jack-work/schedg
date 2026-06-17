// Package sched is a dependency-aware priority scheduler over priority.Task.
//
// State partition (S1): every submitted id is in exactly one of
// {blocked, ready, inflight, dead, completed}. Ready is a max-heap; blocked
// tasks drain into ready as their deps complete (Kahn). Cycles are refused at
// Submit.
//
// Lifecycle of a leased task: Next leases it (ready -> inflight, attempt++).
// From inflight it can Complete (success, terminal), Cancel (release back to
// ready, cancel++; auto-buries once cancels reach maxCancels), or Fail (bury
// now, terminal). A buried task can be Requeued (kick back to ready). An
// expired lease (ExpireLeases) is treated as a Cancel.
package sched

import (
	"fmt"
	"sort"
	"time"

	"github.com/jack-work/schedg/internal/heap"
	"github.com/jack-work/schedg/internal/priority"
)

// Meta is per-task runtime bookkeeping, keyed by id so it survives a task moving
// between sets and survives a source-drift rebuild (where ready/blocked tasks
// are re-submitted from fresh rows).
type Meta struct {
	Attempts int       `json:"attempts,omitempty"` // times leased via Next
	Cancels  int       `json:"cancels,omitempty"`  // times released or lease-expired
	LeasedAt time.Time `json:"leased_at,omitempty"`
	Reason   string    `json:"reason,omitempty"` // last cancel/fail reason
	Caller   string    `json:"caller,omitempty"` // who leased this (e.g. angl name)
}

type Scheduler struct {
	cmp        priority.Comparator
	maxCancels int // 0 => unlimited (never auto-bury on cancel)
	ready      *heap.Heap[priority.Task]
	blocked    map[string]priority.Task
	inflight   map[string]priority.Task
	dead       map[string]priority.Task
	completed  map[string]priority.Task
	dependents map[string][]string
	inDegree   map[string]int
	meta       map[string]Meta
	nextSeq    int64
	now        func() time.Time
}

type Snapshot struct {
	Ready      []priority.Task          `json:"ready"`
	Blocked    map[string]priority.Task `json:"blocked"`
	Inflight   map[string]priority.Task `json:"inflight"`
	Dead       map[string]priority.Task `json:"dead"`
	Completed  map[string]priority.Task `json:"completed"`
	Dependents map[string][]string      `json:"dependents"`
	InDegree   map[string]int           `json:"in_degree"`
	Meta       map[string]Meta          `json:"meta"`
	NextSeq    int64                    `json:"next_seq"`
}

func New(cmp priority.Comparator, maxCancels int) *Scheduler {
	return &Scheduler{
		cmp:        cmp,
		maxCancels: maxCancels,
		ready:      heap.New(cmp.Compare, nil),
		blocked:    map[string]priority.Task{},
		inflight:   map[string]priority.Task{},
		dead:       map[string]priority.Task{},
		completed:  map[string]priority.Task{},
		dependents: map[string][]string{},
		inDegree:   map[string]int{},
		meta:       map[string]Meta{},
		now:        time.Now,
	}
}

// Restore rebuilds an exact scheduler from a snapshot (no source read).
func Restore(cmp priority.Comparator, s Snapshot, maxCancels int) *Scheduler {
	sc := New(cmp, maxCancels)
	sc.ready = heap.New(cmp.Compare, s.Ready)
	if s.Blocked != nil {
		sc.blocked = s.Blocked
	}
	if s.Inflight != nil {
		sc.inflight = s.Inflight
	}
	if s.Dead != nil {
		sc.dead = s.Dead
	}
	if s.Completed != nil {
		sc.completed = s.Completed
	}
	if s.Dependents != nil {
		sc.dependents = s.Dependents
	}
	if s.InDegree != nil {
		sc.inDegree = s.InDegree
	}
	if s.Meta != nil {
		sc.meta = s.Meta
	}
	sc.nextSeq = s.NextSeq
	return sc
}

// SeedCompleted marks ids complete without unblocking (used when rebuilding
// after source drift so deps on done work are filtered at Submit).
func (s *Scheduler) SeedCompleted(completed map[string]priority.Task) {
	for id, t := range completed {
		s.completed[id] = t
	}
}

// SeedInflight restores in-flight tasks across a rebuild.
func (s *Scheduler) SeedInflight(m map[string]priority.Task) { s.seedSet(s.inflight, m) }

// SeedDead restores buried tasks across a rebuild.
func (s *Scheduler) SeedDead(m map[string]priority.Task) { s.seedSet(s.dead, m) }

// SeedMeta restores per-task counters across a rebuild.
func (s *Scheduler) SeedMeta(m map[string]Meta) {
	for id, v := range m {
		s.meta[id] = v
	}
}

func (s *Scheduler) seedSet(dst, src map[string]priority.Task) {
	for id, t := range src {
		dst[id] = t
		if t.Seq >= s.nextSeq {
			s.nextSeq = t.Seq + 1
		}
	}
}

func (s *Scheduler) Has(id string) bool {
	if _, ok := s.blocked[id]; ok {
		return true
	}
	if _, ok := s.inflight[id]; ok {
		return true
	}
	if _, ok := s.dead[id]; ok {
		return true
	}
	if _, ok := s.completed[id]; ok {
		return true
	}
	for _, t := range s.ready.Items() {
		if t.ID == id {
			return true
		}
	}
	return false
}

func (s *Scheduler) Completed(id string) bool {
	_, ok := s.completed[id]
	return ok
}
func (s *Scheduler) Buried(id string) bool    { _, ok := s.dead[id]; return ok }

// Submit accepts a task with optional dependency ids. Deps already completed
// are dropped. Returns an error if the edges would close a cycle. Existing
// per-task counters (meta) are preserved.
func (s *Scheduler) Submit(t priority.Task, deps []string) error {
	t.Seq = s.nextSeq
	s.nextSeq++
	if _, ok := s.meta[t.ID]; !ok {
		s.meta[t.ID] = Meta{}
	}

	var eff []string
	for _, d := range deps {
		if _, done := s.completed[d]; !done {
			eff = append(eff, d)
		}
	}

	if s.createsCycle(t.ID, eff) {
		return fmt.Errorf("submitting %s with deps %v would create a cycle", t.ID, eff)
	}

	if len(eff) == 0 {
		s.ready.Push(t)
		return nil
	}
	s.blocked[t.ID] = t
	s.inDegree[t.ID] = len(eff)
	for _, d := range eff {
		s.dependents[d] = append(s.dependents[d], t.ID)
	}
	return nil
}

// createsCycle reports whether adding edges dep->id closes a loop: walk forward
// through dependents from id; if we reach any dep, id->...->dep->id would cycle.
func (s *Scheduler) createsCycle(id string, deps []string) bool {
	if len(deps) == 0 {
		return false
	}
	targets := map[string]bool{}
	for _, d := range deps {
		targets[d] = true
	}
	seen := map[string]bool{}
	var reaches func(string) bool
	reaches = func(cur string) bool {
		if targets[cur] {
			return true
		}
		if seen[cur] {
			return false
		}
		seen[cur] = true
		for _, w := range s.dependents[cur] {
			if reaches(w) {
				return true
			}
		}
		return false
	}
	return reaches(id)
}

// Next leases the highest-priority ready task (ready -> inflight).
func (s *Scheduler) Next() (priority.Task, bool) {
	return s.NextFor("")
}

func (s *Scheduler) NextFor(caller string) (priority.Task, bool) {
	t, ok := s.ready.Pop()
	if !ok {
		return t, false
	}
	s.inflight[t.ID] = t
	m := s.meta[t.ID]
	m.Attempts++
	m.LeasedAt = s.now()
	m.Reason = ""
	m.Caller = caller
	s.meta[t.ID] = m
	return t, true
}

func (s *Scheduler) Peek() (priority.Task, bool) { return s.ready.Peek() }

func (s *Scheduler) Complete(id string) error {
	t, ok := s.inflight[id]
	if !ok {
		return fmt.Errorf("complete(%s): task is not in-flight", id)
	}
	delete(s.inflight, id)
	s.completed[id] = t
	m := s.meta[id]
	m.LeasedAt = time.Time{}
	s.meta[id] = m

	waiters := s.dependents[id]
	delete(s.dependents, id)
	for _, w := range waiters {
		if _, ok := s.inDegree[w]; !ok {
			continue
		}
		s.inDegree[w]--
		if s.inDegree[w] == 0 {
			t := s.blocked[w]
			delete(s.blocked, w)
			delete(s.inDegree, w)
			s.ready.Push(t)
		}
	}
	return nil
}

// Cancel releases an in-flight task that could not be completed now: it returns
// to ready and its cancel count increments. Once the count reaches maxCancels
// (when set) the task is buried instead. Returns whether it was buried.
func (s *Scheduler) Cancel(id, reason string) (buried bool, err error) {
	t, ok := s.inflight[id]
	if !ok {
		return false, fmt.Errorf("cancel(%s): task is not in-flight", id)
	}
	delete(s.inflight, id)
	m := s.meta[id]
	m.Cancels++
	m.LeasedAt = time.Time{}
	m.Reason = reason
	s.meta[id] = m

	if s.maxCancels > 0 && m.Cancels >= s.maxCancels {
		s.dead[id] = t
		return true, nil
	}
	s.ready.Push(t)
	return false, nil
}

// Fail buries an in-flight task (terminal). Its dependents stay blocked and
// surface in Status; revive with Requeue.
func (s *Scheduler) Fail(id, reason string) error {
	t, ok := s.inflight[id]
	if !ok {
		return fmt.Errorf("fail(%s): task is not in-flight", id)
	}
	delete(s.inflight, id)
	m := s.meta[id]
	m.Reason = reason
	m.LeasedAt = time.Time{}
	s.meta[id] = m
	s.dead[id] = t
	return nil
}

// Requeue kicks a buried task back to ready and resets its cancel count so it
// gets a fresh set of attempts.
func (s *Scheduler) Requeue(id string) error {
	t, ok := s.dead[id]
	if !ok {
		return fmt.Errorf("requeue(%s): task is not buried", id)
	}
	delete(s.dead, id)
	m := s.meta[id]
	m.Cancels = 0
	m.Reason = ""
	s.meta[id] = m
	s.ready.Push(t)
	return nil
}

// ExpireLeases cancels in-flight tasks whose lease is older than ttl (treating a
// vanished worker as a cancel). Returns the ids expired. ttl<=0 is a no-op.
func (s *Scheduler) ExpireLeases(ttl time.Duration) []string {
	if ttl <= 0 {
		return nil
	}
	now := s.now()
	var expired []string
	for id := range s.inflight {
		m := s.meta[id]
		if m.LeasedAt.IsZero() {
			continue
		}
		if now.Sub(m.LeasedAt) > ttl {
			expired = append(expired, id)
		}
	}
	sort.Strings(expired)
	for _, id := range expired {
		s.Cancel(id, "lease expired")
	}
	return expired
}

type Status struct {
	Ready     int `json:"ready"`
	Blocked   int `json:"blocked"`
	Inflight  int `json:"inflight"`
	Dead      int `json:"dead"`
	Completed int `json:"completed"`
}

func (s *Scheduler) Status() Status {
	return Status{
		Ready:     s.ready.Len(),
		Blocked:   len(s.blocked),
		Inflight:  len(s.inflight),
		Dead:      len(s.dead),
		Completed: len(s.completed),
	}
}

func (s *Scheduler) ReadyTasks() []priority.Task {
	out := append([]priority.Task(nil), s.ready.Items()...)
	sort.SliceStable(out, func(i, j int) bool { return s.cmp.Compare(out[i], out[j]) > 0 })
	return out
}

func (s *Scheduler) InflightTasks() map[string]priority.Task { return s.inflight }
func (s *Scheduler) DeadTasks() map[string]priority.Task     { return s.dead }
func (s *Scheduler) BlockedAllTasks() map[string]priority.Task { return s.blocked }
func (s *Scheduler) Meta(id string) Meta                     { return s.meta[id] }

// RefreshFields updates the Fields map on all live tasks from a fresh source
// read. This patches tasks restored from a snapshot that may predate newly
// loaded columns (e.g. description).
func (s *Scheduler) RefreshFields(fieldMap map[string]map[string]string) {
	for i := range s.ready.Items() {
		if f, ok := fieldMap[s.ready.Items()[i].ID]; ok {
			s.ready.Items()[i].Fields = f
		}
	}
	for id, t := range s.blocked {
		if f, ok := fieldMap[id]; ok {
			t.Fields = f
			s.blocked[id] = t
		}
	}
	for id, t := range s.inflight {
		if f, ok := fieldMap[id]; ok {
			t.Fields = f
			s.inflight[id] = t
		}
	}
	for id, t := range s.dead {
		if f, ok := fieldMap[id]; ok {
			t.Fields = f
			s.dead[id] = t
		}
	}
	for id, t := range s.completed {
		if f, ok := fieldMap[id]; ok {
			t.Fields = f
			s.completed[id] = t
		}
	}
}

func (s *Scheduler) CompletedIDs() []string {
	out := make([]string, 0, len(s.completed))
	for id := range s.completed {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// BlockedTasks maps a blocked id to its still-unmet dependency ids.
func (s *Scheduler) BlockedTasks() map[string][]string {
	out := map[string][]string{}
	for dep, waiters := range s.dependents {
		for _, w := range waiters {
			if _, blocked := s.blocked[w]; blocked {
				out[w] = append(out[w], dep)
			}
		}
	}
	return out
}

// CompletedTasks returns the full task data for completed items.
func (s *Scheduler) CompletedTasks() map[string]priority.Task { return s.completed }

func (s *Scheduler) Snapshot() Snapshot {
	return Snapshot{
		Ready:      append([]priority.Task(nil), s.ready.Items()...),
		Blocked:    s.blocked,
		Inflight:   s.inflight,
		Dead:       s.dead,
		Completed:  s.completed,
		Dependents: s.dependents,
		InDegree:   s.inDegree,
		Meta:       s.meta,
		NextSeq:    s.nextSeq,
	}
}
