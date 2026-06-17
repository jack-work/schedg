// Package schedg is the exported Go library for creating and operating
// dependency-aware priority queues backed by SQL (SQLite, Dolt, or any
// future database/sql driver).
//
// Quick start (SQLite):
//
//	db, _ := schedg.Init(ctx, schedg.Options{Path: "tasks.db"})
//	defer db.Close()
//	id, _ := db.Add(ctx, "fix the build", schedg.TaskOpts{Priority: 5})
//	task, _ := db.Next()
//	db.Complete(task.ID)
//	db.Save()
package schedg

import (
	"context"
	"time"

	"github.com/jack-work/schedg/internal/config"
	"github.com/jack-work/schedg/internal/priority"
	"github.com/jack-work/schedg/internal/queue"
	"github.com/jack-work/schedg/internal/sched"
	"github.com/jack-work/schedg/internal/source"
)

// Options configures a schedg database.
type Options struct {
	Driver     string        // "sqlite" (default) or "dolt"
	Path       string        // DB file path (sqlite) or data-dir (dolt)
	StatePath  string        // queue state file; empty = <Path>.state.json
	Name       string        // queue name; empty = derived from Path
	Comparator string        // priority module; empty = default
	MaxCancels int           // auto-bury threshold; 0 = unlimited
	LeaseTTL   time.Duration // auto-cancel idle leases; 0 = no expiry
}

// Task is a todo item as exposed by the library.
type Task struct {
	ID          string
	Title       string
	Description string
	Priority    int64
	Submitted   time.Time
}

// TaskOpts carries optional fields for Add and Update.
type TaskOpts struct {
	Description string
	Priority    int64
	ParentID    string
}

// Status counts tasks in each lifecycle state.
type Status struct {
	Ready     int
	Blocked   int
	Inflight  int
	Dead      int
	Completed int
}

// DB is an open schedg queue database.
type DB struct {
	q *queue.Queue
}

func toConfigDB(opts Options) config.DB {
	driver := opts.Driver
	if driver == "" {
		driver = "sqlite"
	}
	name := opts.Name
	if name == "" {
		name = opts.Path
	}
	statePath := opts.StatePath
	if statePath == "" {
		statePath = opts.Path + ".state.json"
	}
	db := config.DB{
		Name:       name,
		Driver:     driver,
		Path:       opts.Path,
		Comparator: opts.Comparator,
		MaxCancels: opts.MaxCancels,
		StatePath:  statePath,
	}
	if opts.LeaseTTL > 0 {
		db.LeaseTTL = opts.LeaseTTL.String()
	}
	return db
}

// Init creates the database schema (idempotent) and returns an open DB. Use
// this for first-time setup or when you want to ensure tables exist.
func Init(ctx context.Context, opts Options) (*DB, error) {
	cdb := toConfigDB(opts)
	q, err := queue.Open(cdb)
	if err != nil {
		return nil, err
	}
	if err := q.Init(ctx, ""); err != nil {
		q.Close()
		return nil, err
	}
	if err := q.Save(); err != nil {
		q.Close()
		return nil, err
	}
	return &DB{q: q}, nil
}

// Open opens an existing database without running schema init.
func Open(opts Options) (*DB, error) {
	cdb := toConfigDB(opts)
	q, err := queue.Open(cdb)
	if err != nil {
		return nil, err
	}
	return &DB{q: q}, nil
}

// --- CRUD ---

// Add creates a new task in the database and syncs the queue.
func (db *DB) Add(ctx context.Context, title string, opts TaskOpts) (string, error) {
	return db.q.Add(ctx, title, toSourceOpts(opts))
}

// Update replaces mutable fields on an existing task.
func (db *DB) Update(ctx context.Context, id, title string, opts TaskOpts) error {
	return db.q.Update(ctx, id, title, toSourceOpts(opts))
}

// Remove deletes a task from the database.
func (db *DB) Remove(ctx context.Context, id string) error {
	return db.q.Remove(ctx, id)
}

// Done marks a task as done in the database (sets done=1).
func (db *DB) Done(ctx context.Context, id string) error {
	return db.q.SetDone(ctx, id)
}

// AddDep records that taskID depends on depID.
func (db *DB) AddDep(ctx context.Context, taskID, depID string) error {
	return db.q.AddDep(ctx, taskID, depID)
}

// RemoveDep removes a dependency edge.
func (db *DB) RemoveDep(ctx context.Context, taskID, depID string) error {
	return db.q.RemoveDep(ctx, taskID, depID)
}

// --- queue operations ---

// Next leases the highest-priority ready task (ready -> in-flight).
func (db *DB) Next() (Task, bool) {
	t, ok := db.q.Next()
	return fromInternal(t), ok
}

// Peek shows the highest-priority ready task without leasing it.
func (db *DB) Peek() (Task, bool) {
	t, ok := db.q.Peek()
	return fromInternal(t), ok
}

// Complete marks a leased (in-flight) task as successfully done in the queue
// and unblocks its dependents. This is a queue-level operation; use Done() to
// also set done=1 in the database.
func (db *DB) Complete(id string) error { return db.q.Complete(id) }

// Cancel releases a leased task back to ready, incrementing its cancel count.
// Returns true if the task was buried (reached max-cancels).
func (db *DB) Cancel(id, reason string) (buried bool, err error) { return db.q.Cancel(id, reason) }

// Fail buries a leased task to the dead-letter set (terminal).
func (db *DB) Fail(id, reason string) error { return db.q.Fail(id, reason) }

// Requeue kicks a buried task back to ready (resets cancel count).
func (db *DB) Requeue(id string) error { return db.q.Requeue(id) }

// Status returns counts for each lifecycle state.
func (db *DB) Status() Status {
	s := db.q.Status()
	return Status{
		Ready:     s.Ready,
		Blocked:   s.Blocked,
		Inflight:  s.Inflight,
		Dead:      s.Dead,
		Completed: s.Completed,
	}
}

// Ready returns all ready tasks in priority order.
func (db *DB) Ready() []Task {
	internal := db.q.Ready()
	out := make([]Task, len(internal))
	for i, t := range internal {
		out[i] = fromInternal(t)
	}
	return out
}

// Save persists the current queue state to the state file.
func (db *DB) Save() error { return db.q.Save() }

// Close releases the database connection.
func (db *DB) Close() error { return db.q.Close() }

// --- conversions ---

func fromInternal(t priority.Task) Task {
	var title, desc string
	if t.Fields != nil {
		title = t.Fields["label"]
		desc = t.Fields["description"]
	}
	return Task{
		ID:          t.ID,
		Title:       title,
		Description: desc,
		Priority:    t.Priority,
		Submitted:   t.Submitted,
	}
}

func toSourceOpts(o TaskOpts) source.TaskOpts {
	return source.TaskOpts{
		Description: o.Description,
		Priority:    o.Priority,
		ParentID:    o.ParentID,
	}
}

// Force registration of all built-in source drivers.
var _ = func() int {
	// The imports above pull in internal/queue -> internal/source, which
	// triggers init() in dolt.go and sqlite.go. This blank reference
	// silences "imported and not used" if the compiler gets clever.
	_ = sched.Status{}
	return 0
}()
