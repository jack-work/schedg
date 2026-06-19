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
	"fmt"
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
	KV          map[string]string
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

// QueueInfo describes a registered queue.
type QueueInfo struct {
	Name       string
	Driver     string
	Path       string
	Comparator string
}

// ListQueues returns all registered queues from the schedg config.
func ListQueues() ([]QueueInfo, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	out := make([]QueueInfo, len(cfg.DBs))
	for i, db := range cfg.DBs {
		cmp := db.Comparator
		if cmp == "" {
			cmp = "priority-submitted"
		}
		out[i] = QueueInfo{Name: db.Name, Driver: db.Driver, Path: db.Path, Comparator: cmp}
	}
	return out, nil
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

// OpenByName resolves a queue by name from the schedg config
// (~/.config/schedg/config.json) and opens it.
func OpenByName(name string) (*DB, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load schedg config: %w", err)
	}
	dbCfg, ok := cfg.Find(name)
	if !ok {
		return nil, fmt.Errorf("queue %q not found in schedg config", name)
	}
	q, err := queue.Open(*dbCfg)
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

// SetKV sets a key-value pair on a task (value max 500 chars, supports markdown).
func (db *DB) SetKV(ctx context.Context, taskID, key, value string) error {
	return db.q.SetKV(ctx, taskID, key, value)
}

// DeleteKV removes a key-value pair from a task.
func (db *DB) DeleteKV(ctx context.Context, taskID, key string) error {
	return db.q.DeleteKV(ctx, taskID, key)
}

// GetKV returns all key-value pairs for a task.
func (db *DB) GetKV(ctx context.Context, taskID string) (map[string]string, error) {
	return db.q.GetKV(ctx, taskID)
}

// SetDBMeta sets a database-level key-value pair.
func (db *DB) SetDBMeta(ctx context.Context, key, value string) error {
	return db.q.SetDBMeta(ctx, key, value)
}

// GetDBMeta returns all database-level key-value pairs.
func (db *DB) GetDBMeta(ctx context.Context) (map[string]string, error) {
	return db.q.GetDBMeta(ctx)
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

// NextFor leases the highest-priority ready task, recording the caller identity.
func (db *DB) NextFor(caller string) (Task, bool) {
	t, ok := db.q.NextFor(caller)
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

// Inflight returns all currently leased tasks.
func (db *DB) Inflight() []Task {
	internal := db.q.Inflight()
	out := make([]Task, 0, len(internal))
	for _, t := range internal {
		out = append(out, fromInternal(t))
	}
	return out
}

// Meta returns per-task metadata (attempts, cancels, caller, etc.).
func (db *DB) Meta(id string) sched.Meta { return db.q.Meta(id) }

// DepEdge represents a dependency relationship.
type DepEdge struct {
	From string
	To   string
}

// QueueSnapshot is a full snapshot of a queue's state for display.
type QueueSnapshot struct {
	Name       string
	Driver     string
	Ready      []Task
	Blocked    []Task
	Inflight   []Task
	Dead       []Task
	Completed  []Task
	Deps       []DepEdge
	BlockedBy  map[string][]string
	Counts     map[string]int
	DBMeta     map[string]string
	SnapshotAt time.Time
	// Per-task metadata
	Meta       map[string]TaskMeta
}

// TaskMeta holds per-task runtime bookkeeping.
type TaskMeta struct {
	Attempts int
	Cancels  int
	Reason   string
	LeasedAt time.Time
	Caller   string
}

// Snapshot returns a full snapshot of the queue's current state.
func (db *DB) Snapshot() QueueSnapshot {
	st := db.q.Status()
	snap := QueueSnapshot{
		Counts: map[string]int{
			"ready": st.Ready, "blocked": st.Blocked,
			"inflight": st.Inflight, "dead": st.Dead, "completed": st.Completed,
		},
		SnapshotAt: time.Now(),
		Meta:       map[string]TaskMeta{},
	}

	addMeta := func(id string) {
		m := db.q.Meta(id)
		snap.Meta[id] = TaskMeta{
			Attempts: m.Attempts, Cancels: m.Cancels,
			Reason: m.Reason, LeasedAt: m.LeasedAt, Caller: m.Caller,
		}
	}

	for _, t := range db.q.Ready() {
		snap.Ready = append(snap.Ready, fromInternal(t))
		addMeta(t.ID)
	}
	snap.BlockedBy = db.q.Blocked()
	for id, t := range db.q.BlockedAll() {
		snap.Blocked = append(snap.Blocked, fromInternal(t))
		addMeta(id)
	}
	for id, t := range db.q.Inflight() {
		snap.Inflight = append(snap.Inflight, fromInternal(t))
		addMeta(id)
	}
	for id, t := range db.q.Dead() {
		snap.Dead = append(snap.Dead, fromInternal(t))
		addMeta(id)
	}
	for id, t := range db.q.CompletedTasks() {
		snap.Completed = append(snap.Completed, fromInternal(t))
		addMeta(id)
	}
	for taskID, deps := range snap.BlockedBy {
		for _, depID := range deps {
			snap.Deps = append(snap.Deps, DepEdge{From: taskID, To: depID})
		}
	}
	snap.DBMeta, _ = db.q.GetDBMeta(context.Background())

	return snap
}

// Reload re-reads from the source and reconciles state.
func (db *DB) Reload() error {
	return db.q.Reload(context.Background())
}

// Save persists the current queue state to the state file.
func (db *DB) Save() error { return db.q.Save() }

// Close releases the database connection.
func (db *DB) Close() error { return db.q.Close() }

// ConfigDB describes a database entry in the schedg config file.
type ConfigDB = config.DB

// RegisterConfig adds or updates a DB entry in the schedg config.
func RegisterConfig(db ConfigDB) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	cfg.Put(db)
	return cfg.Save()
}

// UnregisterConfig removes a DB entry from the schedg config by name.
func UnregisterConfig(name string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	newDBs := make([]config.DB, 0, len(cfg.DBs))
	for _, d := range cfg.DBs {
		if d.Name != name {
			newDBs = append(newDBs, d)
		}
	}
	cfg.DBs = newDBs
	return cfg.Save()
}

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
		KV:          t.KV,
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
