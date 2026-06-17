// Package source abstracts the task store behind the queue. The queue treats a
// Source as read-only for task data by default; CRUD methods allow writes when
// the driver supports them. Drivers register themselves so new SQL backends
// drop in without touching the queue.
package source

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jack-work/schedg/internal/schema"
)

type Row struct {
	ID        string
	Priority  int64
	Submitted time.Time
	Deps      []string
	Fields    map[string]string
}

type Config struct {
	Path   string // data-dir / DSN / file path, driver-specific
	Schema schema.Schema
}

// TaskOpts carries optional fields for task creation and update.
type TaskOpts struct {
	Description string
	Priority    int64
	ParentID    string
}

type Source interface {
	Name() string
	// Load reads open tasks (a read-only snapshot of the catalog).
	Load(ctx context.Context) ([]Row, error)
	// InitSchema creates tables/columns idempotently.
	InitSchema(ctx context.Context) error
	// EnsureSavedQueries registers render queries (no-op if unsupported).
	EnsureSavedQueries(ctx context.Context) error
	// WriteMeta records the repo back-reference into the DB.
	WriteMeta(ctx context.Context, key, val string) error
	// Passthrough runs an ad-hoc command against the backing tool.
	Passthrough(ctx context.Context, args []string) error
	Close() error

	// --- CRUD ---

	// AddTask inserts a new task and returns its id.
	AddTask(ctx context.Context, title string, opts TaskOpts) (string, error)
	// UpdateTask replaces mutable fields on an existing task.
	UpdateTask(ctx context.Context, id string, title string, opts TaskOpts) error
	// RemoveTask deletes a task by id.
	RemoveTask(ctx context.Context, id string) error
	// SetDone sets the done flag on a task (true = done, false = open).
	SetDone(ctx context.Context, id string, done bool) error
	// AddDep records that taskID depends on depID.
	AddDep(ctx context.Context, taskID, depID string) error
	// RemoveDep removes a dependency edge.
	RemoveDep(ctx context.Context, taskID, depID string) error
}

type Factory func(Config) (Source, error)

var drivers = map[string]Factory{}

func Register(name string, f Factory) { drivers[name] = f }

func Open(driver string, cfg Config) (Source, error) {
	f, ok := drivers[driver]
	if !ok {
		return nil, fmt.Errorf("unknown source driver %q (have %v)", driver, Drivers())
	}
	return f(cfg)
}

func Drivers() []string {
	out := make([]string, 0, len(drivers))
	for n := range drivers {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// --- shared helpers ---

var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999 -0700 MST",
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
