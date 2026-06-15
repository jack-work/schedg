// Package source abstracts the task store behind the queue. The queue treats a
// Source as read-only for task data; only Init* (one-time setup) and Passthrough
// may mutate the backing DB. Drivers register themselves so new SQL backends
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
	Path   string // data-dir / DSN, driver-specific
	Schema schema.Schema
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
