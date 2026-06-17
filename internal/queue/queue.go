// Package queue binds a Source, a Comparator, and the sched.Scheduler together,
// persisting queue runtime state to a serialized file. The Source is read-only
// for task data by default; CRUD methods write to the source and resync the
// queue. A checksum over the loaded rows detects drift so stale state is rebuilt
// rather than trusted.
package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jack-work/schedg/internal/config"
	"github.com/jack-work/schedg/internal/priority"
	"github.com/jack-work/schedg/internal/sched"
	"github.com/jack-work/schedg/internal/schema"
	"github.com/jack-work/schedg/internal/source"
)

const stateVersion = 1

type stateFile struct {
	Version    int            `json:"version"`
	Checksum   string         `json:"checksum"`
	Comparator string         `json:"comparator"`
	Snapshot   sched.Snapshot `json:"snapshot"`
	SavedAt    time.Time      `json:"saved_at"`
}

type Queue struct {
	name       string
	src        source.Source
	cmp        priority.Comparator
	schema     schema.Schema
	statePath  string
	maxCancels int
	leaseTTL   time.Duration

	sched      *sched.Scheduler
	checksum   string
	drift      bool
	expired    []string
	submitErrs []error
}

func Open(db config.DB) (*Queue, error) {
	cmp := priority.Default()
	if db.Comparator != "" {
		c, ok := priority.Get(db.Comparator)
		if !ok {
			return nil, fmt.Errorf("unknown comparator %q (have %v)", db.Comparator, priority.Names())
		}
		cmp = c
	}
	var ttl time.Duration
	if db.LeaseTTL != "" {
		d, err := time.ParseDuration(db.LeaseTTL)
		if err != nil {
			return nil, fmt.Errorf("lease_ttl %q: %w", db.LeaseTTL, err)
		}
		ttl = d
	}
	sc := schema.Default(db.Driver)
	src, err := source.Open(db.Driver, source.Config{Path: db.Path, Schema: sc})
	if err != nil {
		return nil, err
	}

	statePath := db.StatePath
	if statePath == "" {
		statePath = config.StatePath(db.Name)
	}

	q := &Queue{
		name: db.Name, src: src, cmp: cmp, schema: sc,
		statePath: statePath, maxCancels: db.MaxCancels, leaseTTL: ttl,
	}
	if err := q.load(context.Background()); err != nil {
		return nil, err
	}
	if q.drift || len(q.expired) > 0 {
		if err := q.Save(); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (q *Queue) load(ctx context.Context) error {
	rows, err := q.src.Load(ctx)
	if err != nil {
		return err
	}
	checksum := rowsChecksum(rows)
	st := readState(q.statePath)

	if st != nil && st.Version == stateVersion && st.Checksum == checksum && st.Comparator == q.cmp.Name() {
		q.sched = sched.Restore(q.cmp, st.Snapshot, q.maxCancels)
		// Refresh Fields from the live source rows -- the snapshot may
		// predate columns added after it was written (e.g. description).
		fieldMap := make(map[string]map[string]string, len(rows))
		for _, r := range rows {
			if len(r.Fields) > 0 {
				fieldMap[r.ID] = r.Fields
			}
		}
		q.sched.RefreshFields(fieldMap)
		q.drift = false
	} else {
		var prev *sched.Snapshot
		if st != nil {
			prev = &st.Snapshot
			q.drift = true
		}
		q.submitErrs = q.rebuild(rows, prev)
	}
	q.checksum = checksum
	q.expired = q.sched.ExpireLeases(q.leaseTTL)
	return nil
}

func (q *Queue) rebuild(rows []source.Row, prev *sched.Snapshot) []error {
	sc := sched.New(q.cmp, q.maxCancels)
	if prev != nil {
		sc.SeedCompleted(prev.Completed)
		sc.SeedInflight(prev.Inflight)
		sc.SeedDead(prev.Dead)
		sc.SeedMeta(prev.Meta)
	}
	var errs []error
	for _, r := range rows {
		if sc.Completed(r.ID) || sc.Buried(r.ID) {
			continue
		}
		if prev != nil {
			if _, ok := prev.Inflight[r.ID]; ok {
				continue
			}
		}
		t := priority.Task{ID: r.ID, Priority: r.Priority, Submitted: r.Submitted, Fields: r.Fields}
		if err := sc.Submit(t, r.Deps); err != nil {
			errs = append(errs, err)
		}
	}
	q.sched = sc
	return errs
}

// Reload re-reads from the source and reconciles state. Used by the web server
// to refresh a cached queue without reopening the source connection.
func (q *Queue) Reload(ctx context.Context) error {
	return q.load(ctx)
}

// Drifted reports whether prior state was discarded because the source changed.
func (q *Queue) Drifted() bool { return q.drift }

// Expired lists in-flight ids whose lease elapsed on this open (lease TTL only).
func (q *Queue) Expired() []string { return q.expired }

// SubmitErrors are rejected rows from the last (re)build, e.g. dependency cycles.
func (q *Queue) SubmitErrors() []error { return q.submitErrs }

func (q *Queue) Next() (priority.Task, bool)            { return q.sched.Next() }
func (q *Queue) NextFor(caller string) (priority.Task, bool) { return q.sched.NextFor(caller) }
func (q *Queue) Peek() (priority.Task, bool)            { return q.sched.Peek() }
func (q *Queue) Complete(id string) error               { return q.sched.Complete(id) }
func (q *Queue) Cancel(id, reason string) (bool, error) { return q.sched.Cancel(id, reason) }
func (q *Queue) Fail(id, reason string) error           { return q.sched.Fail(id, reason) }
func (q *Queue) Requeue(id string) error                { return q.sched.Requeue(id) }
func (q *Queue) Status() sched.Status                   { return q.sched.Status() }
func (q *Queue) Ready() []priority.Task                 { return q.sched.ReadyTasks() }
func (q *Queue) Blocked() map[string][]string           { return q.sched.BlockedTasks() }
func (q *Queue) Inflight() map[string]priority.Task     { return q.sched.InflightTasks() }
func (q *Queue) Dead() map[string]priority.Task         { return q.sched.DeadTasks() }
func (q *Queue) BlockedAll() map[string]priority.Task    { return q.sched.BlockedAllTasks() }
func (q *Queue) CompletedIDs() []string                    { return q.sched.CompletedIDs() }
func (q *Queue) CompletedTasks() map[string]priority.Task  { return q.sched.CompletedTasks() }
func (q *Queue) Meta(id string) sched.Meta               { return q.sched.Meta(id) }

func (q *Queue) Init(ctx context.Context, repo string) error {
	if err := q.src.InitSchema(ctx); err != nil {
		return err
	}
	if err := q.src.EnsureSavedQueries(ctx); err != nil {
		return err
	}
	if err := q.src.WriteMeta(ctx, "repo", repo); err != nil {
		return err
	}
	return q.src.WriteMeta(ctx, "queue", q.name)
}

func (q *Queue) Passthrough(ctx context.Context, args []string) error {
	return q.src.Passthrough(ctx, args)
}

// --- CRUD (write to source, then resync queue) ---

// writeAndSync persists current state, runs fn (a source mutation), reloads
// from the now-changed source, and saves the reconciled state.
func (q *Queue) writeAndSync(ctx context.Context, fn func() error) error {
	if err := q.Save(); err != nil {
		return err
	}
	if err := fn(); err != nil {
		return err
	}
	if err := q.load(ctx); err != nil {
		return err
	}
	return q.Save()
}

func (q *Queue) Add(ctx context.Context, title string, opts source.TaskOpts) (string, error) {
	var id string
	err := q.writeAndSync(ctx, func() error {
		var e error
		id, e = q.src.AddTask(ctx, title, opts)
		return e
	})
	return id, err
}

func (q *Queue) Update(ctx context.Context, id string, title string, opts source.TaskOpts) error {
	return q.writeAndSync(ctx, func() error {
		return q.src.UpdateTask(ctx, id, title, opts)
	})
}

func (q *Queue) Remove(ctx context.Context, id string) error {
	return q.writeAndSync(ctx, func() error {
		return q.src.RemoveTask(ctx, id)
	})
}

func (q *Queue) SetDone(ctx context.Context, id string) error {
	return q.writeAndSync(ctx, func() error {
		return q.src.SetDone(ctx, id, true)
	})
}

func (q *Queue) AddDep(ctx context.Context, taskID, depID string) error {
	return q.writeAndSync(ctx, func() error {
		return q.src.AddDep(ctx, taskID, depID)
	})
}

func (q *Queue) RemoveDep(ctx context.Context, taskID, depID string) error {
	return q.writeAndSync(ctx, func() error {
		return q.src.RemoveDep(ctx, taskID, depID)
	})
}

// --- persistence ---

func (q *Queue) Save() error {
	if err := os.MkdirAll(filepath.Dir(q.statePath), 0o755); err != nil {
		return err
	}
	st := stateFile{
		Version:    stateVersion,
		Checksum:   q.checksum,
		Comparator: q.cmp.Name(),
		Snapshot:   q.sched.Snapshot(),
		SavedAt:    time.Now(),
	}
	data, _ := json.MarshalIndent(st, "", "  ")
	return os.WriteFile(q.statePath, append(data, '\n'), 0o644)
}

func (q *Queue) Close() error { return q.src.Close() }

func readState(path string) *stateFile {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var st stateFile
	if json.Unmarshal(data, &st) != nil {
		return nil
	}
	return &st
}

func rowsChecksum(rows []source.Row) string {
	keys := make([]string, len(rows))
	for i, r := range rows {
		deps := append([]string(nil), r.Deps...)
		sort.Strings(deps)
		keys[i] = strings.Join([]string{
			r.ID,
			strconv.FormatInt(r.Priority, 10),
			strconv.FormatInt(r.Submitted.UnixNano(), 10),
			strings.Join(deps, ","),
		}, "|")
	}
	sort.Strings(keys)
	h := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(h[:])
}
