---
name: schedg
description: Setting up and operating schedg - a dependency-aware priority queue backed by SQL (Dolt or SQLite). Covers building/installing, the Go library, registering a database, task CRUD, the task lifecycle (lease/complete/cancel/fail/requeue), lease expiry and dead-letter, drift detection, and extending with new ranking modules or SQL backends.
---

# schedg

`schedg` (`~/dev/schedg`, Go) is a priority queue with dependency resolution
backed by a SQL task catalog. It supports **Dolt** (the original git-for-data
backend) and **SQLite** (lightweight, single-file, no server). You create and
manage tasks via the **CLI** or a **Go library**; the queue reads the catalog,
ranks tasks, and resolves dependencies. Runtime state (ready heap, blocked set,
in-flight, completed) lives in a serialized state file with a checksum for drift
detection. **Keep this skill current when schedg changes.**

## Mental model

- **Catalog** = the SQL database. Rows are tasks.
- **Queue state** = a serialized heap + sidecar maps in
  `$SCHEDG_CONFIG_DIR/state/<db>.json`, stamped with a **checksum** of loaded
  rows.
- **Drift**: when the catalog diverges from the saved checksum, state is
  reconciled (completed/in-flight/dead preserved by id, counters kept) instead
  of trusted.
- **CRUD writes** to the DB go through the `Source` driver and automatically
  resync the queue (save state, mutate DB, reload, save again).
- Only the **ready** frontier lives in the max-heap; blocked tasks enter it as
  their last dependency completes (Kahn). Cycles are refused at submit time.

## Setup

```bash
cd ~/dev/schedg
go build -o ~/bin/schedg ./cmd/schedg
```

### Init from CLI

**SQLite** (single-file, no server):

```bash
schedg init mytasks --driver sqlite --data-dir ~/tasks.db
```

**Dolt** (versioned, MySQL-compatible):

```bash
schedg init figtodo --driver dolt --data-dir ~/dev/figtodo/data --repo ~/dev/figaro-qua \
                     --max-cancels 3 --lease-ttl 15m
```

Common flags for `init`:

- `--driver NAME` - source driver: `sqlite` or `dolt` (default `dolt`)
- `--data-dir PATH` - DB file (sqlite) or data-dir (dolt)
- `--repo PATH` - repo back-reference (optional)
- `--max-cancels N` - bury a task after N cancels (0 = unlimited)
- `--lease-ttl D` - auto-cancel idle in-flight tasks (e.g. `15m`; empty = off)
- `--comparator NAME` - ranking module (default `priority-submitted`)

Config + state default to `$XDG_CONFIG_HOME/schedg/`; override with
`SCHEDG_CONFIG_DIR`.

### Init from Go library

```go
import "github.com/jack-work/schedg"

ctx := context.Background()

// Init creates the schema (idempotent) and returns an open DB.
db, err := schedg.Init(ctx, schedg.Options{
    Driver: "sqlite",            // or "dolt"
    Path:   "tasks.db",          // file path (sqlite) or data-dir (dolt)
    // StatePath: "tasks.state.json", // optional; defaults to <Path>.state.json
    // MaxCancels: 3,
    // LeaseTTL: 15 * time.Minute,
})
if err != nil { log.Fatal(err) }
defer db.Close()
```

To open an existing DB without running schema init:

```go
db, err := schedg.Open(schedg.Options{
    Path: "tasks.db",
})
```

## Task CRUD

CRUD operations write to the SQL database and automatically resync the queue.

### CLI

```bash
schedg add <db> <title> [--priority N] [--description TEXT]
schedg update <db> <id> <title> [--priority N] [--description TEXT]
schedg rm <db> <id>
schedg mark-done <db> <id>
schedg add-dep <db> <task-id> <dep-id>
schedg rm-dep <db> <task-id> <dep-id>
```

Examples:

```bash
schedg add mytasks "fix the build" --priority 5
schedg add mytasks "write tests" --priority 3
schedg add mytasks "deploy" --priority 8
schedg add-dep mytasks 3 1          # deploy depends on fix the build
schedg mark-done mytasks 2          # sets done=1 in the DB
schedg rm mytasks 3                 # deletes the row
```

### Go library

```go
id, err := db.Add(ctx, "fix the build", schedg.TaskOpts{
    Priority:    5,
    Description: "## Repro\n\n1. Run `go test`\n2. See panic in handler.go:42",
})
err = db.Update(ctx, id, "fix the build (urgent)", schedg.TaskOpts{Priority: 10})
err = db.AddDep(ctx, deployID, id)
err = db.Done(ctx, id)              // sets done=1 in DB
err = db.Remove(ctx, id)            // deletes the row
err = db.RemoveDep(ctx, deployID, id)
```

## Queue operations

Queue operations affect the in-memory scheduler state (persisted to the state
file), not the database rows.

### CLI

```bash
schedg dbs                          # registered queues
schedg status <db>                  # ready/blocked/in-flight/dead/completed
schedg peek <db>                    # show top ready task without leasing
schedg next <db>                    # lease top ready task (-> in-flight)
schedg complete <db> <id>           # success, terminal; unblocks dependents
schedg cancel <db> <id> [reason]    # release to ready; auto-buries at max-cancels
schedg fail <db> <id> [reason]      # bury to dead-letter, terminal
schedg requeue <db> <id>            # kick a buried task back to ready
schedg sync <db>                    # reload from source, report drift
schedg sql <db> -- <args>           # passthrough (dolt: dolt subcommand; sqlite: raw SQL)
schedg comparators                  # list registered ranking modules
```

Aliases: `ls`=`status`, `done`=`complete`, `release`=`cancel`, `bury`=`fail`,
`kick`=`requeue`, `remove`=`rm`.

### Go library

```go
task, ok := db.Next()               // lease top ready task
task, ok = db.Peek()                // look without leasing
err = db.Complete(task.ID)          // success, unblocks dependents
buried, err := db.Cancel(id, "retry later")
err = db.Fail(id, "broken")        // bury to dead-letter
err = db.Requeue(id)               // kick buried task back to ready
status := db.Status()              // Status{Ready, Blocked, Inflight, Dead, Completed}
ready := db.Ready()                // []Task in priority order
err = db.Save()                    // persist queue state
err = db.Close()                   // release DB connection
```

### Lifecycle

`next` leases the top ready task. A leased task ends one of four ways:
`complete` (done), `cancel` (retry later - back to ready, cancel count++,
buried once it hits `--max-cancels`), `fail` (bury now), or - if `--lease-ttl`
is set - **lease expiry** (crashed worker: auto-cancelled on next invocation).

### mark-done vs complete

- **`mark-done`** / `db.Done(ctx, id)` sets `done=1` in the SQL database. The
  task disappears from the catalog on the next load.
- **`complete`** / `db.Complete(id)` moves a leased (in-flight) task to the
  completed set in queue state, unblocking dependents. It does not touch the DB.

Use `mark-done` for simple todo workflows. Use `complete` for the
lease-based work queue pattern. They can be combined.

### Rendering the catalog (dolt)

Dolt databases get saved queries registered by `init`:

```bash
schedg sql <db> -- sql -x schedg-open    # open tasks by priority then age
schedg sql <db> -- sql -x schedg-ready   # open tasks with no unmet prerequisite
schedg sql <db> -- sql -x schedg-deps    # dependency edges
schedg sql <db> -- sql -x schedg-meta    # metadata
```

### Rendering the catalog (sqlite)

```bash
schedg sql <db> -- "SELECT id, priority, title FROM todo WHERE done=0 ORDER BY priority DESC"
```

## Schema

The default schema (for both drivers) mirrors the figtodo `todo` table shape:

| Column | SQLite type | Purpose |
|---|---|---|
| `id` | INTEGER PRIMARY KEY AUTOINCREMENT | task id |
| `title` | TEXT NOT NULL | short human label |
| `done` | INTEGER NOT NULL DEFAULT 0 | 0=open, 1=done |
| `parent_id` | INTEGER (FK) | hierarchy (not blocking) |
| `created_at` | TEXT DEFAULT datetime('now') | submission timestamp |
| `description` | TEXT | longer markdown body (repro steps, acceptance criteria, notes) |
| `priority` | INTEGER NOT NULL DEFAULT 0 | ranking (higher = more urgent) |

Plus `todo_dep (task_id, depends_on_id)` for blocking dependencies and
`schedg_meta (k, v)` for the repo back-reference.

The Dolt DDL uses MySQL syntax (`AUTO_INCREMENT`, `BOOLEAN`, `TIMESTAMP`,
`VARCHAR`); the SQLite DDL uses SQLite-native types. Both create the same
logical schema. `schema.Default(driver)` returns the right DDL for the driver.

## Web UI

schedg ships a web frontend for browsing queues, tasks, dependencies, and server
logs.

### Running

```bash
cd ~/dev/schedg
go run ./cmd/schedg-web --addr :9746 --web web/dist
```

Then open `http://localhost:9746`.

### Building the frontend

```bash
cd ~/dev/schedg/web
npm install
npm run build      # outputs to web/dist/
npm run dev        # dev server with HMR (proxies /api to :9746)
```

### Features

- **Queue list**: shows all registered schedgs with driver and path
- **Queue view**: tabbed lists for ready/blocked/in-flight/dead/completed,
  fuzzy search filter, keyboard navigation (j/k/Enter/1-5)
- **Task detail**: full metadata, description, blocked-by info, copy buttons
- **Dependency graph**: SVG DAG of blocked/ready tasks, color-coded by state
- **Live updates**: SSE push from server every 2s
- **Server logs**: streaming log viewer (Ctrl+L)
- **Keyboard-driven**: `/` to search, `g` for graph, `?` for help, Backspace
  to go back, `c` to copy

### API

| Endpoint | Method | Description |
|---|---|---|
| `/api/queues` | GET | List all registered queues |
| `/api/queues/{name}/status` | GET | Full snapshot (ready/blocked/inflight/dead/completed/deps) |
| `/api/queues/{name}/events` | GET (SSE) | Live streaming snapshot updates |
| `/api/logs` | GET (SSE) | Streaming server log entries |

### Architecture

```
cmd/schedg-web/main.go        entry point (flag parsing, server start)
internal/webserver/server.go   HTTP handlers, SSE, snapshot logic
internal/webserver/logger/     ring-buffer log with subscriber support
web/                           React + Vite frontend
```

The server uses the schedg Go library internally (`internal/queue`, `internal/config`)
to open queues read-only and produce snapshots. It does not modify queue state.

## Extending

- **New ranking:** implement `priority.Comparator` (`Name`, `Compare(a,b) int`,
  `>0` ranks higher), call `priority.Register` in an `init()`, select with
  `--comparator <name>`.
- **New SQL backend:** implement `source.Source` and `source.Register("name", ...)`;
  select with `--driver name`. For `database/sql`-compatible backends, implement
  the small `sqlDialect` interface (`hasTable`, `hasColumn`, `upsertSQL`) and
  reuse `sqlSource` - this is how the SQLite driver works. The dolt driver
  shells out to the `dolt` CLI instead.
- **Different catalog shape:** pass a custom `schema.Schema` (table, PK,
  priority/submitted/label columns, deps table, DDL).

## Layout

```
schedg.go          exported Go library (package schedg)
schedg_test.go     library tests
cmd/schedg         CLI
internal/heap      generic binary max-heap
internal/priority  Task + Comparator interface + registry + default
internal/sched     scheduler: lease/complete/cancel/fail/requeue, deps, lease TTL
internal/schema    table description; Default(driver) with Dolt and SQLite DDL
internal/source    Source driver interface + implementations:
                     dolt.go    - shells out to dolt CLI
                     sqldb.go   - generic database/sql driver (shared by SQL backends)
                     sqlite.go  - SQLite factory + dialect (uses modernc.org/sqlite)
internal/config    JSON registry of databases + state paths
internal/queue     binds source+comparator+sched, persists state, detects drift, CRUD
```

Tests cover the load-bearing logic: `internal/heap` (heap property vs sort),
`internal/sched` (priority order, dependency unblock, cycle refusal, cancel/
bury/requeue, lease expiry, snapshot round-trip), and the root `schedg_test.go`
(library round-trip, dependencies, remove). `go test ./...` before a commit.
