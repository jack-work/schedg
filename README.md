# schedg

A dependency-aware priority queue layered over a SQL task catalog. Go port of
the `schedj` scheduler (heap + Kahn unblocking + cycle refusal), backed by
**SQLite** or **Dolt**. You keep a multipurpose, queryable database of tasks;
schedg reads it like a heap. Tasks are created via the CLI or a Go library.

## Model

- The database is the **task catalog**. CRUD operations (`add`, `rm`,
  `mark-done`, `update`) write to it through the source driver.
- Queue **runtime state** (ready heap, blocked set, in-flight, completed) lives
  in a serialized state file under the config dir, with a **source checksum +
  format version**. When the catalog drifts from the saved checksum, state is
  reconciled (completed/in-flight preserved by id) instead of trusted.
- Tasks are ranked by a pluggable **Comparator**. The default
  (`priority-submitted`) ranks by the `priority` column descending, then
  submission time ascending. Register another in Go via `priority.Register`.
- Dependencies live in a lookup table (`todo_dep`); a task is ready only once
  all its prerequisites complete. Cycles are refused at submit time.

## Task lifecycle

A task moves through exactly one of `blocked / ready / in-flight / dead /
completed`. `next` **leases** the top ready task (ready -> in-flight). From
in-flight:

- **`complete`** - success, terminal.
- **`cancel [reason]`** - couldn't finish now; returns to ready and bumps the
  task's cancel count. Once the count reaches `--max-cancels` it is **buried**
  to the dead-letter set instead of requeued (poison-task protection).
- **`fail [reason]`** - bury immediately, terminal. Dependents stay blocked and
  surface in `status`.

A buried task is revived with **`requeue`** (resets its cancel count). With
`--lease-ttl` set, an in-flight task whose lease elapses (e.g. the worker
crashed) is auto-cancelled back to ready on the next `schedg` invocation - the
standard visibility-timeout pattern. Per-task `attempts`/`cancels` counters
persist in the state file and survive source drift.

## Layout

```
schedg.go          exported Go library (import "github.com/jack-work/schedg")
cmd/schedg         CLI
internal/heap      generic binary max-heap (invariants in the doc comment)
internal/priority  Task + Comparator interface + registry + default
internal/sched     dependency scheduler: submit/next/peek/complete/fail
internal/schema    table description; Default(driver) with Dolt + SQLite DDL
internal/source    Source driver interface + implementations:
                     dolt.go    shells out to dolt CLI
                     sqldb.go   generic database/sql driver (shared by SQL backends)
                     sqlite.go  SQLite factory + dialect (modernc.org/sqlite, pure Go)
internal/config    the single JSON registry of databases + state paths
internal/queue     binds source+comparator+sched, persists state, detects drift, CRUD
```

## Usage

### CLI

```bash
# Initialize a SQLite-backed queue
schedg init mytasks --driver sqlite --data-dir ~/tasks.db

# Initialize a Dolt-backed queue
schedg init figtodo --driver dolt --data-dir ~/dev/figtodo/data --repo ~/dev/figaro-qua

# Task CRUD (writes to the database)
schedg add mytasks "fix the build" --priority 5 \
  --description "## Repro\n\n1. Run go test\n2. See panic"
schedg add mytasks "deploy" --priority 8
schedg add-dep mytasks 2 1           # deploy depends on fix-the-build
schedg update mytasks 1 "fix build (urgent)" --priority 10
schedg mark-done mytasks 1           # sets done=1 in DB
schedg rm mytasks 2                  # deletes the row

# Queue operations (state file only)
schedg dbs                           # registered queues
schedg status mytasks                # counts + ready / in-flight / blocked / dead
schedg peek mytasks                  # highest-priority ready task
schedg next mytasks                  # lease it (marks in-flight)
schedg complete mytasks 1            # success -> unblocks dependents
schedg cancel mytasks 1 "flaky"      # release back to ready
schedg fail mytasks 1 "broken"       # bury to dead-letter
schedg requeue mytasks 1             # kick a buried task back to ready
schedg sync mytasks                  # reload from source, report drift
schedg sql mytasks -- "SELECT * FROM todo"   # passthrough SQL (sqlite)
schedg sql figtodo -- sql -x schedg-ready    # passthrough to dolt
schedg comparators                   # list ranking modules
```

### Go library

```go
import "github.com/jack-work/schedg"

ctx := context.Background()
db, _ := schedg.Init(ctx, schedg.Options{
    Driver: "sqlite",
    Path:   "tasks.db",
})
defer db.Close()

// CRUD
id, _ := db.Add(ctx, "fix the build", schedg.TaskOpts{
    Priority:    5,
    Description: "markdown body with repro steps, etc.",
})
db.AddDep(ctx, deployID, id)
db.Done(ctx, id)
db.Remove(ctx, id)

// Queue operations
task, ok := db.Next()
db.Complete(task.ID)
db.Save()
```

State and registry default to `$XDG_CONFIG_HOME/schedg/` (override with
`SCHEDG_CONFIG_DIR`). The Go library defaults state to `<Path>.state.json`
instead of using the config dir.

## Extending

- **New SQL backend:** implement the small `sqlDialect` interface (`hasTable`,
  `hasColumn`, `upsertSQL`) and pass it to `newSQLSource` with a `*sql.DB`. See
  `sqlite.go` for the pattern. Or implement `source.Source` directly and
  `source.Register("name", ...)` like the dolt driver.
- **New ranking:** implement `priority.Comparator` and `priority.Register`;
  select per-queue with `--comparator name`.
