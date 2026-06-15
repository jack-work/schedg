# schedg

A dependency-aware priority queue layered, **read-only**, over a SQL task
catalog. Go port of the `schedj` scheduler (heap + Kahn unblocking + cycle
refusal), backed by Dolt. You keep a multipurpose, queryable database of tasks;
schedg reads it like a heap.

## Model

- The database is the **task catalog** — schedg never writes task data. The only
  DB writes are one-time setup (`init`: schema, saved queries, metadata) and
  explicit `sql` passthrough.
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
completed`. `next` **leases** the top ready task (ready → in-flight). From
in-flight:

- **`complete`** — success, terminal.
- **`cancel [reason]`** — couldn't finish now; returns to ready and bumps the
  task's cancel count. Once the count reaches `--max-cancels` it is **buried**
  to the dead-letter set instead of requeued (poison-task protection).
- **`fail [reason]`** — bury immediately, terminal. Dependents stay blocked and
  surface in `status`.

A buried task is revived with **`requeue`** (resets its cancel count). With
`--lease-ttl` set, an in-flight task whose lease elapses (e.g. the worker
crashed) is auto-cancelled back to ready on the next `schedg` invocation — the
standard visibility-timeout pattern. Per-task `attempts`/`cancels` counters
persist in the state file and survive source drift.

## Layout

```
internal/heap      generic binary max-heap (invariants in the doc comment)
internal/priority  Task + Comparator interface + registry + default
internal/sched     dependency scheduler: submit/next/peek/complete/fail
internal/schema    table description; Default() = figtodo `todo` template
internal/source    Source driver interface (+ dolt impl); Register for new DBs
internal/config    the single JSON registry of databases + state paths
internal/queue     binds source+comparator+sched, persists state, detects drift
cmd/schedg         CLI
```

The default schema mirrors the figaro-qua figtodo `todo` table (see the
figaro-dev skill): `id, title, done, parent_id, created_at, description`, plus a
`priority` column, a `todo_dep` lookup, and a `schedg_meta` table holding the
repo back-reference. Loads tolerate a missing `priority`/deps table, so schedg
runs against an unmodified figtodo (everything ranks `p0`, FIFO by `created_at`).

## Usage

```bash
schedg init demo --data-dir ~/dev/figtodo/data --repo ~/dev/figaro-qua \
                 --max-cancels 3 --lease-ttl 15m
schedg dbs                       # registered queues
schedg status demo               # counts + ready / in-flight / blocked / dead
schedg peek demo                 # highest-priority ready task
schedg next demo                 # lease it (marks in-flight)
schedg complete demo 25          # success -> unblocks dependents
schedg cancel demo 25 "flaky"    # release back to ready (auto-buries at max-cancels)
schedg fail demo 25 "broken"     # bury to dead-letter (terminal)
schedg requeue demo 25           # kick a buried task back to ready
schedg sync demo                 # reload from source, report drift
schedg sql demo -- sql -x schedg-ready   # passthrough to dolt (saved queries)
schedg comparators               # list ranking modules
```

State and registry default to `$XDG_CONFIG_HOME/schedg/` (override with
`SCHEDG_CONFIG_DIR`).

## Extending

- **New SQL backend:** implement `source.Source` and `source.Register("name", …)`;
  select with `--driver name`. The dolt driver shells out; a `database/sql`
  driver against `dolt sql-server` or any MySQL-compatible DB drops in the same
  way.
- **New ranking:** implement `priority.Comparator` and `priority.Register`;
  select per-queue with `--comparator name`.
