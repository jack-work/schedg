---
name: schedg
description: Setting up and operating schedg — a dependency-aware priority queue that reads, read-only, from a Dolt (or other SQL) task catalog. Covers building/installing, registering a database, the task lifecycle (lease/complete/cancel/fail/requeue), lease expiry and dead-letter, drift detection, and extending it with new ranking modules or SQL backends.
---

# schedg

`schedg` (`~/dev/schedg`, Go, stdlib-only) is a priority queue with dependency
resolution layered **read-only** over a SQL task catalog. You keep a
multipurpose, queryable database of tasks (default: the figaro-qua figtodo
`todo` template — see the **figaro-dev** skill); schedg reads it like a heap and
tracks queue runtime state in a serialized file beside its config. It never
writes task data — only `init` (one-time schema/saved-queries/meta) and explicit
`sql` passthrough touch the DB. **Keep this skill current when schedg changes.**

## Mental model

- **Catalog** = the DB. Rows are tasks. schedg loads open tasks (`done=0`),
  ranks them, resolves dependencies. Humans/figtodo own the catalog.
- **Queue state** = a serialized heap + sidecar maps (blocked, in-flight, dead,
  completed, per-task counters) in `$SCHEDG_CONFIG_DIR/state/<db>.json`, stamped
  with a format **version** and a **checksum** of the loaded rows.
- **Drift**: when the catalog no longer matches the saved checksum, state is
  reconciled (completed/in-flight/dead preserved by id, counters kept) instead
  of trusted. The reconciled baseline is persisted so the notice fires once.
- Only the **ready** frontier lives in the max-heap; blocked tasks enter it as
  their last dependency completes (Kahn). Cycles are refused at submit time.

## Setup

```bash
cd ~/dev/schedg
go build -o ~/bin/schedg ./cmd/schedg   # or: go install ./cmd/schedg
```

Register a database (idempotent; creates the `priority` column, `todo_dep`
lookup, `schedg_meta` repo back-reference, and the `schedg-*` saved queries):

```bash
schedg init figtodo --data-dir ~/dev/figtodo/data --repo ~/dev/figaro-qua \
                     --max-cancels 3 --lease-ttl 15m
```

- `--max-cancels N` — bury a task after N cancels (0 = unlimited).
- `--lease-ttl D` — auto-cancel in-flight tasks idle longer than `D`
  (e.g. `15m`); empty = off.
- `--comparator NAME` — ranking module (default `priority-submitted`).
- `--driver NAME` — source driver (default `dolt`).

schedg runs against an **unmodified** figtodo too (no `priority` column or
`todo_dep` table): everything ranks `p0`, FIFO by `created_at`, no deps. Skip
`init` and register by hand in `$SCHEDG_CONFIG_DIR/config.json` if you want
strictly zero DB writes.

Config + state default to `$XDG_CONFIG_HOME/schedg/`; override with
`SCHEDG_CONFIG_DIR` (used for isolated testing).

## Operating

```bash
schedg dbs                        # registered queues
schedg status <db>                # ready / blocked / in-flight / dead / completed
schedg peek <db>                  # show top ready task, don't lease
schedg next <db>                  # lease top ready task (-> in-flight)
schedg complete <db> <id>         # success, terminal; unblocks dependents
schedg cancel <db> <id> [reason]  # release to ready; auto-buries at max-cancels
schedg fail <db> <id> [reason]    # bury to dead-letter, terminal
schedg requeue <db> <id>          # kick a buried task back to ready (resets cancels)
schedg sync <db>                  # reload from source, report drift
schedg sql <db> -- <args>         # passthrough to dolt (no args = sql shell)
schedg comparators                # list registered ranking modules
```

Aliases: `ls`=`status`, `done`=`complete`, `release`=`cancel`, `bury`=`fail`,
`kick`=`requeue`.

### Lifecycle

`next` leases the top ready task. A leased task ends one of four ways:
`complete` (done), `cancel` (retry later — back to ready, cancel count++,
buried once it hits `--max-cancels`), `fail` (bury now), or — if `--lease-ttl`
is set — **lease expiry** (a crashed/abandoned worker: the lease elapses and
the task is auto-cancelled back to ready on the next `schedg` invocation). A
buried task waits in the dead-letter set until `requeue`. Per-task
`attempts`/`cancels` counters show in `status` and survive drift.

### Rendering the catalog

The heap operates on ids+priority; rich views come from the saved queries:

```bash
schedg sql <db> -- sql -x schedg-open    # open tasks by priority then age
schedg sql <db> -- sql -x schedg-ready   # open tasks with no unmet prerequisite
schedg sql <db> -- sql -x schedg-deps    # dependency edges
schedg sql <db> -- sql -x schedg-meta    # repo back-reference + metadata
```

Dependencies are rows in `<table>_dep (task_id, depends_on_id)` — distinct from
figtodo's `parent_id` (which is hierarchy, not blocking). Add an edge with
`schedg sql <db> -- sql -q "INSERT INTO todo_dep VALUES (4,25);"` (4 waits on 25).

## Extending

- **New ranking:** implement `priority.Comparator` (`Name`, `Compare(a,b) int`,
  `>0` ranks higher), call `priority.Register` in an `init()`, select with
  `--comparator <name>`. The default considers the `priority` then `submitted`
  columns.
- **New SQL backend:** implement `source.Source` and `source.Register("name", …)`;
  select with `--driver name`. The dolt driver shells out to `dolt`; a
  `database/sql` driver against `dolt sql-server` or any MySQL-compatible DB
  drops in the same way. `Load` is the only hot path; `Init*`/`Passthrough` are
  setup/escape hatches.
- **Different catalog shape:** pass a custom `schema.Schema` (table, PK,
  priority/submitted/label columns, deps table, DDL, saved queries). `Default()`
  is the figtodo template.

## Layout

```
internal/heap      generic binary max-heap (invariants in the doc comment)
internal/priority  Task + Comparator interface + registry + default
internal/sched     scheduler: lease/complete/cancel/fail/requeue, deps, lease TTL
internal/schema    table description; Default() = figtodo `todo` template
internal/source    Source driver interface (+ dolt impl); Register for new DBs
internal/config    the single JSON registry of databases + state paths
internal/queue     binds source+comparator+sched, persists state, detects drift
cmd/schedg         CLI
```

Tests cover the load-bearing logic: `internal/heap` (heap property vs sort) and
`internal/sched` (priority order, dependency unblock, cycle refusal, cancel/
bury/requeue, lease expiry, snapshot round-trip). `go test ./...` before a
commit; a green slice gets one.
