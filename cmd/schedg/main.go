// schedg - a priority queue with dependency resolution layered over a SQL task
// catalog (Dolt or SQLite). State persists in a serialized heap file with a
// source checksum for drift detection.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jack-work/schedg/internal/config"
	"github.com/jack-work/schedg/internal/priority"
	"github.com/jack-work/schedg/internal/queue"
	"github.com/jack-work/schedg/internal/schema"
	"github.com/jack-work/schedg/internal/source"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "init":
		err = cmdInit(args)
	case "dbs":
		err = cmdDBs(args)
	case "status", "ls":
		err = cmdStatus(args)
	case "next":
		err = cmdNext(args)
	case "peek":
		err = cmdPeek(args)
	case "complete", "done":
		err = cmdComplete(args)
	case "cancel", "release":
		err = cmdCancel(args)
	case "fail", "bury":
		err = cmdFail(args)
	case "requeue", "kick":
		err = cmdRequeue(args)
	case "sync":
		err = cmdSync(args)
	case "sql":
		err = cmdSQL(args)
	case "add":
		err = cmdAdd(args)
	case "rm", "remove":
		err = cmdRemove(args)
	case "update":
		err = cmdUpdate(args)
	case "mark-done":
		err = cmdMarkDone(args)
	case "add-dep":
		err = cmdAddDep(args)
	case "rm-dep":
		err = cmdRemoveDep(args)
	case "comparators":
		for _, n := range priority.Names() {
			fmt.Println(n)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `schedg - dependency-aware priority queue over a SQL task catalog

  schedg init <name> --data-dir <path> [--driver sqlite|dolt] [--repo <path>]
                     [--comparator <name>] [--max-cancels N] [--lease-ttl D]
  schedg dbs

  Task CRUD (writes to the database):
  schedg add <db> <title> [--priority N] [--description TEXT]
  schedg update <db> <id> <title> [--priority N] [--description TEXT]
  schedg rm <db> <id>
  schedg mark-done <db> <id>
  schedg add-dep <db> <task-id> <dep-id>
  schedg rm-dep <db> <task-id> <dep-id>

  Queue operations (state file only):
  schedg status <db>
  schedg peek <db>
  schedg next <db>
  schedg complete <db> <id>
  schedg cancel <db> <id> [reason]
  schedg fail <db> <id> [reason]
  schedg requeue <db> <id>
  schedg sync <db>
  schedg sql <db> -- <args>
  schedg comparators
`)
}

// --- init / dbs ---

func cmdInit(args []string) error {
	if len(args) < 1 || strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("usage: schedg init <name> --data-dir <path> [--driver sqlite|dolt]")
	}
	name := args[0]
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	driver := fs.String("driver", "dolt", "source driver (dolt, sqlite)")
	dataDir := fs.String("data-dir", "", "source data-dir / file path")
	repo := fs.String("repo", "", "repo location this queue serves")
	cmp := fs.String("comparator", "", "priority module (default: priority-submitted)")
	maxCancels := fs.Int("max-cancels", 0, "auto-bury a task after N cancels (0 = unlimited)")
	leaseTTL := fs.String("lease-ttl", "", "auto-cancel in-flight tasks idle longer than this (e.g. 10m); empty = off")
	fs.Parse(args[1:])
	if *dataDir == "" {
		return fmt.Errorf("usage: schedg init <name> --data-dir <path>")
	}
	if *leaseTTL != "" {
		if _, err := time.ParseDuration(*leaseTTL); err != nil {
			return fmt.Errorf("--lease-ttl %q: %w", *leaseTTL, err)
		}
	}
	if *cmp != "" {
		if _, ok := priority.Get(*cmp); !ok {
			return fmt.Errorf("unknown comparator %q (have %v)", *cmp, priority.Names())
		}
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	db := config.DB{Name: name, Driver: *driver, Path: *dataDir, Repo: *repo, Comparator: *cmp, MaxCancels: *maxCancels, LeaseTTL: *leaseTTL}
	cfg.Put(db)
	if err := cfg.Save(); err != nil {
		return err
	}
	q, err := queue.Open(db)
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Init(context.Background(), *repo); err != nil {
		return err
	}
	if err := q.Save(); err != nil {
		return err
	}
	fmt.Printf("registered %q (driver=%s, data-dir=%s, repo=%s)\n", name, *driver, *dataDir, *repo)
	st := q.Status()
	fmt.Printf("ready=%d blocked=%d in-flight=%d completed=%d\n", st.Ready, st.Blocked, st.Inflight, st.Completed)
	return nil
}

func cmdDBs(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if len(cfg.DBs) == 0 {
		fmt.Println("no databases registered (schedg init ...)")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tDRIVER\tDATA-DIR\tREPO\tCOMPARATOR")
	for _, db := range cfg.DBs {
		c := db.Comparator
		if c == "" {
			c = priority.Default().Name()
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", db.Name, db.Driver, db.Path, db.Repo, c)
	}
	return w.Flush()
}

// --- helpers ---

func open(name string) (*queue.Queue, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	db, ok := cfg.Find(name)
	if !ok {
		return nil, fmt.Errorf("no database %q registered (schedg dbs)", name)
	}
	q, err := queue.Open(*db)
	if err != nil {
		return nil, err
	}
	if q.Drifted() {
		fmt.Fprintln(os.Stderr, "note: source changed since last run - queue state reconciled")
	}
	if ex := q.Expired(); len(ex) > 0 {
		fmt.Fprintf(os.Stderr, "note: %d lease(s) expired, returned to queue: %s\n", len(ex), strings.Join(prefix(ex, "#"), " "))
	}
	for _, e := range q.SubmitErrors() {
		fmt.Fprintln(os.Stderr, "warning:", e)
	}
	return q, nil
}

func label(t priority.Task) string {
	if t.Fields != nil {
		if l := t.Fields["label"]; l != "" {
			return l
		}
	}
	return ""
}

func printTask(t priority.Task) {
	fmt.Printf("#%s\tp%d\t%s\n", t.ID, t.Priority, label(t))
}

// --- queue commands ---

func cmdStatus(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: schedg status <name>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	st := q.Status()
	fmt.Printf("ready=%d blocked=%d in-flight=%d dead=%d completed=%d\n",
		st.Ready, st.Blocked, st.Inflight, st.Dead, st.Completed)

	ready := q.Ready()
	if len(ready) > 0 {
		fmt.Println("\nready (highest priority first):")
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		for _, t := range ready {
			fmt.Fprintf(w, "  #%s\tp%d\t%s\n", t.ID, t.Priority, label(t))
		}
		w.Flush()
	}
	if inf := q.Inflight(); len(inf) > 0 {
		fmt.Println("\nin-flight (id, attempt):")
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		for _, id := range sortedIDs(inf) {
			fmt.Fprintf(w, "  #%s\ttry %d\t%s\n", id, q.Meta(id).Attempts, label(inf[id]))
		}
		w.Flush()
	}
	if bl := q.Blocked(); len(bl) > 0 {
		fmt.Println("\nblocked (task -> unmet deps):")
		ids := make([]string, 0, len(bl))
		for id := range bl {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Printf("  #%s -> %s\n", id, strings.Join(prefix(bl[id], "#"), " "))
		}
	}
	if dead := q.Dead(); len(dead) > 0 {
		fmt.Println("\ndead-letter (id, cancels, reason):")
		w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		for _, id := range sortedIDs(dead) {
			m := q.Meta(id)
			reason := m.Reason
			if reason == "" {
				reason = "(none)"
			}
			fmt.Fprintf(w, "  #%s\t%d cancels\t%s\n", id, m.Cancels, reason)
		}
		w.Flush()
	}
	return nil
}

func sortedIDs(m map[string]priority.Task) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func cmdNext(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: schedg next <name>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	t, ok := q.Next()
	if !ok {
		fmt.Println("no ready tasks")
		return nil
	}
	printTask(t)
	return q.Save()
}

func cmdPeek(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: schedg peek <name>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	t, ok := q.Peek()
	if !ok {
		fmt.Println("no ready tasks")
		return nil
	}
	printTask(t)
	return nil
}

func cmdComplete(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg complete <name> <id>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Complete(args[1]); err != nil {
		return err
	}
	fmt.Printf("completed #%s\n", args[1])
	return q.Save()
}

func cmdCancel(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg cancel <name> <id> [reason]")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	reason := strings.Join(args[2:], " ")
	buried, err := q.Cancel(args[1], reason)
	if err != nil {
		return err
	}
	if buried {
		fmt.Printf("cancelled #%s - reached max-cancels, buried to dead-letter\n", args[1])
	} else {
		fmt.Printf("cancelled #%s (returned to queue, %d cancels)\n", args[1], q.Meta(args[1]).Cancels)
	}
	return q.Save()
}

func cmdFail(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg fail <name> <id> [reason]")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Fail(args[1], strings.Join(args[2:], " ")); err != nil {
		return err
	}
	fmt.Printf("buried #%s to dead-letter\n", args[1])
	return q.Save()
}

func cmdRequeue(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg requeue <name> <id>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	if err := q.Requeue(args[1]); err != nil {
		return err
	}
	fmt.Printf("requeued #%s from dead-letter\n", args[1])
	return q.Save()
}

func cmdSync(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: schedg sync <name>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()
	st := q.Status()
	if q.Drifted() {
		fmt.Println("reconciled drift from source")
	} else {
		fmt.Println("in sync with source")
	}
	fmt.Printf("ready=%d blocked=%d in-flight=%d completed=%d\n", st.Ready, st.Blocked, st.Inflight, st.Completed)
	return q.Save()
}

func cmdSQL(args []string) error {
	var name string
	var rest []string
	for i, a := range args {
		if a == "--" {
			if i > 0 {
				name = args[0]
			}
			rest = args[i+1:]
			break
		}
	}
	if name == "" && len(args) > 0 {
		name = args[0]
		rest = args[1:]
	}
	if name == "" {
		return fmt.Errorf("usage: schedg sql <name> -- <args>")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	db, ok := cfg.Find(name)
	if !ok {
		return fmt.Errorf("no database %q registered", name)
	}
	sc := schema.Default(db.Driver)
	src, err := source.Open(db.Driver, source.Config{Path: db.Path, Schema: sc})
	if err != nil {
		return err
	}
	defer src.Close()
	if len(rest) == 0 {
		rest = []string{"sql"}
	}
	return src.Passthrough(context.Background(), rest)
}

// --- CRUD commands ---

func cmdAdd(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg add <db> <title> [--priority N] [--description TEXT]")
	}
	name, title := args[0], args[1]
	fs := flag.NewFlagSet("add", flag.ExitOnError)
	prio := fs.Int64("priority", 0, "task priority")
	desc := fs.String("description", "", "task description")
	fs.Parse(args[2:])

	q, err := open(name)
	if err != nil {
		return err
	}
	defer q.Close()

	id, err := q.Add(context.Background(), title, source.TaskOpts{
		Priority:    *prio,
		Description: *desc,
	})
	if err != nil {
		return err
	}
	fmt.Printf("added #%s p%d %s\n", id, *prio, title)
	return nil
}

func cmdUpdate(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: schedg update <db> <id> <title> [--priority N] [--description TEXT]")
	}
	name, id, title := args[0], args[1], args[2]
	fs := flag.NewFlagSet("update", flag.ExitOnError)
	prio := fs.Int64("priority", 0, "task priority")
	desc := fs.String("description", "", "task description")
	fs.Parse(args[3:])

	q, err := open(name)
	if err != nil {
		return err
	}
	defer q.Close()

	if err := q.Update(context.Background(), id, title, source.TaskOpts{
		Priority:    *prio,
		Description: *desc,
	}); err != nil {
		return err
	}
	fmt.Printf("updated #%s\n", id)
	return nil
}

func cmdRemove(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg rm <db> <id>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()

	if err := q.Remove(context.Background(), args[1]); err != nil {
		return err
	}
	fmt.Printf("removed #%s\n", args[1])
	return nil
}

func cmdMarkDone(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: schedg mark-done <db> <id>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()

	if err := q.SetDone(context.Background(), args[1]); err != nil {
		return err
	}
	fmt.Printf("marked #%s done\n", args[1])
	return nil
}

func cmdAddDep(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: schedg add-dep <db> <task-id> <dep-id>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()

	if err := q.AddDep(context.Background(), args[1], args[2]); err != nil {
		return err
	}
	fmt.Printf("added dependency: #%s depends on #%s\n", args[1], args[2])
	return nil
}

func cmdRemoveDep(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: schedg rm-dep <db> <task-id> <dep-id>")
	}
	q, err := open(args[0])
	if err != nil {
		return err
	}
	defer q.Close()

	if err := q.RemoveDep(context.Background(), args[1], args[2]); err != nil {
		return err
	}
	fmt.Printf("removed dependency: #%s no longer depends on #%s\n", args[1], args[2])
	return nil
}

// --- utils ---

func prefix(ss []string, p string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = p + s
	}
	return out
}
