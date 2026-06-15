package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jack-work/schedg/internal/schema"
)

func init() { Register("dolt", newDolt) }

type dolt struct {
	dir string
	sc  schema.Schema
}

func newDolt(cfg Config) (Source, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("dolt: empty data-dir")
	}
	return &dolt{dir: cfg.Path, sc: cfg.Schema}, nil
}

func (d *dolt) Name() string { return "dolt" }

func (d *dolt) cmd(ctx context.Context, args ...string) *exec.Cmd {
	full := append([]string{"--data-dir", d.dir}, args...)
	return exec.CommandContext(ctx, "dolt", full...)
}

// query runs a SELECT and returns rows as decoded maps.
func (d *dolt) query(ctx context.Context, q string) ([]map[string]any, error) {
	var out, errb bytes.Buffer
	c := d.cmd(ctx, "sql", "-r", "json", "-q", q)
	c.Stdout, c.Stderr = &out, &errb
	if err := c.Run(); err != nil {
		return nil, fmt.Errorf("dolt sql: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	if strings.TrimSpace(out.String()) == "" {
		return nil, nil
	}
	var doc struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		return nil, fmt.Errorf("dolt sql: decode: %w", err)
	}
	return doc.Rows, nil
}

func (d *dolt) exec(ctx context.Context, q string) error {
	var errb bytes.Buffer
	c := d.cmd(ctx, "sql", "-q", q)
	c.Stderr = &errb
	if err := c.Run(); err != nil {
		return fmt.Errorf("dolt sql: %w: %s", err, strings.TrimSpace(errb.String()))
	}
	return nil
}

func (d *dolt) hasColumn(ctx context.Context, table, col string) bool {
	rows, err := d.query(ctx, fmt.Sprintf(
		"SELECT 1 AS x FROM information_schema.columns WHERE table_name=%s AND column_name=%s LIMIT 1;",
		sqlStr(table), sqlStr(col)))
	return err == nil && len(rows) > 0
}

func (d *dolt) hasTable(ctx context.Context, table string) bool {
	rows, err := d.query(ctx, fmt.Sprintf(
		"SELECT 1 AS x FROM information_schema.tables WHERE table_name=%s LIMIT 1;", sqlStr(table)))
	return err == nil && len(rows) > 0
}

func (d *dolt) Load(ctx context.Context) ([]Row, error) {
	sc := d.sc
	idExpr := sc.PK

	prio := "0"
	if sc.PriorityCol != "" && d.hasColumn(ctx, sc.Table, sc.PriorityCol) {
		prio = fmt.Sprintf("COALESCE(%s,0)", sc.PriorityCol)
	}
	sub := "NULL"
	if sc.SubmittedCol != "" && d.hasColumn(ctx, sc.Table, sc.SubmittedCol) {
		sub = sc.SubmittedCol
	}
	lbl := "NULL"
	if sc.LabelCol != "" && d.hasColumn(ctx, sc.Table, sc.LabelCol) {
		lbl = sc.LabelCol
	}
	where := ""
	if sc.DoneCol != "" && d.hasColumn(ctx, sc.Table, sc.DoneCol) {
		where = fmt.Sprintf(" WHERE %s=0", sc.DoneCol)
	}

	q := fmt.Sprintf("SELECT %s AS id, %s AS priority, %s AS submitted, %s AS label FROM %s%s;",
		idExpr, prio, sub, lbl, sc.Table, where)
	raw, err := d.query(ctx, q)
	if err != nil {
		return nil, err
	}

	deps := map[string][]string{}
	if sc.DepsTable != "" && d.hasTable(ctx, sc.DepsTable) {
		dq := fmt.Sprintf("SELECT %s AS f, %s AS t FROM %s;", sc.DepFrom, sc.DepTo, sc.DepsTable)
		drows, err := d.query(ctx, dq)
		if err != nil {
			return nil, err
		}
		for _, r := range drows {
			f, t := toStr(r["f"]), toStr(r["t"])
			if f != "" && t != "" {
				deps[f] = append(deps[f], t)
			}
		}
	}

	out := make([]Row, 0, len(raw))
	for _, r := range raw {
		id := toStr(r["id"])
		if id == "" {
			continue
		}
		row := Row{
			ID:        id,
			Priority:  toInt64(r["priority"]),
			Submitted: toTime(r["submitted"]),
			Deps:      deps[id],
		}
		if l := toStr(r["label"]); l != "" {
			row.Fields = map[string]string{"label": l}
		}
		out = append(out, row)
	}
	return out, nil
}

func (d *dolt) InitSchema(ctx context.Context) error {
	for _, stmt := range d.sc.DDL {
		if err := d.exec(ctx, stmt); err != nil {
			return err
		}
	}
	for col, def := range d.sc.AddColumns() {
		if !d.hasColumn(ctx, d.sc.Table, col) {
			if err := d.exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", d.sc.Table, col, def)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *dolt) EnsureSavedQueries(ctx context.Context) error {
	for _, sq := range d.sc.SavedQueries {
		// dolt_query_catalog upsert keyed on id (= name).
		q := fmt.Sprintf(
			"REPLACE INTO dolt_query_catalog (id, display_order, name, query, description) "+
				"VALUES (%s, (SELECT COALESCE(MAX(display_order),0)+1 FROM dolt_query_catalog AS c), %s, %s, %s);",
			sqlStr(sq.Name), sqlStr(sq.Name), sqlStr(sq.Query), sqlStr(sq.Desc))
		if err := d.exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (d *dolt) WriteMeta(ctx context.Context, key, val string) error {
	if d.sc.MetaTable == "" {
		return nil
	}
	return d.exec(ctx, fmt.Sprintf("REPLACE INTO %s (k, v) VALUES (%s, %s);",
		d.sc.MetaTable, sqlStr(key), sqlStr(val)))
}

func (d *dolt) Passthrough(ctx context.Context, args []string) error {
	c := d.cmd(ctx, args...)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	return c.Run()
}

func (d *dolt) Close() error { return nil }

// --- helpers ---

func sqlStr(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func toStr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case json.Number:
		n, _ := x.Int64()
		return n
	default:
		return 0
	}
}

var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999 -0700 MST",
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
}

func toTime(v any) time.Time {
	s := toStr(v)
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
