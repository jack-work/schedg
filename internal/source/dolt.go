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
	dir    string
	sc     schema.Schema
	colHit map[string]bool // cached hasColumn results
	tblHit map[string]bool // cached hasTable results
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
	key := table + "." + col
	if v, ok := d.colHit[key]; ok {
		return v
	}
	rows, err := d.query(ctx, fmt.Sprintf(
		"SELECT 1 AS x FROM information_schema.columns WHERE table_name=%s AND column_name=%s LIMIT 1;",
		sqlStr(table), sqlStr(col)))
	v := err == nil && len(rows) > 0
	if d.colHit == nil {
		d.colHit = map[string]bool{}
	}
	d.colHit[key] = v
	return v
}

func (d *dolt) hasTable(ctx context.Context, table string) bool {
	if v, ok := d.tblHit[table]; ok {
		return v
	}
	rows, err := d.query(ctx, fmt.Sprintf(
		"SELECT 1 AS x FROM information_schema.tables WHERE table_name=%s LIMIT 1;", sqlStr(table)))
	v := err == nil && len(rows) > 0
	if d.tblHit == nil {
		d.tblHit = map[string]bool{}
	}
	d.tblHit[table] = v
	return v
}

func (d *dolt) Load(ctx context.Context) ([]Row, error) {
	sc := d.sc

	if !d.hasTable(ctx, sc.Table) {
		return nil, nil
	}

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
	desc := "NULL"
	if sc.DescCol != "" && d.hasColumn(ctx, sc.Table, sc.DescCol) {
		desc = sc.DescCol
	}
	where := ""
	if sc.DoneCol != "" && d.hasColumn(ctx, sc.Table, sc.DoneCol) {
		where = fmt.Sprintf(" WHERE %s=0", sc.DoneCol)
	}

	q := fmt.Sprintf("SELECT %s AS id, %s AS priority, %s AS submitted, %s AS label, %s AS description FROM %s%s;",
		idExpr, prio, sub, lbl, desc, sc.Table, where)
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
			Fields:    map[string]string{},
		}
		if l := toStr(r["label"]); l != "" {
			row.Fields["label"] = l
		}
		if d := toStr(r["description"]); d != "" {
			row.Fields["description"] = d
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

// --- CRUD ---

func (d *dolt) AddTask(ctx context.Context, title string, opts TaskOpts) (string, error) {
	sc := d.sc
	cols := []string{sc.LabelCol}
	vals := []string{sqlStr(title)}

	if sc.PriorityCol != "" && d.hasColumn(ctx, sc.Table, sc.PriorityCol) {
		cols = append(cols, sc.PriorityCol)
		vals = append(vals, strconv.FormatInt(opts.Priority, 10))
	}
	if opts.Description != "" && sc.DescCol != "" {
		cols = append(cols, sc.DescCol)
		vals = append(vals, sqlStr(opts.Description))
	}
	if opts.ParentID != "" {
		cols = append(cols, "parent_id")
		vals = append(vals, opts.ParentID)
	}

	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		sc.Table, strings.Join(cols, ", "), strings.Join(vals, ", "))
	if err := d.exec(ctx, q); err != nil {
		return "", err
	}

	rows, err := d.query(ctx, "SELECT LAST_INSERT_ID() AS id;")
	if err != nil || len(rows) == 0 {
		return "", fmt.Errorf("dolt: could not retrieve last insert id: %v", err)
	}
	return toStr(rows[0]["id"]), nil
}

func (d *dolt) UpdateTask(ctx context.Context, id string, title string, opts TaskOpts) error {
	sc := d.sc
	sets := []string{fmt.Sprintf("%s=%s", sc.LabelCol, sqlStr(title))}

	if sc.PriorityCol != "" && d.hasColumn(ctx, sc.Table, sc.PriorityCol) {
		sets = append(sets, fmt.Sprintf("%s=%d", sc.PriorityCol, opts.Priority))
	}
	if d.sc.DescCol != "" {
		sets = append(sets, fmt.Sprintf("%s=%s", d.sc.DescCol, sqlStr(opts.Description)))
	}
	if opts.ParentID != "" {
		sets = append(sets, fmt.Sprintf("parent_id=%s", opts.ParentID))
	} else {
		sets = append(sets, "parent_id=NULL")
	}

	return d.exec(ctx, fmt.Sprintf("UPDATE %s SET %s WHERE %s=%s;",
		sc.Table, strings.Join(sets, ", "), sc.PK, sqlStr(id)))
}

func (d *dolt) RemoveTask(ctx context.Context, id string) error {
	return d.exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s=%s;",
		d.sc.Table, d.sc.PK, sqlStr(id)))
}

func (d *dolt) SetDone(ctx context.Context, id string, done bool) error {
	if d.sc.DoneCol == "" {
		return fmt.Errorf("schema has no done column")
	}
	val := "0"
	if done {
		val = "1"
	}
	return d.exec(ctx, fmt.Sprintf("UPDATE %s SET %s=%s WHERE %s=%s;",
		d.sc.Table, d.sc.DoneCol, val, d.sc.PK, sqlStr(id)))
}

func (d *dolt) AddDep(ctx context.Context, taskID, depID string) error {
	if d.sc.DepsTable == "" {
		return fmt.Errorf("schema has no dependency table")
	}
	return d.exec(ctx, fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (%s, %s);",
		d.sc.DepsTable, d.sc.DepFrom, d.sc.DepTo, sqlStr(taskID), sqlStr(depID)))
}

func (d *dolt) RemoveDep(ctx context.Context, taskID, depID string) error {
	if d.sc.DepsTable == "" {
		return fmt.Errorf("schema has no dependency table")
	}
	return d.exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s=%s AND %s=%s;",
		d.sc.DepsTable, d.sc.DepFrom, sqlStr(taskID), d.sc.DepTo, sqlStr(depID)))
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

func toTime(v any) time.Time {
	return parseTime(toStr(v))
}
