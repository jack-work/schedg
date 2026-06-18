package source

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jack-work/schedg/internal/schema"
)

// sqlDialect captures the small set of SQL differences between backends.
// Everything else (SELECT/INSERT/UPDATE/DELETE) is standard SQL with ?
// placeholders.
type sqlDialect interface {
	name() string
	hasTable(ctx context.Context, db *sql.DB, table string) (bool, error)
	hasColumn(ctx context.Context, db *sql.DB, table, col string) (bool, error)
	upsertSQL(table string, keyCols, valCols []string) string
}

// sqlSource implements Source over a generic *sql.DB with a dialect for the
// handful of things that differ between SQLite, MySQL, etc.
type sqlSource struct {
	db      *sql.DB
	sc      schema.Schema
	dialect sqlDialect
}

func newSQLSource(db *sql.DB, sc schema.Schema, d sqlDialect) *sqlSource {
	return &sqlSource{db: db, sc: sc, dialect: d}
}

func (s *sqlSource) Name() string { return s.dialect.name() }

func (s *sqlSource) Load(ctx context.Context) ([]Row, error) {
	sc := s.sc

	// Pre-init: table may not exist yet.
	if ok, _ := s.dialect.hasTable(ctx, s.db, sc.Table); !ok {
		return nil, nil
	}

	prio := "0"
	if sc.PriorityCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.PriorityCol); ok {
			prio = fmt.Sprintf("COALESCE(%s,0)", sc.PriorityCol)
		}
	}
	sub := "NULL"
	if sc.SubmittedCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.SubmittedCol); ok {
			sub = sc.SubmittedCol
		}
	}
	lbl := "NULL"
	if sc.LabelCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.LabelCol); ok {
			lbl = sc.LabelCol
		}
	}
	desc := "NULL"
	if sc.DescCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.DescCol); ok {
			desc = sc.DescCol
		}
	}
	where := ""
	if sc.DoneCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.DoneCol); ok {
			where = fmt.Sprintf(" WHERE %s=0", sc.DoneCol)
		}
	}

	q := fmt.Sprintf("SELECT %s AS id, %s AS priority, %s AS submitted, %s AS label, %s AS description FROM %s%s",
		sc.PK, prio, sub, lbl, desc, sc.Table, where)
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}

	var result []Row
	for rows.Next() {
		var rawID, rawPrio any
		var submitted, label, descVal sql.NullString
		if err := rows.Scan(&rawID, &rawPrio, &submitted, &label, &descVal); err != nil {
			rows.Close()
			return nil, err
		}
		id := anyToStr(rawID)
		if id == "" {
			continue
		}
		r := Row{
			ID:       id,
			Priority: anyToInt64(rawPrio),
			Fields:   map[string]string{},
		}
		if submitted.Valid {
			r.Submitted = parseTime(submitted.String)
		}
		if label.Valid && label.String != "" {
			r.Fields["label"] = label.String
		}
		if descVal.Valid && descVal.String != "" {
			r.Fields["description"] = descVal.String
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	// Load per-task KV pairs.
	if sc.KVTable != "" {
		if ok, _ := s.dialect.hasTable(ctx, s.db, sc.KVTable); ok {
			kvMap := map[string]map[string]string{}
			kq := fmt.Sprintf("SELECT %s, %s, %s FROM %s", sc.KVTaskCol, sc.KVKeyCol, sc.KVValCol, sc.KVTable)
			krows, err := s.db.QueryContext(ctx, kq)
			if err != nil {
				return nil, err
			}
			defer krows.Close()
			for krows.Next() {
				var rawID any
				var k, v string
				if err := krows.Scan(&rawID, &k, &v); err != nil {
					return nil, err
				}
				id := anyToStr(rawID)
				if kvMap[id] == nil {
					kvMap[id] = map[string]string{}
				}
				kvMap[id][k] = v
			}
			if err := krows.Err(); err != nil {
				return nil, err
			}
			for i := range result {
				result[i].KV = kvMap[result[i].ID]
			}
		}
	}

	// Load dependencies.
	if sc.DepsTable != "" {
		if ok, _ := s.dialect.hasTable(ctx, s.db, sc.DepsTable); ok {
			deps := map[string][]string{}
			dq := fmt.Sprintf("SELECT %s, %s FROM %s", sc.DepFrom, sc.DepTo, sc.DepsTable)
			drows, err := s.db.QueryContext(ctx, dq)
			if err != nil {
				return nil, err
			}
			defer drows.Close()
			for drows.Next() {
				var rawFrom, rawTo any
				if err := drows.Scan(&rawFrom, &rawTo); err != nil {
					return nil, err
				}
				f, t := anyToStr(rawFrom), anyToStr(rawTo)
				if f != "" && t != "" {
					deps[f] = append(deps[f], t)
				}
			}
			if err := drows.Err(); err != nil {
				return nil, err
			}
			for i := range result {
				result[i].Deps = deps[result[i].ID]
			}
		}
	}

	return result, nil
}

func (s *sqlSource) InitSchema(ctx context.Context) error {
	for _, stmt := range s.sc.DDL {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	for col, def := range s.sc.AddColumns() {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, s.sc.Table, col); !ok {
			stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", s.sc.Table, col, def)
			if _, err := s.db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sqlSource) EnsureSavedQueries(ctx context.Context) error {
	return nil
}

func (s *sqlSource) WriteMeta(ctx context.Context, key, val string) error {
	if s.sc.MetaTable == "" {
		return nil
	}
	q := s.dialect.upsertSQL(s.sc.MetaTable, []string{"k"}, []string{"v"})
	_, err := s.db.ExecContext(ctx, q, key, val)
	return err
}

func (s *sqlSource) Passthrough(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s: provide a SQL query string", s.dialect.name())
	}
	query := strings.Join(args, " ")
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		res, err2 := s.db.ExecContext(ctx, query)
		if err2 != nil {
			return err
		}
		n, _ := res.RowsAffected()
		fmt.Printf("%d row(s) affected\n", n)
		return nil
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	fmt.Println(strings.Join(cols, "\t"))
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		strs := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				strs[i] = "NULL"
			} else {
				strs[i] = fmt.Sprintf("%v", v)
			}
		}
		fmt.Println(strings.Join(strs, "\t"))
	}
	return rows.Err()
}

// --- CRUD ---

func (s *sqlSource) AddTask(ctx context.Context, title string, opts TaskOpts) (string, error) {
	sc := s.sc
	cols := []string{sc.LabelCol}
	vals := []any{title}
	ph := []string{"?"}

	if sc.PriorityCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.PriorityCol); ok {
			cols = append(cols, sc.PriorityCol)
			vals = append(vals, opts.Priority)
			ph = append(ph, "?")
		}
	}
	if opts.Description != "" && sc.DescCol != "" {
		cols = append(cols, sc.DescCol)
		vals = append(vals, opts.Description)
		ph = append(ph, "?")
	}
	if opts.ParentID != "" {
		cols = append(cols, "parent_id")
		vals = append(vals, opts.ParentID)
		ph = append(ph, "?")
	}

	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		sc.Table, strings.Join(cols, ", "), strings.Join(ph, ", "))
	res, err := s.db.ExecContext(ctx, q, vals...)
	if err != nil {
		return "", err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(id, 10), nil
}

func (s *sqlSource) UpdateTask(ctx context.Context, id string, title string, opts TaskOpts) error {
	sc := s.sc
	sets := []string{fmt.Sprintf("%s = ?", sc.LabelCol)}
	vals := []any{title}

	if sc.PriorityCol != "" {
		if ok, _ := s.dialect.hasColumn(ctx, s.db, sc.Table, sc.PriorityCol); ok {
			sets = append(sets, fmt.Sprintf("%s = ?", sc.PriorityCol))
			vals = append(vals, opts.Priority)
		}
	}
	if sc.DescCol != "" {
		sets = append(sets, fmt.Sprintf("%s = ?", sc.DescCol))
		vals = append(vals, opts.Description)
	}

	if opts.ParentID != "" {
		sets = append(sets, "parent_id = ?")
		vals = append(vals, opts.ParentID)
	} else {
		sets = append(sets, "parent_id = NULL")
	}

	vals = append(vals, id)
	q := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", sc.Table, strings.Join(sets, ", "), sc.PK)
	_, err := s.db.ExecContext(ctx, q, vals...)
	return err
}

func (s *sqlSource) RemoveTask(ctx context.Context, id string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", s.sc.Table, s.sc.PK)
	_, err := s.db.ExecContext(ctx, q, id)
	return err
}

func (s *sqlSource) SetDone(ctx context.Context, id string, done bool) error {
	if s.sc.DoneCol == "" {
		return fmt.Errorf("schema has no done column")
	}
	val := 0
	if done {
		val = 1
	}
	q := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?", s.sc.Table, s.sc.DoneCol, s.sc.PK)
	_, err := s.db.ExecContext(ctx, q, val, id)
	return err
}

func (s *sqlSource) AddDep(ctx context.Context, taskID, depID string) error {
	if s.sc.DepsTable == "" {
		return fmt.Errorf("schema has no dependency table")
	}
	q := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (?, ?)",
		s.sc.DepsTable, s.sc.DepFrom, s.sc.DepTo)
	_, err := s.db.ExecContext(ctx, q, taskID, depID)
	return err
}

func (s *sqlSource) RemoveDep(ctx context.Context, taskID, depID string) error {
	if s.sc.DepsTable == "" {
		return fmt.Errorf("schema has no dependency table")
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE %s = ? AND %s = ?",
		s.sc.DepsTable, s.sc.DepFrom, s.sc.DepTo)
	_, err := s.db.ExecContext(ctx, q, taskID, depID)
	return err
}

func (s *sqlSource) SetKV(ctx context.Context, taskID, key, value string) error {
	if s.sc.KVTable == "" {
		return fmt.Errorf("schema has no KV table")
	}
	if len(value) > 500 {
		return fmt.Errorf("value exceeds 500 character limit (%d chars)", len(value))
	}
	q := s.dialect.upsertSQL(s.sc.KVTable, []string{s.sc.KVTaskCol, s.sc.KVKeyCol}, []string{s.sc.KVValCol})
	_, err := s.db.ExecContext(ctx, q, taskID, key, value)
	return err
}

func (s *sqlSource) DeleteKV(ctx context.Context, taskID, key string) error {
	if s.sc.KVTable == "" {
		return fmt.Errorf("schema has no KV table")
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE %s = ? AND %s = ?",
		s.sc.KVTable, s.sc.KVTaskCol, s.sc.KVKeyCol)
	_, err := s.db.ExecContext(ctx, q, taskID, key)
	return err
}

func (s *sqlSource) GetKV(ctx context.Context, taskID string) (map[string]string, error) {
	if s.sc.KVTable == "" {
		return nil, nil
	}
	q := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = ?",
		s.sc.KVKeyCol, s.sc.KVValCol, s.sc.KVTable, s.sc.KVTaskCol)
	rows, err := s.db.QueryContext(ctx, q, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (s *sqlSource) SetDBMeta(ctx context.Context, key, value string) error {
	return s.WriteMeta(ctx, key, value)
}

func (s *sqlSource) GetDBMeta(ctx context.Context) (map[string]string, error) {
	if s.sc.MetaTable == "" {
		return nil, nil
	}
	q := fmt.Sprintf("SELECT k, v FROM %s", s.sc.MetaTable)
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (s *sqlSource) Close() error { return s.db.Close() }

// --- type helpers ---

func anyToStr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func anyToInt64(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		return x
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	default:
		return 0
	}
}
