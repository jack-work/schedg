// Package schema describes the task table a queue reads from. A Schema is
// passed to a Source to (idempotently) create the table, dependency lookup,
// and metadata. Default(driver) returns the figtodo-shaped schema with DDL
// appropriate for the given SQL backend ("dolt"/"sqlite").
package schema

type SavedQuery struct {
	Name  string
	Desc  string
	Query string
}

type Schema struct {
	Table        string // task table
	PK           string // primary key column (task id)
	PriorityCol  string // ranking column; "" or absent => priority 0
	SubmittedCol string // submission timestamp column
	LabelCol     string // human label for display; "" or absent => none
	DescCol      string // longer-form body (markdown); "" or absent => none
	DoneCol      string // boolean "finished" column; open tasks have it = 0
	DepsTable    string // dependency lookup; absent => no deps
	DepFrom      string // dep table: the dependent task id
	DepTo        string // dep table: the prerequisite task id
	MetaTable    string // single key/value table holding the repo back-reference

	DDL          []string // create statements, run in order, idempotent
	SavedQueries []SavedQuery
}

// Default returns the figtodo-shaped schema with DDL for the given driver.
func Default(driver string) Schema {
	s := Schema{
		Table:        "todo",
		PK:           "id",
		PriorityCol:  "priority",
		SubmittedCol: "created_at",
		LabelCol:     "title",
		DescCol:      "description",
		DoneCol:      "done",
		DepsTable:    "todo_dep",
		DepFrom:      "task_id",
		DepTo:        "depends_on_id",
		MetaTable:    "schedg_meta",
		SavedQueries: defaultSavedQueries(),
	}
	switch driver {
	case "sqlite":
		s.DDL = sqliteDDL()
	default:
		s.DDL = mysqlDDL()
	}
	return s
}

func defaultSavedQueries() []SavedQuery {
	return []SavedQuery{
		{"schedg-open", "Open tasks by priority then age",
			"SELECT id, priority, created_at, title FROM todo WHERE done=0 ORDER BY priority DESC, created_at ASC, id ASC;"},
		{"schedg-deps", "Dependency edges (task depends on prerequisite)",
			"SELECT d.task_id, t.title AS task, d.depends_on_id, p.title AS prerequisite FROM todo_dep d JOIN todo t ON t.id=d.task_id JOIN todo p ON p.id=d.depends_on_id ORDER BY d.task_id;"},
		{"schedg-ready", "Open tasks with no unmet (open) prerequisite",
			"SELECT t.id, t.priority, t.title FROM todo t WHERE t.done=0 AND NOT EXISTS (SELECT 1 FROM todo_dep d JOIN todo p ON p.id=d.depends_on_id WHERE d.task_id=t.id AND p.done=0) ORDER BY t.priority DESC, t.created_at ASC;"},
		{"schedg-meta", "Queue metadata (repo back-reference, etc.)",
			"SELECT k, v FROM schedg_meta ORDER BY k;"},
	}
}

func mysqlDDL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS todo (
                id         INT AUTO_INCREMENT PRIMARY KEY,
                title      VARCHAR(255) NOT NULL,
                done       BOOLEAN NOT NULL DEFAULT FALSE,
                parent_id  INT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                description TEXT NULL,
                CONSTRAINT fk_todo_parent     FOREIGN KEY (parent_id) REFERENCES todo(id) ON DELETE CASCADE,
                CONSTRAINT chk_no_self_parent CHECK (parent_id <> id),
                INDEX idx_todo_parent (parent_id)
            );`,
		`CREATE TABLE IF NOT EXISTS todo_dep (
                task_id       INT NOT NULL,
                depends_on_id INT NOT NULL,
                PRIMARY KEY (task_id, depends_on_id),
                CONSTRAINT fk_dep_task FOREIGN KEY (task_id)       REFERENCES todo(id) ON DELETE CASCADE,
                CONSTRAINT fk_dep_pre  FOREIGN KEY (depends_on_id) REFERENCES todo(id) ON DELETE CASCADE,
                CONSTRAINT chk_no_self_dep CHECK (task_id <> depends_on_id)
            );`,
		`CREATE TABLE IF NOT EXISTS schedg_meta (
                k VARCHAR(64) PRIMARY KEY,
                v TEXT NOT NULL
            );`,
	}
}

func sqliteDDL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS todo (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            title       TEXT DEFAULT '',
            done        INTEGER NOT NULL DEFAULT 0,
            parent_id   INTEGER REFERENCES todo(id) ON DELETE CASCADE CHECK (parent_id != id),
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            description TEXT,
            priority    INTEGER NOT NULL DEFAULT 0
        );`,
		`CREATE INDEX IF NOT EXISTS idx_todo_parent ON todo(parent_id);`,
		`CREATE TABLE IF NOT EXISTS todo_dep (
            task_id       INTEGER NOT NULL REFERENCES todo(id) ON DELETE CASCADE,
            depends_on_id INTEGER NOT NULL REFERENCES todo(id) ON DELETE CASCADE,
            PRIMARY KEY (task_id, depends_on_id),
            CHECK (task_id != depends_on_id)
        );`,
		`CREATE TABLE IF NOT EXISTS schedg_meta (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );`,
	}
}

// AddColumns lists optional columns the schema wants on Table that aren't in
// the base DDL, so a Source can add them only when missing.
func (s Schema) AddColumns() map[string]string {
	cols := map[string]string{}
	if s.PriorityCol != "" {
		cols[s.PriorityCol] = "INT NOT NULL DEFAULT 0"
	}
	return cols
}
