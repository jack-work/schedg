// Package config is the single registry of queue databases. All registered DBs
// live in one JSON file under the user config dir; per-DB serialized queue state
// lives beside it.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type DB struct {
	Name       string `json:"name"`
	Driver     string `json:"driver"`                // e.g. "dolt", "sqlite"
	Path       string `json:"path"`                  // data-dir / DSN / file path
	Repo       string `json:"repo"`                  // repo location this queue serves
	Comparator string `json:"comparator"`            // priority module name; "" => default
	MaxCancels int    `json:"max_cancels,omitempty"` // auto-bury after N cancels; 0 => unlimited
	LeaseTTL   string `json:"lease_ttl,omitempty"`   // e.g. "10m"; "" => no lease expiry
	StatePath  string `json:"state_path,omitempty"` // override queue state path; "" => default
}

type Config struct {
	DBs []DB `json:"dbs"`
}

func Dir() string {
	if d := os.Getenv("SCHEDG_CONFIG_DIR"); d != "" {
		return d
	}
	base, err := os.UserConfigDir()
	if err != nil || base == "" {
		base = filepath.Join(os.Getenv("HOME"), ".config")
	}
	return filepath.Join(base, "schedg")
}

func path() string     { return filepath.Join(Dir(), "config.json") }
func StateDir() string { return filepath.Join(Dir(), "state") }

func StatePath(name string) string {
	return filepath.Join(StateDir(), name+".json")
}

func Load() (*Config, error) {
	data, err := os.ReadFile(path())
	if os.IsNotExist(err) {
		return &Config{}, nil
	}
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

func (c *Config) Save() error {
	if err := os.MkdirAll(Dir(), 0o755); err != nil {
		return err
	}
	data, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(path(), append(data, '\n'), 0o644)
}

func (c *Config) Find(name string) (*DB, bool) {
	for i := range c.DBs {
		if c.DBs[i].Name == name {
			return &c.DBs[i], true
		}
	}
	return nil, false
}

// Put inserts or replaces a DB by name.
func (c *Config) Put(db DB) {
	if existing, ok := c.Find(db.Name); ok {
		*existing = db
		return
	}
	c.DBs = append(c.DBs, db)
}
