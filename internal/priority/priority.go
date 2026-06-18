// Package priority defines the pluggable ranking interface for queue tasks.
//
// A Comparator is the "get priority" hook: any module can Register one and
// select it by name. Compare returns >0 when a outranks b (max-heap order).
package priority

import (
	"sort"
	"time"
)

type Task struct {
	ID        string            `json:"id"`
	Priority  int64             `json:"priority"`
	Submitted time.Time         `json:"submitted"`
	Seq       int64             `json:"seq"`
	Fields    map[string]string `json:"fields,omitempty"`
	KV        map[string]string `json:"kv,omitempty"`
}

type Comparator interface {
	Name() string
	Compare(a, b Task) int
}

var registry = map[string]Comparator{}

func Register(c Comparator) { registry[c.Name()] = c }

func Get(name string) (Comparator, bool) {
	c, ok := registry[name]
	return c, ok
}

func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// prioritySubmitted is the default: higher priority first, then earlier
// submission, then lower seq (stable FIFO within a rank).
type prioritySubmitted struct{}

func (prioritySubmitted) Name() string { return "priority-submitted" }

func (prioritySubmitted) Compare(a, b Task) int {
	if a.Priority != b.Priority {
		return cmpInt64(a.Priority, b.Priority)
	}
	if !a.Submitted.Equal(b.Submitted) {
		// earlier submitted outranks => invert
		return -cmpInt64(a.Submitted.UnixNano(), b.Submitted.UnixNano())
	}
	return -cmpInt64(a.Seq, b.Seq)
}

func cmpInt64(x, y int64) int {
	switch {
	case x > y:
		return 1
	case x < y:
		return -1
	default:
		return 0
	}
}

func init() { Register(prioritySubmitted{}) }

// Default is the comparator used when a DB names none.
func Default() Comparator { return prioritySubmitted{} }
