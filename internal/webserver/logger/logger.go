// Package logger provides a ring-buffer log with SSE subscriber support.
package logger

import (
	"fmt"
	"sync"
	"time"
)

type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelError Level = "error"
)

type Entry struct {
	Time    string `json:"time"`
	Level   Level  `json:"level"`
	Message string `json:"message"`
}

type RingLog struct {
	mu      sync.Mutex
	entries []Entry
	cap     int
	subs    map[chan Entry]struct{}
}

func New(capacity int) *RingLog {
	return &RingLog{
		entries: make([]Entry, 0, capacity),
		cap:     capacity,
		subs:    make(map[chan Entry]struct{}),
	}
}

func (r *RingLog) append(level Level, format string, args ...any) {
	e := Entry{
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
		Level:   level,
		Message: fmt.Sprintf(format, args...),
	}
	r.mu.Lock()
	if len(r.entries) >= r.cap {
		r.entries = r.entries[1:]
	}
	r.entries = append(r.entries, e)
	for ch := range r.subs {
		select {
		case ch <- e:
		default:
		}
	}
	r.mu.Unlock()
}

func (r *RingLog) Debug(format string, args ...any) { r.append(LevelDebug, format, args...) }
func (r *RingLog) Info(format string, args ...any)  { r.append(LevelInfo, format, args...) }
func (r *RingLog) Error(format string, args ...any) { r.append(LevelError, format, args...) }

func (r *RingLog) Entries() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Entry, len(r.entries))
	copy(out, r.entries)
	return out
}

func (r *RingLog) Subscribe() chan Entry {
	ch := make(chan Entry, 64)
	r.mu.Lock()
	r.subs[ch] = struct{}{}
	r.mu.Unlock()
	return ch
}

func (r *RingLog) Unsubscribe(ch chan Entry) {
	r.mu.Lock()
	delete(r.subs, ch)
	r.mu.Unlock()
	close(ch)
}
