package schedg

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLibraryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	ctx := context.Background()

	db, err := Init(ctx, Options{Path: dbPath, StatePath: filepath.Join(dir, "test.state.json")})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer db.Close()

	id1, err := db.Add(ctx, "first", TaskOpts{Priority: 1})
	if err != nil {
		t.Fatalf("Add first: %v", err)
	}
	id2, err := db.Add(ctx, "second", TaskOpts{Priority: 10})
	if err != nil {
		t.Fatalf("Add second: %v", err)
	}

	st := db.Status()
	if st.Ready != 2 {
		t.Fatalf("ready = %d, want 2", st.Ready)
	}

	// Peek returns highest priority without leasing.
	top, ok := db.Peek()
	if !ok || top.ID != id2 {
		t.Fatalf("Peek = %v,%v want id=%s", top, ok, id2)
	}

	// Next leases highest priority.
	task, ok := db.Next()
	if !ok || task.ID != id2 || task.Title != "second" {
		t.Fatalf("Next = %v,%v want id=%s title=second", task, ok, id2)
	}
	if err := db.Save(); err != nil {
		t.Fatal(err)
	}

	// Complete unblocks dependents.
	if err := db.Complete(id2); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	task2, ok := db.Next()
	if !ok || task2.ID != id1 {
		t.Fatalf("Next after complete = %v,%v want id=%s", task2, ok, id1)
	}

	// Mark done in DB.
	if err := db.Done(ctx, id1); err != nil {
		t.Fatalf("Done: %v", err)
	}

	// File should exist.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file missing: %v", err)
	}
}

func TestLibraryDependencies(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := Init(ctx, Options{
		Path:      filepath.Join(dir, "deps.db"),
		StatePath: filepath.Join(dir, "deps.state.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	a, _ := db.Add(ctx, "a", TaskOpts{Priority: 1})
	b, _ := db.Add(ctx, "b", TaskOpts{Priority: 10})

	if err := db.AddDep(ctx, b, a); err != nil {
		t.Fatalf("AddDep: %v", err)
	}

	// b is blocked on a despite higher priority.
	task, ok := db.Next()
	if !ok || task.ID != a {
		t.Fatalf("Next = %v,%v want a (b blocked)", task, ok)
	}
	db.Save()
	if err := db.Complete(a); err != nil {
		t.Fatal(err)
	}
	db.Save()

	task, ok = db.Next()
	if !ok || task.ID != b {
		t.Fatalf("Next after complete a = %v,%v want b", task, ok)
	}
}

func TestLibraryDescription(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := Init(ctx, Options{
		Path:      filepath.Join(dir, "desc.db"),
		StatePath: filepath.Join(dir, "desc.state.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	body := "## Steps\n\n1. Reproduce the bug\n2. Write a failing test\n3. Fix it"
	id, err := db.Add(ctx, "fix the crash", TaskOpts{Priority: 5, Description: body})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	task, ok := db.Peek()
	if !ok || task.ID != id {
		t.Fatalf("Peek = %v,%v want id=%s", task, ok, id)
	}
	if task.Description != body {
		t.Fatalf("Description = %q, want %q", task.Description, body)
	}
	if task.Title != "fix the crash" {
		t.Fatalf("Title = %q, want 'fix the crash'", task.Title)
	}
}

func TestLibraryRemove(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := Init(ctx, Options{
		Path:      filepath.Join(dir, "rm.db"),
		StatePath: filepath.Join(dir, "rm.state.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id, _ := db.Add(ctx, "doomed", TaskOpts{})
	if st := db.Status(); st.Ready != 1 {
		t.Fatalf("ready = %d, want 1", st.Ready)
	}
	if err := db.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if st := db.Status(); st.Ready != 0 {
		t.Fatalf("ready after rm = %d, want 0", st.Ready)
	}
}
