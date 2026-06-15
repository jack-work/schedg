package heap

import (
	"math/rand"
	"sort"
	"testing"
)

func maxInt(a, b int) int {
	switch {
	case a > b:
		return 1
	case a < b:
		return -1
	default:
		return 0
	}
}

func TestPopReturnsDescendingOrder(t *testing.T) {
	h := New(maxInt, []int{3, 1, 4, 1, 5, 9, 2, 6})
	var got []int
	for {
		v, ok := h.Pop()
		if !ok {
			break
		}
		got = append(got, v)
	}
	want := []int{9, 6, 5, 4, 3, 2, 1, 1}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d: got %d, want %d (%v)", i, got[i], want[i], got)
		}
	}
}

func TestPushPopMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for trial := 0; trial < 200; trial++ {
		n := rng.Intn(64)
		vals := make([]int, n)
		for i := range vals {
			vals[i] = rng.Intn(1000)
		}
		h := New(maxInt, nil)
		for _, v := range vals {
			h.Push(v)
		}
		var got []int
		for {
			v, ok := h.Pop()
			if !ok {
				break
			}
			got = append(got, v)
		}
		want := append([]int(nil), vals...)
		sort.Sort(sort.Reverse(sort.IntSlice(want)))
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("trial %d mismatch at %d: %v vs %v", trial, i, got, want)
			}
		}
	}
}

func TestPeekAndEmpty(t *testing.T) {
	h := New(maxInt, nil)
	if _, ok := h.Pop(); ok {
		t.Fatal("Pop on empty should fail")
	}
	if _, ok := h.Peek(); ok {
		t.Fatal("Peek on empty should fail")
	}
	h.Push(7)
	if v, ok := h.Peek(); !ok || v != 7 {
		t.Fatalf("Peek = %d,%v", v, ok)
	}
	if h.Len() != 1 {
		t.Fatalf("Len = %d after peek (peek must not pop)", h.Len())
	}
}
