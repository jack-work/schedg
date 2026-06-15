// Package heap is a generic binary max-heap.
//
// Invariants (hold before/after every Push/Pop):
//
//	I1. arr is dense — every index in [0,len) holds a value.
//	I2. max-heap: for every i>0, cmp(arr[parent(i)], arr[i]) >= 0.
//	I3. cmp is total; only its sign matters (>0 => left ranks higher).
package heap

type Heap[T any] struct {
	arr []T
	cmp func(a, b T) int
}

func New[T any](cmp func(a, b T) int, initial []T) *Heap[T] {
	h := &Heap[T]{cmp: cmp}
	if len(initial) > 0 {
		h.arr = append(h.arr, initial...)
		for i := len(h.arr)/2 - 1; i >= 0; i-- {
			h.down(i)
		}
	}
	return h
}

func (h *Heap[T]) Len() int { return len(h.arr) }

func (h *Heap[T]) Push(v T) {
	h.arr = append(h.arr, v)
	h.up(len(h.arr) - 1)
}

func (h *Heap[T]) Pop() (T, bool) {
	var zero T
	n := len(h.arr)
	if n == 0 {
		return zero, false
	}
	top := h.arr[0]
	h.arr[0] = h.arr[n-1]
	h.arr[n-1] = zero
	h.arr = h.arr[:n-1]
	if len(h.arr) > 0 {
		h.down(0)
	}
	return top, true
}

func (h *Heap[T]) Peek() (T, bool) {
	var zero T
	if len(h.arr) == 0 {
		return zero, false
	}
	return h.arr[0], true
}

// Items returns the backing slice (heap order, not sorted). Read-only.
func (h *Heap[T]) Items() []T { return h.arr }

func (h *Heap[T]) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.cmp(h.arr[i], h.arr[parent]) <= 0 {
			break
		}
		h.arr[i], h.arr[parent] = h.arr[parent], h.arr[i]
		i = parent
	}
}

func (h *Heap[T]) down(i int) {
	n := len(h.arr)
	for {
		l, r, largest := 2*i+1, 2*i+2, i
		if l < n && h.cmp(h.arr[l], h.arr[largest]) > 0 {
			largest = l
		}
		if r < n && h.cmp(h.arr[r], h.arr[largest]) > 0 {
			largest = r
		}
		if largest == i {
			break
		}
		h.arr[i], h.arr[largest] = h.arr[largest], h.arr[i]
		i = largest
	}
}
