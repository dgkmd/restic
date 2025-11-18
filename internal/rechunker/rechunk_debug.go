package rechunker

import "sync"

type debugNoteType struct {
	d  map[string]int
	mu sync.Mutex
}

func newDebugNote() *debugNoteType {
	return &debugNoteType{
		d: map[string]int{},
	}
}

func (n *debugNoteType) Add(k string, v int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.d[k] += v
}

func (n *debugNoteType) AddMap(m map[string]int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for k, v := range m {
		n.d[k] += v
	}
}

func (n *debugNoteType) UpdateMax(k string, v int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.d[k] < v {
		n.d[k] = v
	}
}
