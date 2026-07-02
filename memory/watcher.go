package memory

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"reconkit"
)

type memoryWatcher struct {
	store  *MemoryStore
	gvk    schema.GroupVersionKind
	ch     chan reconkit.Event
	mu     sync.Mutex
	closed bool
}

func newMemoryWatcher(store *MemoryStore, gvk schema.GroupVersionKind, bufSize int) *memoryWatcher {
	return &memoryWatcher{
		store: store,
		gvk:   gvk,
		ch:    make(chan reconkit.Event, bufSize),
	}
}

func (w *memoryWatcher) ResultChan() <-chan reconkit.Event {
	return w.ch
}

func (w *memoryWatcher) Stop() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()

	w.store.removeWatcher(w.gvk, w)
}

func (w *memoryWatcher) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// send delivers an event to the watcher. If the channel buffer is full,
// the watcher is closed so the consumer can reconnect with WatchFromRevision
// and replay missed events from the store's event log.
func (w *memoryWatcher) send(event reconkit.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	select {
	case w.ch <- event:
	default:
		w.closed = true
		close(w.ch)
	}
}
