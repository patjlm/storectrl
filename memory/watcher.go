package memory

import (
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"reconkit"
)

// memoryWatcher implements reconkit.Watcher for the in-memory store.
type memoryWatcher struct {
	store    *MemoryStore
	gvk      schema.GroupVersionKind
	ch       chan reconkit.Event
	stopped  atomic.Bool
	stopMu   sync.Mutex
	stopOnce sync.Once
}

// newMemoryWatcher creates a new watcher for the given GVK.
func newMemoryWatcher(store *MemoryStore, gvk schema.GroupVersionKind) *memoryWatcher {
	return &memoryWatcher{
		store: store,
		gvk:   gvk,
		ch:    make(chan reconkit.Event, 100),
	}
}

// ResultChan returns the channel for receiving watch events.
func (w *memoryWatcher) ResultChan() <-chan reconkit.Event {
	return w.ch
}

// Stop closes the watcher and stops sending events.
func (w *memoryWatcher) Stop() {
	w.stopOnce.Do(func() {
		w.stopped.Store(true)
		w.stopMu.Lock()
		close(w.ch)
		w.stopMu.Unlock()

		// Remove this watcher from the store
		w.store.removeWatcher(w.gvk, w)
	})
}

// isStopped returns whether the watcher has been stopped.
func (w *memoryWatcher) isStopped() bool {
	return w.stopped.Load()
}

// send sends an event to the watcher's channel.
// It is non-blocking and will drop events if the channel is full or the watcher is stopped.
func (w *memoryWatcher) send(event reconkit.Event) {
	if w.isStopped() {
		return
	}

	// Non-blocking send
	select {
	case w.ch <- event:
		// Event sent successfully
	default:
		// Channel full, drop the event
		// In a production implementation, you might want to log this
	}
}
