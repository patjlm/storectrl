package filesystem

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/patjlm/ctrlforge"
)

type fileWatcher struct {
	store  *FileStore
	gvk    schema.GroupVersionKind
	ch     chan ctrlforge.Event
	mu     sync.Mutex
	closed bool
}

func newFileWatcher(store *FileStore, gvk schema.GroupVersionKind, bufSize int) *fileWatcher {
	return &fileWatcher{
		store: store,
		gvk:   gvk,
		ch:    make(chan ctrlforge.Event, bufSize),
	}
}

func (w *fileWatcher) ResultChan() <-chan ctrlforge.Event {
	return w.ch
}

func (w *fileWatcher) Stop() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()

	w.store.removeWatcher(w.gvk, w)
}

func (w *fileWatcher) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *fileWatcher) send(event ctrlforge.Event) {
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
