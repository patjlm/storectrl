package filesystem

import (
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"reconkit"
)

type fileWatcher struct {
	store    *FileStore
	gvk      schema.GroupVersionKind
	ch       chan reconkit.Event
	stopped  atomic.Bool
	stopMu   sync.Mutex
	stopOnce sync.Once
}

func newFileWatcher(store *FileStore, gvk schema.GroupVersionKind) *fileWatcher {
	return &fileWatcher{
		store: store,
		gvk:   gvk,
		ch:    make(chan reconkit.Event, 100),
	}
}

func (w *fileWatcher) ResultChan() <-chan reconkit.Event {
	return w.ch
}

func (w *fileWatcher) Stop() {
	w.stopOnce.Do(func() {
		w.stopped.Store(true)
		w.stopMu.Lock()
		close(w.ch)
		w.stopMu.Unlock()
		w.store.removeWatcher(w.gvk, w)
	})
}

func (w *fileWatcher) isStopped() bool {
	return w.stopped.Load()
}

func (w *fileWatcher) send(event reconkit.Event) {
	if w.isStopped() {
		return
	}

	select {
	case w.ch <- event:
	default:
	}
}
