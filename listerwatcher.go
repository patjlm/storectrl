package storectrl

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// StoreListerWatcher adapts a Store into a ListerWatcher for a single type.
// It implements both ListerWatcher and ListerWatcherWithContext so that
// client-go's Reflector can drive List/Watch against any Store backend.
type StoreListerWatcher struct {
	store   Store
	listObj client.ObjectList
	ctx     context.Context
}

// NewStoreListerWatcher creates a ListerWatcher that delegates to the given Store.
// listObj is a prototype used to create fresh list instances for each List/Watch call.
func NewStoreListerWatcher(store Store, listObj client.ObjectList) *StoreListerWatcher {
	return &StoreListerWatcher{
		store:   store,
		listObj: listObj,
		ctx:     context.Background(),
	}
}

func (s *StoreListerWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return s.ListWithContext(s.ctx, options)
}

func (s *StoreListerWatcher) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	listCopy := s.listObj.DeepCopyObject().(client.ObjectList)

	var opts []client.ListOption
	if options.LabelSelector != "" {
		sel, err := labels.Parse(options.LabelSelector)
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.MatchingLabelsSelector{Selector: sel})
	}

	if err := s.store.List(ctx, listCopy, opts...); err != nil {
		return nil, err
	}
	return listCopy, nil
}

func (s *StoreListerWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return s.WatchWithContext(s.ctx, options)
}

func (s *StoreListerWatcher) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	listCopy := s.listObj.DeepCopyObject().(client.ObjectList)

	var opts []client.ListOption
	if options.ResourceVersion != "" {
		rv, err := strconv.ParseInt(options.ResourceVersion, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid ResourceVersion %q: %w", options.ResourceVersion, err)
		}
		opts = append(opts, WatchFromRevision(rv))
	}

	watcher, err := s.store.Watch(ctx, listCopy, opts...)
	if err != nil {
		return nil, err
	}
	return newWatchAdapter(watcher), nil
}

var _ toolscache.ListerWatcher = &StoreListerWatcher{}
var _ toolscache.ListerWatcherWithContext = &StoreListerWatcher{}

// watchAdapter wraps a storectrl Watcher as a watch.Interface.
type watchAdapter struct {
	inner    Watcher
	ch       chan watch.Event
	stopCh   chan struct{}
	stopOnce sync.Once
}

func newWatchAdapter(inner Watcher) *watchAdapter {
	a := &watchAdapter{
		inner:  inner,
		ch:     make(chan watch.Event, 100),
		stopCh: make(chan struct{}),
	}
	go a.relay()
	return a
}

func (a *watchAdapter) relay() {
	defer close(a.ch)
	for {
		select {
		case <-a.stopCh:
			return
		case evt, ok := <-a.inner.ResultChan():
			if !ok {
				return
			}
			watchEvt := watch.Event{
				Object: evt.Object,
			}
			switch evt.Type {
			case EventAdded:
				watchEvt.Type = watch.Added
			case EventModified:
				watchEvt.Type = watch.Modified
			case EventDeleted:
				watchEvt.Type = watch.Deleted
			case EventBookmark:
				watchEvt.Type = watch.Bookmark
			}
			select {
			case a.ch <- watchEvt:
			case <-a.stopCh:
				return
			}
		}
	}
}

func (a *watchAdapter) Stop() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
	a.inner.Stop()
}

func (a *watchAdapter) ResultChan() <-chan watch.Event {
	return a.ch
}

var _ watch.Interface = &watchAdapter{}
