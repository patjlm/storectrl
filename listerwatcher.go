package storectrl

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
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

	opts, err := parseListOpts(options)
	if err != nil {
		return nil, err
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

	listOpts, err := parseListOpts(options)
	if err != nil {
		return nil, err
	}

	if options.SendInitialEvents != nil && *options.SendInitialEvents {
		return s.watchWithInitialEvents(ctx, listCopy, listOpts)
	}

	if options.ResourceVersion != "" {
		rv, err := strconv.ParseInt(options.ResourceVersion, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid ResourceVersion %q: %w", options.ResourceVersion, err)
		}
		listOpts = append(listOpts, WatchFromRevision(rv))
	}

	watcher, err := s.store.Watch(ctx, listCopy, listOpts...)
	if err != nil {
		return nil, err
	}
	return newWatchAdapter(watcher), nil
}

// watchWithInitialEvents implements the watchList protocol: List all objects,
// send each as ADDED, send a BOOKMARK with the list's ResourceVersion, then
// relay live events from Watch(fromRevision=listRV).
func (s *StoreListerWatcher) watchWithInitialEvents(ctx context.Context, listObj client.ObjectList, opts []client.ListOption) (watch.Interface, error) {
	if err := s.store.List(ctx, listObj, opts...); err != nil {
		return nil, err
	}

	rv := listObj.GetResourceVersion()
	rvInt, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid list ResourceVersion %q: %w", rv, err)
	}

	items, err := apimeta.ExtractList(listObj)
	if err != nil {
		return nil, fmt.Errorf("extracting items from list: %w", err)
	}

	watchOpts := make([]client.ListOption, len(opts), len(opts)+1)
	copy(watchOpts, opts)
	watchOpts = append(watchOpts, WatchFromRevision(rvInt))

	watchListCopy := s.listObj.DeepCopyObject().(client.ObjectList)
	watcher, err := s.store.Watch(ctx, watchListCopy, watchOpts...)
	if err != nil {
		return nil, err
	}

	bookmark := bookmarkForList(s.listObj, rv)
	return newInitialEventsAdapter(watcher, items, bookmark), nil
}

func parseListOpts(options metav1.ListOptions) ([]client.ListOption, error) {
	var opts []client.ListOption
	if options.LabelSelector != "" {
		sel, err := labels.Parse(options.LabelSelector)
		if err != nil {
			return nil, err
		}
		opts = append(opts, client.MatchingLabelsSelector{Selector: sel})
	}
	return opts, nil
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

// initialEventsAdapter wraps a storectrl Watcher and prepends initial
// ADDED events followed by a BOOKMARK before relaying live events.
// Implements the watchList protocol expected by client-go's Reflector.
type initialEventsAdapter struct {
	inner    Watcher
	ch       chan watch.Event
	stopCh   chan struct{}
	stopOnce sync.Once
}

func newInitialEventsAdapter(inner Watcher, items []runtime.Object, bookmarkObj runtime.Object) *initialEventsAdapter {
	a := &initialEventsAdapter{
		inner:  inner,
		ch:     make(chan watch.Event, 100),
		stopCh: make(chan struct{}),
	}
	go a.run(items, bookmarkObj)
	return a
}

func (a *initialEventsAdapter) run(items []runtime.Object, bookmarkObj runtime.Object) {
	defer close(a.ch)

	for _, item := range items {
		select {
		case a.ch <- watch.Event{Type: watch.Added, Object: item}:
		case <-a.stopCh:
			return
		}
	}

	if bookmarkObj != nil {
		select {
		case a.ch <- watch.Event{Type: watch.Bookmark, Object: bookmarkObj}:
		case <-a.stopCh:
			return
		}
	}

	for {
		select {
		case <-a.stopCh:
			return
		case evt, ok := <-a.inner.ResultChan():
			if !ok {
				return
			}
			watchEvt := watch.Event{Object: evt.Object}
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

func (a *initialEventsAdapter) Stop() {
	a.stopOnce.Do(func() {
		close(a.stopCh)
	})
	a.inner.Stop()
}

func (a *initialEventsAdapter) ResultChan() <-chan watch.Event {
	return a.ch
}

var _ watch.Interface = &initialEventsAdapter{}

// bookmarkForList creates a zero-value item matching the list's element type
// with only ResourceVersion set. The Reflector type-checks bookmark objects
// against the expected item type, so this must return the correct concrete type.
func bookmarkForList(listObj client.ObjectList, rv string) runtime.Object {
	listVal := reflect.ValueOf(listObj)
	if listVal.Kind() == reflect.Ptr {
		listVal = listVal.Elem()
	}
	itemsField := listVal.FieldByName("Items")
	if !itemsField.IsValid() {
		return nil
	}
	obj := reflect.New(itemsField.Type().Elem()).Interface().(client.Object)
	obj.SetResourceVersion(rv)
	obj.SetAnnotations(map[string]string{
		metav1.InitialEventsAnnotationKey: "true",
	})
	return obj
}
