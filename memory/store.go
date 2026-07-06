package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/storectrl"
)

const defaultEventLogSize = 1000

type eventLogEntry struct {
	revision int64
	gvk      schema.GroupVersionKind
	event    storectrl.Event
}

// StoreOption configures a MemoryStore.
type StoreOption func(*MemoryStore)

// WithEventLogSize sets the maximum number of events retained for watch
// resumption. Older events are discarded. Default is 1000.
func WithEventLogSize(size int) StoreOption {
	return func(s *MemoryStore) {
		s.eventLogMax = size
	}
}

// MemoryStore is an in-memory implementation of storectrl.Store.
// It stores objects in memory organized by GVK and ObjectKey,
// and supports watching for changes.
type MemoryStore struct {
	scheme *runtime.Scheme
	mu     sync.RWMutex
	// objects maps GVK -> ObjectKey -> stored object
	objects map[schema.GroupVersionKind]map[client.ObjectKey]client.Object
	// watchers maps GVK -> list of active watchers
	watchers map[schema.GroupVersionKind][]*memoryWatcher
	// uidCounter for generating unique UIDs
	uidCounter atomic.Int64
	// revision is a global monotonic counter incremented on every mutation.
	// Mirrors etcd's MVCC revision — every Create/Update/Delete gets a unique,
	// ordered revision number used as the object's ResourceVersion.
	revision atomic.Int64
	// eventLog is a bounded buffer of recent events for watch resumption.
	eventLog    []eventLogEntry
	eventLogMax int
}

// NewStore creates a new in-memory store with the given scheme.
func NewStore(scheme *runtime.Scheme, opts ...StoreOption) *MemoryStore {
	s := &MemoryStore{
		scheme:      scheme,
		objects:     make(map[schema.GroupVersionKind]map[client.ObjectKey]client.Object),
		watchers:    make(map[schema.GroupVersionKind][]*memoryWatcher),
		eventLogMax: defaultEventLogSize,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Get retrieves an object by key.
func (s *MemoryStore) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	gvkMap, exists := s.objects[gvk]
	if !exists {
		return &storectrl.NotFoundError{Key: key.String()}
	}

	stored, exists := gvkMap[key]
	if !exists {
		return &storectrl.NotFoundError{Key: key.String()}
	}

	// Deep copy via JSON round-trip
	return s.copyObject(stored, obj)
}

// List retrieves all objects matching the list type and options.
func (s *MemoryStore) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk, err := s.gvkForList(list)
	if err != nil {
		return err
	}

	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	gvkMap, exists := s.objects[gvk]
	if !exists {
		gvkMap = make(map[client.ObjectKey]client.Object)
	}

	// Filter objects
	var filtered []client.Object
	for key, obj := range gvkMap {
		// Filter by namespace
		if listOpts.Namespace != "" && key.Namespace != listOpts.Namespace {
			continue
		}

		// Filter by labels
		if listOpts.LabelSelector != nil {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				continue
			}
			labels := accessor.GetLabels()
			if !listOpts.LabelSelector.Matches(toLabelSet(labels)) {
				continue
			}
		}

		// Deep copy the object
		objCopy := obj.DeepCopyObject().(client.Object)
		filtered = append(filtered, objCopy)
	}

	// Set list resource version to current global revision so callers
	// can start a watch from this point without missing events.
	if setter, ok := list.(interface{ SetResourceVersion(string) }); ok {
		setter.SetResourceVersion(strconv.FormatInt(s.revision.Load(), 10))
	}

	// Populate the Items field using reflection
	return s.populateListItems(list, filtered)
}

// Create adds a new object to the store.
func (s *MemoryStore) Create(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)

	s.mu.Lock()
	defer s.mu.Unlock()

	gvkMap, exists := s.objects[gvk]
	if !exists {
		gvkMap = make(map[client.ObjectKey]client.Object)
		s.objects[gvk] = gvkMap
	}

	if _, exists := gvkMap[key]; exists {
		return &storectrl.AlreadyExistsError{Key: key.String()}
	}

	// Set metadata
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	if accessor.GetUID() == "" {
		uid := s.generateUID()
		accessor.SetUID(types.UID(uid))
	}

	rv := s.revision.Add(1)
	accessor.SetResourceVersion(strconv.FormatInt(rv, 10))

	// Deep copy and store
	stored := obj.DeepCopyObject().(client.Object)
	gvkMap[key] = stored

	event := storectrl.Event{
		Type:   storectrl.EventAdded,
		Object: stored.DeepCopyObject().(client.Object),
	}
	s.logEvent(gvk, rv, event)
	s.notifyWatchers(gvk, event)

	// Copy back to obj to include generated fields
	return s.copyObject(stored, obj)
}

// Update modifies an existing object in the store.
func (s *MemoryStore) Update(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)

	s.mu.Lock()
	defer s.mu.Unlock()

	gvkMap, exists := s.objects[gvk]
	if !exists {
		return &storectrl.NotFoundError{Key: key.String()}
	}

	stored, exists := gvkMap[key]
	if !exists {
		return &storectrl.NotFoundError{Key: key.String()}
	}

	// Check resource version
	objAccessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	storedAccessor, err := meta.Accessor(stored)
	if err != nil {
		return err
	}

	if objAccessor.GetResourceVersion() != storedAccessor.GetResourceVersion() {
		return &storectrl.ConflictError{Key: key.String()}
	}

	if s.contentEqual(stored, obj) {
		return s.copyObject(stored, obj)
	}

	rv := s.revision.Add(1)
	objAccessor.SetResourceVersion(strconv.FormatInt(rv, 10))

	// Deep copy and store
	updated := obj.DeepCopyObject().(client.Object)
	gvkMap[key] = updated

	event := storectrl.Event{
		Type:   storectrl.EventModified,
		Object: updated.DeepCopyObject().(client.Object),
	}
	s.logEvent(gvk, rv, event)
	s.notifyWatchers(gvk, event)

	// Copy back to obj to include updated fields
	return s.copyObject(updated, obj)
}

// Delete removes an object from the store.
func (s *MemoryStore) Delete(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)

	s.mu.Lock()
	defer s.mu.Unlock()

	gvkMap, exists := s.objects[gvk]
	if !exists {
		return &storectrl.NotFoundError{Key: key.String()}
	}

	stored, exists := gvkMap[key]
	if !exists {
		return &storectrl.NotFoundError{Key: key.String()}
	}

	delete(gvkMap, key)

	deletedCopy := stored.DeepCopyObject().(client.Object)
	deletedAccessor, err := meta.Accessor(deletedCopy)
	if err != nil {
		return err
	}

	rv := s.revision.Add(1)
	deletedAccessor.SetResourceVersion(strconv.FormatInt(rv, 10))

	event := storectrl.Event{
		Type:   storectrl.EventDeleted,
		Object: deletedCopy,
	}
	s.logEvent(gvk, rv, event)
	s.notifyWatchers(gvk, event)

	return nil
}

// Watch creates a watcher for objects of the list type.
// If WatchFromRevision is passed in opts, events after that revision are
// replayed from the event log before live events begin. Returns
// RevisionTooOldError if the requested revision has been compacted.
func (s *MemoryStore) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (storectrl.Watcher, error) {
	gvk, err := s.gvkForList(list)
	if err != nil {
		return nil, err
	}

	var fromRevision int64
	resuming := false
	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		if rv, ok := opt.(storectrl.WatchFromRevision); ok {
			fromRevision = int64(rv)
			resuming = true
		}
		opt.ApplyToList(listOpts)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var replay []storectrl.Event
	if resuming {
		replay, err = s.eventsSince(fromRevision, gvk)
		if err != nil {
			return nil, err
		}
	}

	if listOpts.LabelSelector != nil {
		filtered := make([]storectrl.Event, 0, len(replay))
		for _, evt := range replay {
			if evt.Type == storectrl.EventBookmark || listOpts.LabelSelector.Matches(toLabelSet(evt.Object.GetLabels())) {
				filtered = append(filtered, evt)
			}
		}
		replay = filtered
	}

	bufSize := 100
	if len(replay) > bufSize {
		bufSize = len(replay) + 100
	}
	w := newMemoryWatcher(s, gvk, bufSize)

	// Pre-load replay events while holding the lock — any mutation after
	// unlock will be caught by the live registration below. No gap.
	for _, evt := range replay {
		w.ch <- evt
	}

	s.watchers[gvk] = append(s.watchers[gvk], w)
	return w, nil
}

// Helper methods

func (s *MemoryStore) gvkForObject(obj client.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK found for object type %T", obj)
	}
	return gvks[0], nil
}

func (s *MemoryStore) gvkForList(list client.ObjectList) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(list)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK found for list type %T", list)
	}

	// Convert list GVK to item GVK (e.g., PodList -> Pod)
	gvk := gvks[0]
	if len(gvk.Kind) > 4 && gvk.Kind[len(gvk.Kind)-4:] == "List" {
		gvk.Kind = gvk.Kind[:len(gvk.Kind)-4]
	}

	return gvk, nil
}

func (s *MemoryStore) contentEqual(stored, incoming client.Object) bool {
	inCopy := incoming.DeepCopyObject().(client.Object)
	inCopy.SetResourceVersion(stored.GetResourceVersion())
	inCopy.SetUID(stored.GetUID())
	a, err1 := json.Marshal(stored)
	b, err2 := json.Marshal(inCopy)
	if err1 != nil || err2 != nil {
		return false
	}
	return bytes.Equal(a, b)
}

func (s *MemoryStore) copyObject(src, dst client.Object) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dst)
}

func (s *MemoryStore) populateListItems(list client.ObjectList, items []client.Object) error {
	listVal := reflect.ValueOf(list)
	if listVal.Kind() == reflect.Ptr {
		listVal = listVal.Elem()
	}

	itemsField := listVal.FieldByName("Items")
	if !itemsField.IsValid() {
		return fmt.Errorf("list type %T does not have Items field", list)
	}

	if !itemsField.CanSet() {
		return fmt.Errorf("Items field of list type %T cannot be set", list)
	}

	itemsSlice := reflect.MakeSlice(itemsField.Type(), 0, len(items))
	for _, item := range items {
		itemVal := reflect.ValueOf(item)
		if itemVal.Kind() == reflect.Ptr {
			itemVal = itemVal.Elem()
		}
		itemsSlice = reflect.Append(itemsSlice, itemVal)
	}

	itemsField.Set(itemsSlice)
	return nil
}

func (s *MemoryStore) generateUID() string {
	id := s.uidCounter.Add(1)
	return fmt.Sprintf("uid-%d", id)
}

func (s *MemoryStore) notifyWatchers(gvk schema.GroupVersionKind, event storectrl.Event) {
	watchers := s.watchers[gvk]

	// Clean up stopped watchers
	active := make([]*memoryWatcher, 0, len(watchers))
	for _, w := range watchers {
		if w.isStopped() {
			continue
		}
		active = append(active, w)
		w.send(event)
	}
	s.watchers[gvk] = active
}

func (s *MemoryStore) removeWatcher(gvk schema.GroupVersionKind, watcher *memoryWatcher) {
	s.mu.Lock()
	defer s.mu.Unlock()

	watchers := s.watchers[gvk]
	filtered := make([]*memoryWatcher, 0, len(watchers))
	for _, w := range watchers {
		if w != watcher {
			filtered = append(filtered, w)
		}
	}
	s.watchers[gvk] = filtered
}

// toLabelSet converts a map to a label set for matching.
type labelSet map[string]string

func (l labelSet) Has(label string) bool {
	_, exists := l[label]
	return exists
}

func (l labelSet) Get(label string) string {
	return l[label]
}

func (l labelSet) Lookup(label string) (string, bool) {
	v, ok := l[label]
	return v, ok
}

func toLabelSet(labels map[string]string) labelSet {
	return labelSet(labels)
}

func (s *MemoryStore) logEvent(gvk schema.GroupVersionKind, revision int64, event storectrl.Event) {
	s.eventLog = append(s.eventLog, eventLogEntry{
		revision: revision,
		gvk:      gvk,
		event:    event,
	})
	if len(s.eventLog) > s.eventLogMax {
		excess := len(s.eventLog) - s.eventLogMax
		trimmed := make([]eventLogEntry, s.eventLogMax)
		copy(trimmed, s.eventLog[excess:])
		s.eventLog = trimmed
	}
}

// eventsSince returns events for the given GVK after fromRevision.
// Must be called under s.mu.
func (s *MemoryStore) eventsSince(fromRevision int64, gvk schema.GroupVersionKind) ([]storectrl.Event, error) {
	currentRevision := s.revision.Load()

	if fromRevision >= currentRevision {
		return nil, nil
	}

	if len(s.eventLog) == 0 {
		return nil, &storectrl.RevisionTooOldError{
			RequestedRevision: fromRevision,
			OldestRevision:    currentRevision + 1,
		}
	}

	oldest := s.eventLog[0].revision
	if fromRevision+1 < oldest {
		return nil, &storectrl.RevisionTooOldError{
			RequestedRevision: fromRevision,
			OldestRevision:    oldest,
		}
	}

	var events []storectrl.Event
	for _, entry := range s.eventLog {
		if entry.revision > fromRevision && entry.gvk == gvk {
			events = append(events, entry.event)
		}
	}
	return events, nil
}

func (s *MemoryStore) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return fmt.Errorf("apply not supported by the in-memory store backend")
}
