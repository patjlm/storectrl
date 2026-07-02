package memory

import (
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

	"reconkit"
)

// MemoryStore is an in-memory implementation of reconkit.Store.
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
}

// NewStore creates a new in-memory store with the given scheme.
func NewStore(scheme *runtime.Scheme) *MemoryStore {
	return &MemoryStore{
		scheme:   scheme,
		objects:  make(map[schema.GroupVersionKind]map[client.ObjectKey]client.Object),
		watchers: make(map[schema.GroupVersionKind][]*memoryWatcher),
	}
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
		return &reconkit.NotFoundError{Key: key.String()}
	}

	stored, exists := gvkMap[key]
	if !exists {
		return &reconkit.NotFoundError{Key: key.String()}
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
		return &reconkit.AlreadyExistsError{Key: key.String()}
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

	if accessor.GetResourceVersion() == "" {
		accessor.SetResourceVersion("1")
	}

	// Deep copy and store
	stored := obj.DeepCopyObject().(client.Object)
	gvkMap[key] = stored

	// Notify watchers
	s.notifyWatchers(gvk, reconkit.Event{
		Type:   reconkit.EventAdded,
		Object: stored.DeepCopyObject().(client.Object),
	})

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
		return &reconkit.NotFoundError{Key: key.String()}
	}

	stored, exists := gvkMap[key]
	if !exists {
		return &reconkit.NotFoundError{Key: key.String()}
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
		return &reconkit.ConflictError{Key: key.String()}
	}

	// Increment resource version
	rv, _ := strconv.ParseInt(storedAccessor.GetResourceVersion(), 10, 64)
	newRV := strconv.FormatInt(rv+1, 10)
	objAccessor.SetResourceVersion(newRV)

	// Deep copy and store
	updated := obj.DeepCopyObject().(client.Object)
	gvkMap[key] = updated

	// Notify watchers
	s.notifyWatchers(gvk, reconkit.Event{
		Type:   reconkit.EventModified,
		Object: updated.DeepCopyObject().(client.Object),
	})

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
		return &reconkit.NotFoundError{Key: key.String()}
	}

	stored, exists := gvkMap[key]
	if !exists {
		return &reconkit.NotFoundError{Key: key.String()}
	}

	delete(gvkMap, key)

	// Notify watchers
	s.notifyWatchers(gvk, reconkit.Event{
		Type:   reconkit.EventDeleted,
		Object: stored.DeepCopyObject().(client.Object),
	})

	return nil
}

// Watch creates a watcher for objects of the list type.
func (s *MemoryStore) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (reconkit.Watcher, error) {
	gvk, err := s.gvkForList(list)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	w := newMemoryWatcher(s, gvk)
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

func (s *MemoryStore) notifyWatchers(gvk schema.GroupVersionKind, event reconkit.Event) {
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

func (s *MemoryStore) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return fmt.Errorf("apply not supported by the in-memory store backend")
}
