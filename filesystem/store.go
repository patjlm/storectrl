package filesystem

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"reconkit"
)

const defaultEventLogSize = 1000

type eventLogEntry struct {
	revision int64
	gvk      schema.GroupVersionKind
	event    reconkit.Event
}

// StoreOption configures a FileStore.
type StoreOption func(*FileStore)

// WithEventLogSize sets the maximum number of events retained for watch
// resumption. Default is 1000.
func WithEventLogSize(size int) StoreOption {
	return func(s *FileStore) {
		s.eventLogMax = size
	}
}

// FileStore is a filesystem-backed implementation of reconkit.Store.
// Objects are stored as JSON files organized by GVK, namespace, and name:
//
//	<root>/<group>/<version>/<kind>/<namespace>/<name>.json
//
// Empty group uses "core". Empty namespace uses "_cluster".
// The global revision counter is persisted at <root>/.revision.
type FileStore struct {
	scheme      *runtime.Scheme
	root        string
	mu          sync.RWMutex
	watchers    map[schema.GroupVersionKind][]*fileWatcher
	uidCounter  atomic.Int64
	revision    atomic.Int64
	eventLog    []eventLogEntry
	eventLogMax int
}

// NewStore creates a new filesystem-backed store rooted at the given directory.
// The global revision counter is loaded from <root>/.revision if it exists.
func NewStore(root string, scheme *runtime.Scheme, opts ...StoreOption) *FileStore {
	s := &FileStore{
		scheme:      scheme,
		root:        root,
		watchers:    make(map[schema.GroupVersionKind][]*fileWatcher),
		eventLogMax: defaultEventLogSize,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.loadRevision()
	return s
}

func (s *FileStore) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(objectPath(s.root, gvk, key))
	if err != nil {
		if os.IsNotExist(err) {
			return &reconkit.NotFoundError{Key: key.String()}
		}
		return err
	}

	return json.Unmarshal(data, obj)
}

func (s *FileStore) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
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

	dir := gvkDir(s.root, gvk)
	var items []client.Object

	nsDirs, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return s.populateListItems(list, items)
		}
		return err
	}

	for _, nsDir := range nsDirs {
		if !nsDir.IsDir() {
			continue
		}

		ns := nsDir.Name()
		if ns == "_cluster" {
			ns = ""
		}

		if listOpts.Namespace != "" && ns != listOpts.Namespace {
			continue
		}

		files, err := os.ReadDir(filepath.Join(dir, nsDir.Name()))
		if err != nil {
			return err
		}

		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
				continue
			}

			data, err := os.ReadFile(filepath.Join(dir, nsDir.Name(), f.Name()))
			if err != nil {
				return err
			}

			rObj, err := s.scheme.New(gvk)
			if err != nil {
				return err
			}

			clientObj, ok := rObj.(client.Object)
			if !ok {
				return fmt.Errorf("type %T does not implement client.Object", rObj)
			}

			if err := json.Unmarshal(data, clientObj); err != nil {
				return err
			}

			if listOpts.LabelSelector != nil {
				accessor, err := meta.Accessor(clientObj)
				if err != nil {
					continue
				}
				if !listOpts.LabelSelector.Matches(toLabelSet(accessor.GetLabels())) {
					continue
				}
			}

			items = append(items, clientObj)
		}
	}

	if setter, ok := list.(interface{ SetResourceVersion(string) }); ok {
		setter.SetResourceVersion(strconv.FormatInt(s.revision.Load(), 10))
	}

	return s.populateListItems(list, items)
}

func (s *FileStore) Create(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)

	s.mu.Lock()
	defer s.mu.Unlock()

	path := objectPath(s.root, gvk, key)

	if _, err := os.Stat(path); err == nil {
		return &reconkit.AlreadyExistsError{Key: key.String()}
	}

	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	if accessor.GetUID() == "" {
		accessor.SetUID(types.UID(s.generateUID()))
	}

	rv := s.revision.Add(1)
	accessor.SetResourceVersion(strconv.FormatInt(rv, 10))

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}

	s.persistRevision(rv)

	event := reconkit.Event{
		Type:   reconkit.EventAdded,
		Object: obj.DeepCopyObject().(client.Object),
	}
	s.logEvent(gvk, rv, event)
	s.notifyWatchers(gvk, event)

	return nil
}

func (s *FileStore) Update(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)

	s.mu.Lock()
	defer s.mu.Unlock()

	path := objectPath(s.root, gvk, key)

	existing, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &reconkit.NotFoundError{Key: key.String()}
		}
		return err
	}

	storedObj, err := s.scheme.New(gvk)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(existing, storedObj); err != nil {
		return err
	}

	objAccessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	storedAccessor, err := meta.Accessor(storedObj)
	if err != nil {
		return err
	}

	if objAccessor.GetResourceVersion() != storedAccessor.GetResourceVersion() {
		return &reconkit.ConflictError{Key: key.String()}
	}

	rv := s.revision.Add(1)
	objAccessor.SetResourceVersion(strconv.FormatInt(rv, 10))

	data, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}

	s.persistRevision(rv)

	event := reconkit.Event{
		Type:   reconkit.EventModified,
		Object: obj.DeepCopyObject().(client.Object),
	}
	s.logEvent(gvk, rv, event)
	s.notifyWatchers(gvk, event)

	return nil
}

func (s *FileStore) Delete(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	key := client.ObjectKeyFromObject(obj)

	s.mu.Lock()
	defer s.mu.Unlock()

	path := objectPath(s.root, gvk, key)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &reconkit.NotFoundError{Key: key.String()}
		}
		return err
	}

	storedObj, err := s.scheme.New(gvk)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, storedObj); err != nil {
		return err
	}

	if err := os.Remove(path); err != nil {
		return err
	}

	deletedObj := storedObj.(client.Object)
	deletedAccessor, err := meta.Accessor(deletedObj)
	if err != nil {
		return err
	}

	rv := s.revision.Add(1)
	s.persistRevision(rv)
	deletedAccessor.SetResourceVersion(strconv.FormatInt(rv, 10))

	event := reconkit.Event{
		Type:   reconkit.EventDeleted,
		Object: deletedObj,
	}
	s.logEvent(gvk, rv, event)
	s.notifyWatchers(gvk, event)

	return nil
}

func (s *FileStore) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (reconkit.Watcher, error) {
	gvk, err := s.gvkForList(list)
	if err != nil {
		return nil, err
	}

	var fromRevision int64
	resuming := false
	for _, opt := range opts {
		if rv, ok := opt.(reconkit.WatchFromRevision); ok {
			fromRevision = int64(rv)
			resuming = true
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var replay []reconkit.Event
	if resuming {
		replay, err = s.eventsSince(fromRevision, gvk)
		if err != nil {
			return nil, err
		}
	}

	bufSize := 100
	if len(replay) > bufSize {
		bufSize = len(replay) + 100
	}
	w := newFileWatcher(s, gvk, bufSize)

	for _, evt := range replay {
		w.ch <- evt
	}

	s.watchers[gvk] = append(s.watchers[gvk], w)
	return w, nil
}

func (s *FileStore) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return fmt.Errorf("apply not supported by the filesystem store backend")
}

func (s *FileStore) revisionPath() string {
	return filepath.Join(s.root, ".revision")
}

func (s *FileStore) loadRevision() {
	data, err := os.ReadFile(s.revisionPath())
	if err != nil {
		return
	}
	rv, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return
	}
	s.revision.Store(rv)
}

func (s *FileStore) persistRevision(rv int64) {
	_ = os.MkdirAll(s.root, 0755)
	_ = os.WriteFile(s.revisionPath(), []byte(strconv.FormatInt(rv, 10)), 0644)
}

func (s *FileStore) logEvent(gvk schema.GroupVersionKind, revision int64, event reconkit.Event) {
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

func (s *FileStore) eventsSince(fromRevision int64, gvk schema.GroupVersionKind) ([]reconkit.Event, error) {
	currentRevision := s.revision.Load()

	if fromRevision >= currentRevision {
		return nil, nil
	}

	if len(s.eventLog) == 0 {
		return nil, &reconkit.RevisionTooOldError{
			RequestedRevision: fromRevision,
			OldestRevision:    currentRevision + 1,
		}
	}

	oldest := s.eventLog[0].revision
	if fromRevision+1 < oldest {
		return nil, &reconkit.RevisionTooOldError{
			RequestedRevision: fromRevision,
			OldestRevision:    oldest,
		}
	}

	var events []reconkit.Event
	for _, entry := range s.eventLog {
		if entry.revision > fromRevision && entry.gvk == gvk {
			events = append(events, entry.event)
		}
	}
	return events, nil
}

// --- helpers ---

func gvkDir(root string, gvk schema.GroupVersionKind) string {
	group := gvk.Group
	if group == "" {
		group = "core"
	}
	return filepath.Join(root, group, gvk.Version, gvk.Kind)
}

func objectPath(root string, gvk schema.GroupVersionKind, key client.ObjectKey) string {
	ns := key.Namespace
	if ns == "" {
		ns = "_cluster"
	}
	return filepath.Join(gvkDir(root, gvk), ns, key.Name+".json")
}

func (s *FileStore) gvkForObject(obj client.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK found for object type %T", obj)
	}
	return gvks[0], nil
}

func (s *FileStore) gvkForList(list client.ObjectList) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(list)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK found for list type %T", list)
	}

	gvk := gvks[0]
	if len(gvk.Kind) > 4 && gvk.Kind[len(gvk.Kind)-4:] == "List" {
		gvk.Kind = gvk.Kind[:len(gvk.Kind)-4]
	}

	return gvk, nil
}

func (s *FileStore) populateListItems(list client.ObjectList, items []client.Object) error {
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

func (s *FileStore) generateUID() string {
	id := s.uidCounter.Add(1)
	return fmt.Sprintf("uid-%d", id)
}

func (s *FileStore) notifyWatchers(gvk schema.GroupVersionKind, event reconkit.Event) {
	watchers := s.watchers[gvk]
	active := make([]*fileWatcher, 0, len(watchers))
	for _, w := range watchers {
		if w.isStopped() {
			continue
		}
		active = append(active, w)
		w.send(event)
	}
	s.watchers[gvk] = active
}

func (s *FileStore) removeWatcher(gvk schema.GroupVersionKind, watcher *fileWatcher) {
	s.mu.Lock()
	defer s.mu.Unlock()

	watchers := s.watchers[gvk]
	filtered := make([]*fileWatcher, 0, len(watchers))
	for _, w := range watchers {
		if w != watcher {
			filtered = append(filtered, w)
		}
	}
	s.watchers[gvk] = filtered
}

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
