package reconkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// storeCache implements cache.Cache backed by a Store.
type storeCache struct {
	store  Store
	scheme *runtime.Scheme

	mu        sync.RWMutex
	informers map[schema.GroupVersionKind]*storeInformer
	started   bool
	synced    map[schema.GroupVersionKind]bool
}

// NewCache creates a new cache.Cache backed by the given Store.
func NewCache(store Store, scheme *runtime.Scheme) cache.Cache {
	return &storeCache{
		store:     store,
		scheme:    scheme,
		informers: make(map[schema.GroupVersionKind]*storeInformer),
		synced:    make(map[schema.GroupVersionKind]bool),
	}
}

// Get retrieves an object from the cache.
func (c *storeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return err
	}

	c.mu.RLock()
	informer, exists := c.informers[gvk]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no informer registered for %v", gvk)
	}

	return informer.get(key, obj)
}

// List retrieves a list of objects from the cache.
func (c *storeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk, err := apiutil.GVKForObject(list, c.scheme)
	if err != nil {
		return err
	}
	// Convert list GVK to item GVK (e.g., PodList -> Pod)
	gvk.Kind = gvk.Kind[:len(gvk.Kind)-4] // Remove "List" suffix

	c.mu.RLock()
	informer, exists := c.informers[gvk]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no informer registered for %v", gvk)
	}

	return informer.list(list, opts...)
}

// GetInformer returns an informer for the given object type.
func (c *storeCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return nil, err
	}

	return c.getOrCreateInformer(gvk, obj)
}

// GetInformerForKind returns an informer for the given GVK.
func (c *storeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption) (cache.Informer, error) {
	obj, err := c.scheme.New(gvk)
	if err != nil {
		return nil, err
	}
	clientObj, ok := obj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("object does not implement client.Object")
	}

	return c.getOrCreateInformer(gvk, clientObj)
}

func (c *storeCache) getOrCreateInformer(gvk schema.GroupVersionKind, obj client.Object) (*storeInformer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if informer, exists := c.informers[gvk]; exists {
		return informer, nil
	}

	informer := newStoreInformer(gvk, obj, c.scheme)
	c.informers[gvk] = informer

	// If cache is already started, start this informer immediately
	if c.started {
		ctx := context.Background() // Use background context for goroutine
		if err := c.startInformer(ctx, informer); err != nil {
			delete(c.informers, gvk)
			return nil, err
		}
	}

	return informer, nil
}

// RemoveInformer removes an informer from the cache.
func (c *storeCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if informer, exists := c.informers[gvk]; exists {
		informer.stop()
		delete(c.informers, gvk)
		delete(c.synced, gvk)
	}

	return nil
}

// Start starts the cache by listing and watching all registered types.
func (c *storeCache) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true

	// Get snapshot of informers to start
	informersToStart := make([]*storeInformer, 0, len(c.informers))
	for _, informer := range c.informers {
		informersToStart = append(informersToStart, informer)
	}
	c.mu.Unlock()

	// Start all informers
	for _, informer := range informersToStart {
		if err := c.startInformer(ctx, informer); err != nil {
			return err
		}
	}

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

func (c *storeCache) startInformer(ctx context.Context, informer *storeInformer) error {
	// Create a list object for this type
	listGVK := informer.gvk
	listGVK.Kind = listGVK.Kind + "List"

	listObj, err := c.scheme.New(listGVK)
	if err != nil {
		return err
	}
	list, ok := listObj.(client.ObjectList)
	if !ok {
		return fmt.Errorf("object does not implement client.ObjectList")
	}

	// Initial list to populate cache
	if err := c.store.List(ctx, list); err != nil {
		return err
	}

	// Extract items and populate informer
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}

	for _, item := range items {
		obj, ok := item.(client.Object)
		if !ok {
			continue
		}
		informer.add(obj)
	}

	// Mark as synced
	c.mu.Lock()
	c.synced[informer.gvk] = true
	c.mu.Unlock()

	informer.setSynced(true)

	// Start watch from the list's resource version to avoid
	// missing events between list and watch.
	var listRV int64
	if getter, ok := list.(interface{ GetResourceVersion() string }); ok {
		listRV, _ = strconv.ParseInt(getter.GetResourceVersion(), 10, 64)
	}

	go c.watchInformer(ctx, informer, list, listRV)

	return nil
}

func (c *storeCache) watchInformer(ctx context.Context, informer *storeInformer, list client.ObjectList, initialRV int64) {
	lastRV := initialRV

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		watcher, err := c.store.Watch(ctx, list, WatchFromRevision(lastRV))
		if err != nil {
			var rvErr *RevisionTooOldError
			if errors.As(err, &rvErr) {
				if err := c.relistInformer(ctx, informer, list); err != nil {
					return
				}
				if getter, ok := list.(interface{ GetResourceVersion() string }); ok {
					lastRV, _ = strconv.ParseInt(getter.GetResourceVersion(), 10, 64)
				} else {
					lastRV = 0
				}
				continue
			}
			return
		}

		informer.setWatcher(watcher)

	eventLoop:
		for {
			select {
			case <-ctx.Done():
				watcher.Stop()
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					break eventLoop
				}

				if event.Object != nil {
					if rv, err := strconv.ParseInt(event.Object.GetResourceVersion(), 10, 64); err == nil && rv > lastRV {
						lastRV = rv
					}
				}

				switch event.Type {
				case EventAdded:
					informer.add(event.Object)
				case EventModified:
					informer.update(event.Object)
				case EventDeleted:
					informer.delete(event.Object)
				case EventBookmark:
				}
			}
		}

		watcher.Stop()

		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *storeCache) relistInformer(ctx context.Context, informer *storeInformer, list client.ObjectList) error {
	if err := c.store.List(ctx, list); err != nil {
		return err
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	informer.replaceAll(items)
	return nil
}

// WaitForCacheSync waits for the cache to be synced.
func (c *storeCache) WaitForCacheSync(ctx context.Context) bool {
	c.mu.RLock()
	gvks := make([]schema.GroupVersionKind, 0, len(c.informers))
	for gvk := range c.informers {
		gvks = append(gvks, gvk)
	}
	c.mu.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			allSynced := true
			c.mu.RLock()
			for _, gvk := range gvks {
				if !c.synced[gvk] {
					allSynced = false
					break
				}
			}
			c.mu.RUnlock()

			if allSynced {
				return true
			}
		}
	}
}

// IndexField adds an index for the given field to the cache.
func (c *storeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return err
	}

	c.mu.RLock()
	informer, exists := c.informers[gvk]
	c.mu.RUnlock()

	if !exists {
		// Create informer if it doesn't exist
		informer, err = c.getOrCreateInformer(gvk, obj)
		if err != nil {
			return err
		}
	}

	return informer.addIndexer(field, extractValue)
}

// storeInformer implements cache.Informer for a specific GVK.
type storeInformer struct {
	gvk    schema.GroupVersionKind
	scheme *runtime.Scheme

	mu       sync.RWMutex
	objects  map[client.ObjectKey]client.Object
	indexers map[string]client.IndexerFunc
	indices  map[string]map[string][]client.ObjectKey // index -> value -> keys

	handlerMu sync.RWMutex
	handlers  []handlerRegistration
	nextID    int

	synced  bool
	stopped bool
	watcher Watcher
}

type handlerRegistration struct {
	id      int
	handler toolscache.ResourceEventHandler
}

func newStoreInformer(gvk schema.GroupVersionKind, obj client.Object, scheme *runtime.Scheme) *storeInformer {
	return &storeInformer{
		gvk:      gvk,
		scheme:   scheme,
		objects:  make(map[client.ObjectKey]client.Object),
		indexers: make(map[string]client.IndexerFunc),
		indices:  make(map[string]map[string][]client.ObjectKey),
		handlers: make([]handlerRegistration, 0),
	}
}

func (i *storeInformer) get(key client.ObjectKey, obj client.Object) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	cached, exists := i.objects[key]
	if !exists {
		return fmt.Errorf("object not found")
	}

	data, err := json.Marshal(cached)
	if err != nil {
		return fmt.Errorf("failed to marshal cached object: %w", err)
	}
	if err := json.Unmarshal(data, obj); err != nil {
		return fmt.Errorf("failed to unmarshal into target: %w", err)
	}
	return nil
}

func (i *storeInformer) list(list client.ObjectList, opts ...client.ListOption) error {
	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	var items []client.Object

	// If field selector is specified with an indexed field, use index
	if listOpts.FieldSelector != nil {
		items = i.listFromIndex(listOpts)
	} else {
		// Otherwise, scan all objects
		items = make([]client.Object, 0, len(i.objects))
		for _, obj := range i.objects {
			items = append(items, obj)
		}
	}

	// Apply label selector
	if listOpts.LabelSelector != nil {
		filtered := make([]client.Object, 0, len(items))
		for _, obj := range items {
			if listOpts.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
				filtered = append(filtered, obj)
			}
		}
		items = filtered
	}

	// Apply namespace filter
	if listOpts.Namespace != "" {
		filtered := make([]client.Object, 0, len(items))
		for _, obj := range items {
			if obj.GetNamespace() == listOpts.Namespace {
				filtered = append(filtered, obj)
			}
		}
		items = filtered
	}

	// Convert to runtime.Object slice for meta.SetList
	runtimeItems := make([]runtime.Object, len(items))
	for idx, item := range items {
		runtimeItems[idx] = item.DeepCopyObject()
	}

	return meta.SetList(list, runtimeItems)
}

func (i *storeInformer) listFromIndex(opts *client.ListOptions) []client.Object {
	// Try to extract field selector requirements
	requirements := opts.FieldSelector.Requirements()
	for _, req := range requirements {
		if req.Operator != selection.Equals {
			continue
		}

		indexName := req.Field
		indexValue := req.Value

		if index, exists := i.indices[indexName]; exists {
			if keys, exists := index[indexValue]; exists {
				items := make([]client.Object, 0, len(keys))
				for _, key := range keys {
					if obj, exists := i.objects[key]; exists {
						items = append(items, obj)
					}
				}
				return items
			}
			return nil // Index exists but no matching values
		}
	}

	// Fall back to full scan if no index matches
	items := make([]client.Object, 0, len(i.objects))
	for _, obj := range i.objects {
		items = append(items, obj)
	}
	return items
}

func (i *storeInformer) add(obj client.Object) {
	key := client.ObjectKeyFromObject(obj)

	i.mu.Lock()
	i.objects[key] = obj.DeepCopyObject().(client.Object)
	i.rebuildIndicesForObject(key, obj)
	i.mu.Unlock()

	i.handlerMu.RLock()
	handlers := make([]toolscache.ResourceEventHandler, len(i.handlers))
	for idx, h := range i.handlers {
		handlers[idx] = h.handler
	}
	i.handlerMu.RUnlock()

	for _, handler := range handlers {
		handler.OnAdd(obj, false)
	}
}

func (i *storeInformer) update(obj client.Object) {
	key := client.ObjectKeyFromObject(obj)

	i.mu.Lock()
	oldObj := i.objects[key]
	i.objects[key] = obj.DeepCopyObject().(client.Object)
	i.rebuildIndicesForObject(key, obj)
	i.mu.Unlock()

	i.handlerMu.RLock()
	handlers := make([]toolscache.ResourceEventHandler, len(i.handlers))
	for idx, h := range i.handlers {
		handlers[idx] = h.handler
	}
	i.handlerMu.RUnlock()

	for _, handler := range handlers {
		handler.OnUpdate(oldObj, obj)
	}
}

func (i *storeInformer) delete(obj client.Object) {
	key := client.ObjectKeyFromObject(obj)

	i.mu.Lock()
	cached, exists := i.objects[key]
	if exists {
		delete(i.objects, key)
		i.removeFromIndices(key)
	}
	i.mu.Unlock()

	if exists {
		i.handlerMu.RLock()
		handlers := make([]toolscache.ResourceEventHandler, len(i.handlers))
		for idx, h := range i.handlers {
			handlers[idx] = h.handler
		}
		i.handlerMu.RUnlock()

		for _, handler := range handlers {
			handler.OnDelete(cached)
		}
	}
}

func (i *storeInformer) addIndexer(field string, extractValue client.IndexerFunc) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.indexers[field] = extractValue
	i.indices[field] = make(map[string][]client.ObjectKey)

	// Rebuild index for all existing objects
	for key, obj := range i.objects {
		values := extractValue(obj)
		for _, value := range values {
			i.indices[field][value] = append(i.indices[field][value], key)
		}
	}

	return nil
}

func (i *storeInformer) rebuildIndicesForObject(key client.ObjectKey, obj client.Object) {
	// Remove from all indices first
	i.removeFromIndices(key)

	// Add to indices
	for indexName, extractValue := range i.indexers {
		if i.indices[indexName] == nil {
			i.indices[indexName] = make(map[string][]client.ObjectKey)
		}

		values := extractValue(obj)
		for _, value := range values {
			i.indices[indexName][value] = append(i.indices[indexName][value], key)
		}
	}
}

func (i *storeInformer) removeFromIndices(key client.ObjectKey) {
	for indexName, index := range i.indices {
		for value, keys := range index {
			newKeys := make([]client.ObjectKey, 0, len(keys))
			for _, k := range keys {
				if k != key {
					newKeys = append(newKeys, k)
				}
			}
			if len(newKeys) > 0 {
				i.indices[indexName][value] = newKeys
			} else {
				delete(i.indices[indexName], value)
			}
		}
	}
}

func (i *storeInformer) replaceAll(items []runtime.Object) {
	i.mu.Lock()
	oldObjects := i.objects
	i.objects = make(map[client.ObjectKey]client.Object, len(items))
	for _, item := range items {
		obj, ok := item.(client.Object)
		if !ok {
			continue
		}
		key := client.ObjectKeyFromObject(obj)
		i.objects[key] = obj.DeepCopyObject().(client.Object)
	}
	for indexName := range i.indices {
		i.indices[indexName] = make(map[string][]client.ObjectKey)
	}
	for key, obj := range i.objects {
		i.rebuildIndicesForObject(key, obj)
	}
	newObjects := i.objects
	i.mu.Unlock()

	i.handlerMu.RLock()
	handlers := make([]toolscache.ResourceEventHandler, len(i.handlers))
	for idx, h := range i.handlers {
		handlers[idx] = h.handler
	}
	i.handlerMu.RUnlock()

	for key, obj := range oldObjects {
		if _, exists := newObjects[key]; !exists {
			for _, h := range handlers {
				h.OnDelete(obj)
			}
		}
	}

	for _, obj := range newObjects {
		for _, h := range handlers {
			h.OnAdd(obj, true)
		}
	}
}

func (i *storeInformer) setSynced(synced bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.synced = synced
}

func (i *storeInformer) setWatcher(watcher Watcher) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.watcher = watcher
}

func (i *storeInformer) stop() {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.stopped = true
	if i.watcher != nil {
		i.watcher.Stop()
	}
}

// AddEventHandler adds an event handler to the informer.
func (i *storeInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.handlerMu.Lock()
	defer i.handlerMu.Unlock()

	id := i.nextID
	i.nextID++

	reg := handlerRegistration{
		id:      id,
		handler: handler,
	}
	i.handlers = append(i.handlers, reg)

	return &resourceEventHandlerRegistration{
		informer: i,
		id:       id,
	}, nil
}

func (i *storeInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) (toolscache.ResourceEventHandlerRegistration, error) {
	return i.AddEventHandler(handler)
}

func (i *storeInformer) AddEventHandlerWithOptions(handler toolscache.ResourceEventHandler, options toolscache.HandlerOptions) (toolscache.ResourceEventHandlerRegistration, error) {
	return i.AddEventHandler(handler)
}

// RemoveEventHandler removes an event handler from the informer.
func (i *storeInformer) RemoveEventHandler(registration toolscache.ResourceEventHandlerRegistration) error {
	reg, ok := registration.(*resourceEventHandlerRegistration)
	if !ok {
		return fmt.Errorf("invalid registration type")
	}

	i.handlerMu.Lock()
	defer i.handlerMu.Unlock()

	for idx, h := range i.handlers {
		if h.id == reg.id {
			i.handlers = append(i.handlers[:idx], i.handlers[idx+1:]...)
			return nil
		}
	}

	return fmt.Errorf("handler not found")
}

// AddIndexers adds indexers to the informer.
func (i *storeInformer) AddIndexers(indexers toolscache.Indexers) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	for name, indexFunc := range indexers {
		// Convert toolscache.IndexFunc to client.IndexerFunc
		extractValue := func(obj client.Object) []string {
			vals, err := indexFunc(obj)
			if err != nil {
				return nil
			}
			return vals
		}

		i.indexers[name] = extractValue
		i.indices[name] = make(map[string][]client.ObjectKey)

		// Rebuild index for all existing objects
		for key, obj := range i.objects {
			values := extractValue(obj)
			for _, value := range values {
				i.indices[name][value] = append(i.indices[name][value], key)
			}
		}
	}

	return nil
}

// HasSynced returns true if the informer has synced.
func (i *storeInformer) HasSynced() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.synced
}

// IsStopped returns true if the informer has been stopped.
func (i *storeInformer) IsStopped() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.stopped
}

func (i *storeInformer) HasSyncedChecker() toolscache.DoneChecker {
	return &informerDoneChecker{informer: i}
}

type informerDoneChecker struct {
	informer *storeInformer
}

func (d *informerDoneChecker) Name() string {
	return d.informer.gvk.String()
}

func (d *informerDoneChecker) Done() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for {
			if d.informer.HasSynced() {
				close(ch)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return ch
}

type resourceEventHandlerRegistration struct {
	informer *storeInformer
	id       int
}

func (r *resourceEventHandlerRegistration) HasSynced() bool {
	return r.informer.HasSynced()
}

func (r *resourceEventHandlerRegistration) HasSyncedChecker() toolscache.DoneChecker {
	return r.informer.HasSyncedChecker()
}

var _ cache.Cache = &storeCache{}
var _ cache.Informer = &storeInformer{}
var _ toolscache.ResourceEventHandlerRegistration = &resourceEventHandlerRegistration{}
