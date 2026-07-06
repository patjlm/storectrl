package storectrl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// CacheOption configures the cache. Use With* functions to create options.
type CacheOption func(*cacheOptions)

type cacheOptions struct {
	defaultTransform             toolscache.TransformFunc
	defaultUnsafeDisableDeepCopy bool
	defaultLabelSelector         labels.Selector
	defaultFieldSelector         fields.Selector
	defaultEnableWatchBookmarks  *bool
	defaultNamespaces            map[string]struct{}
	byObject                     []byObjectEntry
	readerFailOnMissingInformer  bool
	syncPeriod                   *time.Duration
}

type byObjectEntry struct {
	obj    client.Object
	config ByObjectConfig
}

// ByObjectConfig configures the cache for a specific object type,
// overriding any default settings.
type ByObjectConfig struct {
	Transform             toolscache.TransformFunc
	UnsafeDisableDeepCopy *bool
	Label                 labels.Selector
	Field                 fields.Selector
	EnableWatchBookmarks  *bool
}

// informerConfig holds the resolved per-GVK configuration for an informer.
type informerConfig struct {
	transform             toolscache.TransformFunc
	unsafeDisableDeepCopy bool
	labelSelector         labels.Selector
	fieldSelector         fields.Selector
	enableWatchBookmarks  *bool
	namespaces            map[string]struct{}
	syncPeriod            *time.Duration
}

// WithDefaultTransform sets the default transform function applied to objects
// before they are cached. Per-object transforms override this.
func WithDefaultTransform(fn toolscache.TransformFunc) CacheOption {
	return func(o *cacheOptions) {
		o.defaultTransform = fn
	}
}

// WithDefaultUnsafeDisableDeepCopy disables deep copying of cached objects
// on read when set to true. This improves performance but callers must not
// mutate returned objects. Per-object settings override this.
func WithDefaultUnsafeDisableDeepCopy(disable bool) CacheOption {
	return func(o *cacheOptions) {
		o.defaultUnsafeDisableDeepCopy = disable
	}
}

// WithDefaultLabelSelector sets a default label selector that filters which
// objects are cached. Per-object selectors override this.
func WithDefaultLabelSelector(sel labels.Selector) CacheOption {
	return func(o *cacheOptions) {
		o.defaultLabelSelector = sel
	}
}

// WithDefaultFieldSelector sets a default field selector that filters which
// objects are cached. Per-object selectors override this.
func WithDefaultFieldSelector(sel fields.Selector) CacheOption {
	return func(o *cacheOptions) {
		o.defaultFieldSelector = sel
	}
}

// WithByObject configures the cache for a specific object type, overriding
// any default settings for transform, deep copy, and selectors.
func WithByObject(obj client.Object, config ByObjectConfig) CacheOption {
	return func(o *cacheOptions) {
		o.byObject = append(o.byObject, byObjectEntry{obj: obj, config: config})
	}
}

// WithReaderFailOnMissingInformer controls whether Get/List return an error
// when no informer exists for the requested type. When true (the default),
// callers must register informers explicitly via GetInformer. When false,
// informers are auto-created on demand, matching controller-runtime behavior.
func WithReaderFailOnMissingInformer(fail bool) CacheOption {
	return func(o *cacheOptions) {
		o.readerFailOnMissingInformer = fail
	}
}

// WithSyncPeriod sets the interval at which the cache re-delivers all cached
// objects to event handlers as update events. This matches controller-runtime's
// resync behavior. A zero or negative duration disables periodic resync.
func WithSyncPeriod(d time.Duration) CacheOption {
	return func(o *cacheOptions) {
		o.syncPeriod = &d
	}
}

// WithDefaultEnableWatchBookmarks requests backends to send periodic BOOKMARK
// events during Watch. Backends that support bookmarks honor this; others may
// ignore it. Defaults to true when not set, matching controller-runtime behavior.
// Per-object settings in ByObjectConfig override this.
func WithDefaultEnableWatchBookmarks(enable bool) CacheOption {
	return func(o *cacheOptions) {
		o.defaultEnableWatchBookmarks = &enable
	}
}

// WithDefaultNamespaces restricts the cache to objects from the specified
// namespaces. This is a simplified version of controller-runtime's
// DefaultNamespaces map[string]cache.Config — only namespace filtering is
// supported, not per-namespace configuration (selectors, transforms).
// Use the AllNamespaces constant (empty string) to include all namespaces
// not explicitly listed. Use WithByObject for per-type configuration.
func WithDefaultNamespaces(namespaces []string) CacheOption {
	return func(o *cacheOptions) {
		o.defaultNamespaces = make(map[string]struct{}, len(namespaces))
		for _, ns := range namespaces {
			o.defaultNamespaces[ns] = struct{}{}
		}
	}
}

// AllNamespaces is the key used in WithDefaultNamespaces to indicate all
// namespaces not explicitly listed should be cached.
const AllNamespaces = ""

// TransformStripManagedFields returns a transform that strips managed fields
// from objects before caching, reducing memory usage.
func TransformStripManagedFields() toolscache.TransformFunc {
	return func(in any) (any, error) {
		if obj, ok := in.(client.Object); ok && obj.GetManagedFields() != nil {
			obj.SetManagedFields(nil)
		}
		return in, nil
	}
}

// cacheEventType represents the type of cache event for async handler delivery.
type cacheEventType int

const (
	cacheEventAdd cacheEventType = iota
	cacheEventUpdate
	cacheEventDelete
)

// cacheEvent represents a pending event in the async processing queue.
type cacheEvent struct {
	eventType       cacheEventType
	obj             client.Object
	oldObj          client.Object // only for cacheEventUpdate
	isInInitialList bool
}

// eventQueue provides per-key coalescing of events for async handler delivery.
// Multiple updates to the same object between processor cycles are merged into
// a single event, reducing handler call overhead under high event rates.
type eventQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	order   []client.ObjectKey
	pending map[client.ObjectKey][]*cacheEvent
	closed  bool
}

func newEventQueue() *eventQueue {
	q := &eventQueue{
		pending: make(map[client.ObjectKey][]*cacheEvent),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *eventQueue) enqueue(key client.ObjectKey, evt *cacheEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	existing, exists := q.pending[key]
	if !exists {
		q.pending[key] = []*cacheEvent{evt}
		q.order = append(q.order, key)
	} else {
		last := existing[len(existing)-1]
		if last.eventType == cacheEventDelete && evt.eventType == cacheEventAdd {
			q.pending[key] = append(existing, evt)
		} else {
			coalesced := coalesceEvents(last, evt)
			if coalesced == nil {
				existing = existing[:len(existing)-1]
				if len(existing) == 0 {
					delete(q.pending, key)
				} else {
					q.pending[key] = existing
				}
			} else {
				existing[len(existing)-1] = coalesced
			}
		}
	}
	q.cond.Signal()
}

func coalesceEvents(existing, incoming *cacheEvent) *cacheEvent {
	switch {
	case existing.eventType == cacheEventAdd && incoming.eventType == cacheEventUpdate:
		return &cacheEvent{eventType: cacheEventAdd, obj: incoming.obj, isInInitialList: existing.isInInitialList}
	case existing.eventType == cacheEventAdd && incoming.eventType == cacheEventDelete:
		return nil
	case existing.eventType == cacheEventUpdate && incoming.eventType == cacheEventUpdate:
		return &cacheEvent{eventType: cacheEventUpdate, oldObj: existing.oldObj, obj: incoming.obj}
	case existing.eventType == cacheEventUpdate && incoming.eventType == cacheEventDelete:
		return &cacheEvent{eventType: cacheEventDelete, obj: incoming.obj}
	default:
		return incoming
	}
}

// drain blocks until events are available (or the queue is closed), then
// returns all pending events and resets the pending map. Returns nil when
// the queue is closed and empty.
type drainResult struct {
	order  []client.ObjectKey
	events map[client.ObjectKey][]*cacheEvent
}

func (q *eventQueue) drain() *drainResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.pending) == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.closed && len(q.pending) == 0 {
		return nil
	}
	result := &drainResult{
		order:  q.order,
		events: q.pending,
	}
	q.order = nil
	q.pending = make(map[client.ObjectKey][]*cacheEvent)
	return result
}

func (q *eventQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// storeCache implements cache.Cache backed by a Store.
type storeCache struct {
	store  Store
	scheme *runtime.Scheme

	mu        sync.RWMutex
	informers map[schema.GroupVersionKind]*storeInformer
	started   bool
	startCtx  context.Context
	synced    map[schema.GroupVersionKind]bool

	opts            cacheOptions
	informerConfigs map[schema.GroupVersionKind]informerConfig
}

// NewCache creates a new cache.Cache backed by the given Store.
func NewCache(store Store, scheme *runtime.Scheme, opts ...CacheOption) cache.Cache {
	options := cacheOptions{
		readerFailOnMissingInformer: true,
	}
	for _, opt := range opts {
		opt(&options)
	}

	configs := make(map[schema.GroupVersionKind]informerConfig)
	for _, entry := range options.byObject {
		gvk, err := apiutil.GVKForObject(entry.obj, scheme)
		if err != nil {
			continue
		}
		cfg := informerConfig{
			transform:     entry.config.Transform,
			labelSelector: entry.config.Label,
			fieldSelector: entry.config.Field,
			namespaces:    options.defaultNamespaces,
			syncPeriod:    options.syncPeriod,
		}
		if entry.config.UnsafeDisableDeepCopy != nil {
			cfg.unsafeDisableDeepCopy = *entry.config.UnsafeDisableDeepCopy
		} else {
			cfg.unsafeDisableDeepCopy = options.defaultUnsafeDisableDeepCopy
		}
		if entry.config.EnableWatchBookmarks != nil {
			cfg.enableWatchBookmarks = entry.config.EnableWatchBookmarks
		} else {
			cfg.enableWatchBookmarks = options.defaultEnableWatchBookmarks
		}
		if cfg.transform == nil {
			cfg.transform = options.defaultTransform
		}
		if cfg.labelSelector == nil {
			cfg.labelSelector = options.defaultLabelSelector
		}
		if cfg.fieldSelector == nil {
			cfg.fieldSelector = options.defaultFieldSelector
		}
		configs[gvk] = cfg
	}

	return &storeCache{
		store:           store,
		scheme:          scheme,
		informers:       make(map[schema.GroupVersionKind]*storeInformer),
		synced:          make(map[schema.GroupVersionKind]bool),
		opts:            options,
		informerConfigs: configs,
	}
}

func (c *storeCache) resolveConfig(gvk schema.GroupVersionKind) informerConfig {
	if cfg, ok := c.informerConfigs[gvk]; ok {
		return cfg
	}
	return informerConfig{
		transform:             c.opts.defaultTransform,
		unsafeDisableDeepCopy: c.opts.defaultUnsafeDisableDeepCopy,
		labelSelector:         c.opts.defaultLabelSelector,
		fieldSelector:         c.opts.defaultFieldSelector,
		enableWatchBookmarks:  c.opts.defaultEnableWatchBookmarks,
		namespaces:            c.opts.defaultNamespaces,
		syncPeriod:            c.opts.syncPeriod,
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
		if c.opts.readerFailOnMissingInformer {
			return fmt.Errorf("no informer registered for %v; set up an informer via GetInformer or disable ReaderFailOnMissingInformer", gvk)
		}
		informer, err = c.getOrCreateInformer(gvk, obj)
		if err != nil {
			return err
		}
	}

	return informer.get(key, obj)
}

// List retrieves a list of objects from the cache.
func (c *storeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk, err := apiutil.GVKForObject(list, c.scheme)
	if err != nil {
		return err
	}
	if len(gvk.Kind) <= 4 || gvk.Kind[len(gvk.Kind)-4:] != "List" {
		return fmt.Errorf("expected list kind ending in 'List', got %q", gvk.Kind)
	}
	gvk.Kind = gvk.Kind[:len(gvk.Kind)-4]

	c.mu.RLock()
	informer, exists := c.informers[gvk]
	c.mu.RUnlock()

	if !exists {
		if c.opts.readerFailOnMissingInformer {
			return fmt.Errorf("no informer registered for %v; set up an informer via GetInformer or disable ReaderFailOnMissingInformer", gvk)
		}
		runtimeObj, schemeErr := c.scheme.New(gvk)
		if schemeErr != nil {
			return schemeErr
		}
		clientObj, ok := runtimeObj.(client.Object)
		if !ok {
			return fmt.Errorf("object for %v does not implement client.Object", gvk)
		}
		informer, err = c.getOrCreateInformer(gvk, clientObj)
		if err != nil {
			return err
		}
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

	if informer, exists := c.informers[gvk]; exists {
		c.mu.Unlock()
		return informer, nil
	}

	config := c.resolveConfig(gvk)
	informer := newStoreInformer(gvk, obj, c.scheme, config)
	c.informers[gvk] = informer

	needStart := c.started
	startCtx := c.startCtx
	c.mu.Unlock()

	if needStart {
		select {
		case <-startCtx.Done():
			c.mu.Lock()
			delete(c.informers, gvk)
			c.mu.Unlock()
			return nil, fmt.Errorf("cache is stopped")
		default:
		}
		if err := c.startInformer(startCtx, informer); err != nil {
			c.mu.Lock()
			delete(c.informers, gvk)
			c.mu.Unlock()
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
	c.startCtx = ctx

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
	informer.startProcessing(ctx)

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

	// Build list options from informer config selectors
	var listOpts []client.ListOption
	if informer.config.labelSelector != nil {
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: informer.config.labelSelector})
	}
	if informer.config.fieldSelector != nil {
		listOpts = append(listOpts, client.MatchingFieldsSelector{Selector: informer.config.fieldSelector})
	}

	// Initial list to populate cache
	if err := c.store.List(ctx, list, listOpts...); err != nil {
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
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	// Build watch options from informer config selectors
	var watchOpts []client.ListOption
	if informer.config.labelSelector != nil {
		watchOpts = append(watchOpts, client.MatchingLabelsSelector{Selector: informer.config.labelSelector})
	}
	if informer.config.fieldSelector != nil {
		watchOpts = append(watchOpts, client.MatchingFieldsSelector{Selector: informer.config.fieldSelector})
	}
	if informer.config.enableWatchBookmarks != nil {
		watchOpts = append(watchOpts, EnableWatchBookmarks(*informer.config.enableWatchBookmarks))
	}

	// Setup sync period ticker
	var syncCh <-chan time.Time
	if informer.config.syncPeriod != nil && *informer.config.syncPeriod > 0 {
		ticker := time.NewTicker(*informer.config.syncPeriod)
		defer ticker.Stop()
		syncCh = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		opts := append([]client.ListOption{WatchFromRevision(lastRV)}, watchOpts...)
		watcher, err := c.store.Watch(ctx, list, opts...)
		if err != nil {
			var rvErr *RevisionTooOldError
			if errors.As(err, &rvErr) {
				if err := c.relistInformer(ctx, informer, list); err != nil {
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
					backoff = min(backoff*2, maxBackoff)
					continue
				}
				if getter, ok := list.(interface{ GetResourceVersion() string }); ok {
					lastRV, _ = strconv.ParseInt(getter.GetResourceVersion(), 10, 64)
				} else {
					lastRV = 0
				}
				backoff = time.Second
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = time.Second
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
			case <-syncCh:
				c.resyncInformer(informer)
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

func (c *storeCache) resyncInformer(informer *storeInformer) {
	informer.mu.RLock()
	snapshot := make(map[client.ObjectKey]client.Object, len(informer.objects))
	for key, obj := range informer.objects {
		snapshot[key] = obj.DeepCopyObject().(client.Object)
	}
	informer.mu.RUnlock()

	for key, objCopy := range snapshot {
		informer.queue.enqueue(key, &cacheEvent{
			eventType: cacheEventUpdate,
			oldObj:    objCopy,
			obj:       objCopy,
		})
	}
}

func (c *storeCache) relistInformer(ctx context.Context, informer *storeInformer, list client.ObjectList) error {
	var listOpts []client.ListOption
	if informer.config.labelSelector != nil {
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: informer.config.labelSelector})
	}
	if informer.config.fieldSelector != nil {
		listOpts = append(listOpts, client.MatchingFieldsSelector{Selector: informer.config.fieldSelector})
	}
	if err := c.store.List(ctx, list, listOpts...); err != nil {
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
		}

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
		time.Sleep(10 * time.Millisecond)
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

	synced   bool
	syncedCh chan struct{}
	stopped  bool
	watcher  Watcher

	queue  *eventQueue
	config informerConfig
}

type handlerRegistration struct {
	id      int
	handler toolscache.ResourceEventHandler
}

func newStoreInformer(gvk schema.GroupVersionKind, obj client.Object, scheme *runtime.Scheme, config informerConfig) *storeInformer {
	return &storeInformer{
		gvk:      gvk,
		scheme:   scheme,
		objects:  make(map[client.ObjectKey]client.Object),
		indexers: make(map[string]client.IndexerFunc),
		indices:  make(map[string]map[string][]client.ObjectKey),
		handlers: make([]handlerRegistration, 0),
		syncedCh: make(chan struct{}),
		queue:    newEventQueue(),
		config:   config,
	}
}

func (i *storeInformer) applyTransform(obj client.Object) (client.Object, error) {
	if i.config.transform == nil {
		return obj, nil
	}
	result, err := i.config.transform(obj)
	if err != nil {
		return nil, err
	}
	transformed, ok := result.(client.Object)
	if !ok {
		return nil, fmt.Errorf("transform returned %T, expected client.Object", result)
	}
	return transformed, nil
}

func objectToFieldsSet(obj client.Object) fields.Set {
	return fields.Set{
		"metadata.name":      obj.GetName(),
		"metadata.namespace": obj.GetNamespace(),
	}
}

func (i *storeInformer) matchesSelectors(obj client.Object) bool {
	if i.config.namespaces != nil {
		ns := obj.GetNamespace()
		_, nsAllowed := i.config.namespaces[ns]
		_, hasAllNamespaces := i.config.namespaces[AllNamespaces]
		if !nsAllowed && !hasAllNamespaces {
			return false
		}
	}
	if i.config.labelSelector != nil && !i.config.labelSelector.Matches(labels.Set(obj.GetLabels())) {
		return false
	}
	if i.config.fieldSelector != nil && !i.config.fieldSelector.Matches(objectToFieldsSet(obj)) {
		return false
	}
	return true
}

func (i *storeInformer) get(key client.ObjectKey, obj client.Object) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	cached, exists := i.objects[key]
	if !exists {
		return &NotFoundError{Key: key.String()}
	}

	if i.config.unsafeDisableDeepCopy {
		outVal := reflect.ValueOf(obj)
		objVal := reflect.ValueOf(cached)
		if !objVal.Type().AssignableTo(outVal.Type()) {
			return fmt.Errorf("cache had type %s, but %s was asked for", objVal.Type(), outVal.Type())
		}
		reflect.Indirect(outVal).Set(reflect.Indirect(objVal))
		return nil
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
		if i.config.unsafeDisableDeepCopy {
			runtimeItems[idx] = item.(runtime.Object)
		} else {
			runtimeItems[idx] = item.DeepCopyObject()
		}
	}

	return meta.SetList(list, runtimeItems)
}

func (i *storeInformer) listFromIndex(opts *client.ListOptions) []client.Object {
	requirements := opts.FieldSelector.Requirements()

	var resultKeys map[client.ObjectKey]struct{}
	initialized := false

	for _, req := range requirements {
		if req.Operator != selection.Equals {
			continue
		}

		index, exists := i.indices[req.Field]
		if !exists {
			return nil
		}

		keys, exists := index[req.Value]
		if !exists {
			return nil
		}

		keySet := make(map[client.ObjectKey]struct{}, len(keys))
		for _, key := range keys {
			keySet[key] = struct{}{}
		}

		if !initialized {
			resultKeys = keySet
			initialized = true
		} else {
			for key := range resultKeys {
				if _, ok := keySet[key]; !ok {
					delete(resultKeys, key)
				}
			}
		}
	}

	if !initialized {
		items := make([]client.Object, 0, len(i.objects))
		for _, obj := range i.objects {
			items = append(items, obj)
		}
		return items
	}

	items := make([]client.Object, 0, len(resultKeys))
	for key := range resultKeys {
		if obj, exists := i.objects[key]; exists {
			items = append(items, obj)
		}
	}
	return items
}

func (i *storeInformer) add(obj client.Object) {
	if !i.matchesSelectors(obj) {
		return
	}

	transformed, err := i.applyTransform(obj)
	if err != nil {
		return
	}

	key := client.ObjectKeyFromObject(transformed)

	i.mu.Lock()
	if existing, exists := i.objects[key]; exists && !newerResourceVersion(transformed.GetResourceVersion(), existing.GetResourceVersion()) {
		i.mu.Unlock()
		return
	}
	i.objects[key] = transformed.DeepCopyObject().(client.Object)
	i.rebuildIndicesForObject(key, transformed)
	i.mu.Unlock()

	i.queue.enqueue(key, &cacheEvent{
		eventType: cacheEventAdd,
		obj:       transformed,
	})
}

func (i *storeInformer) update(obj client.Object) {
	matches := i.matchesSelectors(obj)
	key := client.ObjectKeyFromObject(obj)

	if !matches {
		i.mu.Lock()
		cached, existed := i.objects[key]
		if existed && !newerResourceVersion(obj.GetResourceVersion(), cached.GetResourceVersion()) {
			i.mu.Unlock()
			return
		}
		if existed {
			delete(i.objects, key)
			i.removeFromIndices(key)
		}
		i.mu.Unlock()

		if existed {
			i.queue.enqueue(key, &cacheEvent{
				eventType: cacheEventDelete,
				obj:       cached,
			})
		}
		return
	}

	transformed, err := i.applyTransform(obj)
	if err != nil {
		return
	}

	i.mu.Lock()
	oldObj, exists := i.objects[key]
	if exists && !newerResourceVersion(transformed.GetResourceVersion(), oldObj.GetResourceVersion()) {
		i.mu.Unlock()
		return
	}
	i.objects[key] = transformed.DeepCopyObject().(client.Object)
	i.rebuildIndicesForObject(key, transformed)
	i.mu.Unlock()

	if !exists {
		i.queue.enqueue(key, &cacheEvent{
			eventType: cacheEventAdd,
			obj:       transformed,
		})
		return
	}
	i.queue.enqueue(key, &cacheEvent{
		eventType: cacheEventUpdate,
		oldObj:    oldObj,
		obj:       transformed,
	})
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
		i.queue.enqueue(key, &cacheEvent{
			eventType: cacheEventDelete,
			obj:       cached,
		})
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
		if !i.matchesSelectors(obj) {
			continue
		}
		transformed, err := i.applyTransform(obj)
		if err != nil {
			continue
		}
		key := client.ObjectKeyFromObject(transformed)
		i.objects[key] = transformed.DeepCopyObject().(client.Object)
	}
	for indexName := range i.indices {
		i.indices[indexName] = make(map[string][]client.ObjectKey)
	}
	for key, obj := range i.objects {
		i.rebuildIndicesForObject(key, obj)
	}
	newObjects := i.objects
	i.mu.Unlock()

	for key, obj := range oldObjects {
		if _, exists := newObjects[key]; !exists {
			i.queue.enqueue(key, &cacheEvent{
				eventType: cacheEventDelete,
				obj:       obj,
			})
		}
	}

	for key, obj := range newObjects {
		oldObj, existed := oldObjects[key]
		if !existed {
			i.queue.enqueue(key, &cacheEvent{
				eventType:       cacheEventAdd,
				obj:             obj.DeepCopyObject().(client.Object),
				isInInitialList: true,
			})
		} else if oldObj.GetResourceVersion() != obj.GetResourceVersion() {
			i.queue.enqueue(key, &cacheEvent{
				eventType: cacheEventUpdate,
				oldObj:    oldObj,
				obj:       obj.DeepCopyObject().(client.Object),
			})
		}
	}
}

func (i *storeInformer) startProcessing(ctx context.Context) {
	go func() {
		<-ctx.Done()
		i.queue.close()
	}()
	go i.processEvents()
}

func (i *storeInformer) processEvents() {
	for {
		result := i.queue.drain()
		if result == nil {
			return
		}

		i.handlerMu.RLock()
		handlers := make([]toolscache.ResourceEventHandler, len(i.handlers))
		for idx, h := range i.handlers {
			handlers[idx] = h.handler
		}
		i.handlerMu.RUnlock()

		seen := make(map[client.ObjectKey]bool, len(result.events))
		for _, key := range result.order {
			if seen[key] {
				continue
			}
			seen[key] = true
			for _, evt := range result.events[key] {
				switch evt.eventType {
				case cacheEventAdd:
					for _, h := range handlers {
						h.OnAdd(evt.obj, evt.isInInitialList)
					}
				case cacheEventUpdate:
					for _, h := range handlers {
						h.OnUpdate(evt.oldObj, evt.obj)
					}
				case cacheEventDelete:
					for _, h := range handlers {
						h.OnDelete(evt.obj)
					}
				}
			}
		}
	}
}

func (i *storeInformer) setSynced(synced bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if synced && !i.synced {
		i.synced = true
		close(i.syncedCh)
	}
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
	i.queue.close()
	if i.watcher != nil {
		i.watcher.Stop()
	}
}

// AddEventHandler adds an event handler to the informer.
func (i *storeInformer) AddEventHandler(handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	// Register + snapshot atomically: handlerMu.Lock blocks event delivery,
	// so no events can reach the handler between registration and snapshot.
	i.handlerMu.Lock()
	id := i.nextID
	i.nextID++
	reg := handlerRegistration{
		id:      id,
		handler: handler,
	}
	i.handlers = append(i.handlers, reg)

	i.mu.RLock()
	existing := make([]client.Object, 0, len(i.objects))
	for _, obj := range i.objects {
		existing = append(existing, obj.DeepCopyObject().(client.Object))
	}
	i.mu.RUnlock()
	i.handlerMu.Unlock()

	for _, obj := range existing {
		handler.OnAdd(obj, true)
	}

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
	return d.informer.syncedCh
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

func newerResourceVersion(incoming, existing string) bool {
	inRV, inErr := strconv.ParseInt(incoming, 10, 64)
	exRV, exErr := strconv.ParseInt(existing, 10, 64)
	if inErr == nil && exErr == nil {
		return inRV > exRV
	}
	return incoming != existing
}

var _ cache.Cache = &storeCache{}
var _ cache.Informer = &storeInformer{}
var _ toolscache.ResourceEventHandlerRegistration = &resourceEventHandlerRegistration{}
