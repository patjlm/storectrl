# Components & Usage

storectrl provides three pluggable components. Each wraps a `Store` backend into a standard controller-runtime interface. They're independent — use any combination.

## Components

### NewClient → `client.Client`

Wraps a Store into controller-runtime's `client.Client` interface. Handles reads (Get, List) and writes (Create, Update, Delete, Patch, Status).

**Visibility:** Invisible after wiring. Controllers use the standard `client.Client` interface — they don't know the difference.

**What it brings vs CR's default client:**
- Works without an API server
- Pluggable backend (SQL, GCP APIs, in-memory, filesystem, etc.)

**What it lacks:**
- **Patch is simulated** — Get + apply diff + Update, not real server-side patch. Works for JSON Patch and Merge Patch. Strategic merge patch falls back to regular merge. Apply patch type is unsupported.
- **DeleteAllOf is simulated** — List + Delete each, not atomic.
- **SubResources** — only `status` works (routes to Update). Custom subresources like `scale` return an error.
- **No admission webhooks** — there's no API server to serve them.
- **No API discovery** — RESTMapper is a stub.

**Honest take:** It's a simulation of `client.Client`. Works well for standard reconciler CRUD patterns. Code relying on server-side semantics (real SSA, atomic bulk deletes, strategic merge patch, webhooks) will behave differently.

**See:** [`TestWiringClientOnly`](../wiring_test.go) for a runnable example.

### NewCache → `cache.Cache`

Watch-backed in-memory cache. Performs initial sync via `Store.List`, then receives incremental updates via `Store.Watch`. All reads (Get, List) hit the in-memory cache, not the store. Provides event handlers (`AddEventHandler`), field indexers (`IndexField`), and `WaitForCacheSync`.

**Visibility:** Invisible after wiring. Event handlers, indexers, and sync work the same as CR's cache.

**What it brings vs CR's default cache:**
- Works without an API server
- Simpler internals — easier to debug than CR's Reflector → DeltaFIFO → SharedIndexInformer pipeline
- **Smart relist diff** — on relist after watch reconnection, diffs old vs new objects by ResourceVersion. Unchanged objects produce no handler call. Reduces relist cost from O(N) to O(changed).
- **Async event queue** — per-key coalescing queue decouples handler calls from the watch goroutine. Prevents slow handlers from blocking the watch channel and triggering overflow → relist cascades.

**What it lacks:**
- **Intermediate states are lost** — if an object is updated 5 times before your handler runs, you see one event with the latest state. Fine for level-triggered reconcilers (vast majority). Not suitable for controllers that need to observe every state transition.
- **No per-namespace informers** — simplified namespace filtering via `WithDefaultNamespaces` covers common cases, but you can't use different selectors or transforms per namespace.
- **No WatchErrorHandler** — watch errors are handled internally with exponential backoff. Not configurable.
- **No automatic type discovery** — no RESTMapper. Types must be registered with the scheme.

**Honest take:** Good for most reconcilers. The event coalescing is fine for level-triggered controllers. The simplified namespace handling covers common cases but not "different selectors per namespace." The smart relist diff and async event queue are genuine improvements over CR's cache for high-churn workloads.

**See:** [`TestWiringCacheOnly`](../wiring_test.go) for a runnable example.

### StoreListerWatcher → `ListerWatcher`

Thin adapter that wraps a Store into client-go's `ListerWatcher` interface. This lets you plug a Store backend into controller-runtime's full informer stack (Reflector → DeltaFIFO → SharedIndexInformer) instead of using storectrl's own cache.

**Visibility:** Configuration-time adapter — used inside CR's `NewInformer` factory override. Controllers don't interact with it directly.

**What it brings vs storectrl's NewCache:**
- **CR's full informer stack** — DeltaFIFO, SharedIndexInformer, per-namespace informers with independent config, all CR cache features automatically.
- **Automatic feature parity** — when CR adds new cache features, you get them for free.
- **DeltaFIFO preserves intermediate states** — every change (Added, Modified, Deleted) is recorded as an individual delta per object. However, CR's SharedInformer coalesces before delivering to your `ResourceEventHandler`, so by default you still see the latest state (similar to NewCache). Code that reads from DeltaFIFO directly can observe every transition.

**What it lacks:**
- **Read-only** — only covers List/Watch. For writes, combine with `NewClient`.
- **Requires dummy `rest.Config`** — CR's `cache.New` expects one. If CR adds config validation in a future version, this may break.
- **Partial ListOptions translation** — only `LabelSelector` and `ResourceVersion` are translated from `metav1.ListOptions`. Pagination, field selectors, and timeouts are silently ignored (CR's Reflector handles this gracefully).
- **Higher K8s coupling** — you depend on CR's internal informer implementation.

**Known limitation:** client-go v0.36+ uses `watchList` by default in its Reflector, which requires the backend to send bookmark events to signal end of initial events. StoreListerWatcher does not currently send bookmarks, so `SharedIndexInformer` may take 10+ seconds to sync (it falls back to traditional List+Watch after a timeout). This needs a fix in the adapter.

**Honest take:** Couples you to CR internals, and the compatibility surface is fragile — the watchList issue above is an example. The dummy `rest.Config` is a hack. Use when you genuinely need per-namespace informers or DeltaFIFO, or want automatic CR feature parity without maintaining storectrl's cache.

**See:** [`TestWiringStoreListerWatcher`](../wiring_test.go) for a runnable example.

## Compositions

All components take a `Store` — they're wired into controller-runtime's `manager.Manager` via factory overrides in `ctrl.Options`.

### Full Store-backed (recommended)

Most common setup. Both reads and writes go through the Store. No API server dependency for data operations.

```go
store := memory.NewStore(scheme)

mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
    Scheme: scheme,
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return storectrl.NewCache(store, scheme), nil
    },
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, uncachedObjects ...client.Object) (client.Client, error) {
        return storectrl.NewClient(store, scheme), nil
    },
})
```

**See:** [`TestWiringFullStoreBacked`](../wiring_test.go) for a runnable example.

### StoreListerWatcher + NewClient

CR's informer stack for the cache side, Store-backed writes. Use when you want DeltaFIFO or per-namespace informers with automatic CR feature parity.

```go
store := memory.NewStore(scheme)

mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
    Scheme: scheme,
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return cache.New(cfg, cache.Options{
            Scheme:     scheme,
            HTTPClient: http.DefaultClient,
            Mapper:     meta.NewDefaultRESTMapper([]schema.GroupVersion{}),
            NewInformer: func(lw toolscache.ListerWatcher, obj runtime.Object, d time.Duration, idx toolscache.Indexers) toolscache.SharedIndexInformer {
                storeLW := storectrl.NewStoreListerWatcher(store, &WidgetList{})
                return toolscache.NewSharedIndexInformer(storeLW, obj, d, idx)
            },
        })
    },
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, uncachedObjects ...client.Object) (client.Client, error) {
        return storectrl.NewClient(store, scheme), nil
    },
})
```

### Mixing with controller-runtime defaults

`NewCache` and `NewClient` are independent. You can override just one and let CR handle the other:

```go
// Store-backed writes, CR default cache (reads from API server)
mgr, _ := ctrl.NewManager(restConfig, ctrl.Options{
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, uncachedObjects ...client.Object) (client.Client, error) {
        return storectrl.NewClient(store, scheme), nil
    },
})

// Store-backed cache (reads from Store), CR default client (writes to API server)
mgr, _ := ctrl.NewManager(restConfig, ctrl.Options{
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return storectrl.NewCache(store, scheme), nil
    },
})
```

These hybrid setups are less common but valid — for example, a controller that reads K8s resources from the API server but writes to a custom Store, or vice versa.

### Trade-offs: NewCache vs StoreListerWatcher

| Aspect | NewCache (default) | StoreListerWatcher |
|---|---|---|
| Event coalescing | storectrl's async queue (per-key) | CR's SharedInformer (on top of DeltaFIFO) |
| Intermediate states | Lost before handler | Preserved in DeltaFIFO, coalesced by SharedInformer before handler |
| CR feature parity | Manual catch-up | Automatic |
| Multi-namespace | Simplified (namespace filter) | Full (per-NS informer) |
| K8s coupling | Low | Higher (dummy restConfig, CR internals) |
| Debugging | Simple (our code) | Harder (CR internals + adapter) |

## Cache options reference

`NewCache` accepts `CacheOption` functional options that mirror `cache.Options` from controller-runtime.

| Option | Description |
|---|---|
| `WithDefaultTransform(fn)` | Transform objects before caching. Reduces memory for large objects. |
| `WithDefaultUnsafeDisableDeepCopy(bool)` | Skip deep copy on cache reads (Get/List). Caller must DeepCopy before mutating. |
| `WithDefaultLabelSelector(sel)` | Filter objects entering the cache by label. Passed to Store.List/Watch. |
| `WithDefaultFieldSelector(sel)` | Filter by metadata fields (metadata.name, metadata.namespace). Passed to Store.List/Watch. |
| `WithDefaultEnableWatchBookmarks(bool)` | Request backends to send BOOKMARK events during Watch. Backends honor or ignore. |
| `WithDefaultNamespaces([]string)` | Restrict cache to objects from listed namespaces. Use `AllNamespaces` ("") to include unlisted namespaces. |
| `WithByObject(obj, config)` | Per-GVK overrides via `ByObjectConfig` (Transform, UnsafeDisableDeepCopy, Label, Field, EnableWatchBookmarks). |
| `WithReaderFailOnMissingInformer(bool)` | When true (default), Get/List return error for unregistered types. When false, auto-creates informers. |
| `WithSyncPeriod(d)` | Periodic resync: delivers synthetic OnUpdate events for all cached objects to event handlers. |

### TransformStripManagedFields

Strips `managedFields` from objects before caching — useful when managedFields aren't needed and you want to reduce memory:

```go
storectrl.NewCache(store, scheme,
    storectrl.WithDefaultTransform(storectrl.TransformStripManagedFields()),
)
```

### Per-GVK configuration

Use `WithByObject` to override defaults for specific types:

```go
storectrl.NewCache(store, scheme,
    storectrl.WithDefaultUnsafeDisableDeepCopy(true),
    storectrl.WithByObject(&Widget{}, storectrl.ByObjectConfig{
        UnsafeDisableDeepCopy: ptr.To(false), // override: deep copy Widgets
        Label: labels.SelectorFromSet(labels.Set{"env": "production"}),
    }),
)
```

### Namespace filtering

`WithDefaultNamespaces` restricts the cache to objects from specific namespaces:

```go
storectrl.NewCache(store, scheme,
    storectrl.WithDefaultNamespaces([]string{"prod", "staging"}),
)
```

Use `AllNamespaces` (empty string) to include all namespaces not explicitly listed:

```go
storectrl.NewCache(store, scheme,
    storectrl.WithDefaultNamespaces([]string{"prod", storectrl.AllNamespaces}),
)
```

This is a simplified version of controller-runtime's `DefaultNamespaces map[string]cache.Config`. CR creates separate informers per namespace with independent configurations. storectrl filters by namespace in the cache layer instead. Per-namespace configuration (different selectors, transforms per namespace) is not supported — use `WithByObject` for per-type configuration.

## Unsupported controller-runtime options

These `cache.Options` fields are K8s-specific and have no Store equivalent:

- **HTTPClient, Mapper** — K8s REST plumbing. storectrl replaces the informer stack entirely.
- **NewInformer** — not used by `storectrl.NewCache`, but composable via `StoreListerWatcher` (see above).
- **WatchErrorHandler** — storectrl handles watch errors internally with exponential backoff and automatic reconnection.
