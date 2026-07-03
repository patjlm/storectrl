# Migrating to storectrl

## Overview

storectrl is a component library that provides `client.Client` and `cache.Cache` implementations backed by a pluggable `Store` interface. It does not replace controller-runtime -- it plugs into it. You keep your standard `ctrl.NewManager` setup and override two factory functions.

### When to Use storectrl

Use storectrl when you want controllers that reconcile against non-Kubernetes backends (SQL, GCP APIs, in-memory, etc.) while keeping the standard controller-runtime reconciler pattern, lifecycle management, and operational features.

## Migration: Two Factory Overrides

### Before: Standard controller-runtime

```go
mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
    Scheme: scheme,
})
```

### After: storectrl-backed

```go
store := memory.NewStore(scheme) // or your custom Store implementation

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

Everything else stays the same -- controller setup, reconciler code, `mgr.Start()`.

### With cache options

`NewCache` accepts functional options that mirror controller-runtime's `cache.Options`:

```go
store := memory.NewStore(scheme)

mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
    Scheme: scheme,
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return storectrl.NewCache(store, scheme,
            storectrl.WithDefaultTransform(storectrl.TransformStripManagedFields()),
            storectrl.WithDefaultUnsafeDisableDeepCopy(true),
            storectrl.WithSyncPeriod(30*time.Second),
            storectrl.WithByObject(&Widget{}, storectrl.ByObjectConfig{
                Label: labels.SelectorFromSet(labels.Set{"env": "production"}),
            }),
        ), nil
    },
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, uncachedObjects ...client.Object) (client.Client, error) {
        return storectrl.NewClient(store, scheme), nil
    },
})
```

### What You Keep from controller-runtime

Because storectrl plugs into the standard manager, you get all of its operational features for free:

- Leader election via Kubernetes Leases
- Metrics server (Prometheus)
- Health probes (liveness/readiness)
- Event recording (real Kubernetes Events)
- Graceful shutdown
- All standard controller options (MaxConcurrentReconciles, RateLimiter, etc.)

## Making Your Types Compatible

Types must implement `client.Object`. storectrl provides helpers.

### Option A: Embed storectrl.BaseObject (Recommended)

```go
type Widget struct {
    storectrl.BaseObject `json:",inline"`

    Spec   WidgetSpec   `json:"spec"`
    Status WidgetStatus `json:"status"`
}

// DeepCopyObject implements runtime.Object
func (w *Widget) DeepCopyObject() runtime.Object {
    if w == nil {
        return nil
    }
    out := &Widget{}
    w.BaseObject.DeepCopyInto(&out.BaseObject)
    out.Spec = w.Spec
    out.Status = w.Status
    return out
}
```

### Option B: Use Existing metav1 Embedding

If your types already embed `metav1.TypeMeta` and `metav1.ObjectMeta`, they work as-is.

### List Types

Every object type needs a corresponding list type. Embed `storectrl.BaseList`:

```go
type WidgetList struct {
    storectrl.BaseList `json:",inline"`
    Items              []Widget `json:"items"`
}

func (w *WidgetList) DeepCopyObject() runtime.Object {
    if w == nil {
        return nil
    }
    out := &WidgetList{}
    w.BaseList.DeepCopyInto(&out.BaseList)
    if w.Items != nil {
        out.Items = make([]Widget, len(w.Items))
        for i := range w.Items {
            out.Items[i] = *w.Items[i].DeepCopyObject().(*Widget)
        }
    }
    return out
}
```

### Register Types with a Scheme

```go
var (
    GroupVersion = schema.GroupVersion{Group: "myapp.example.com", Version: "v1"}
    scheme       = runtime.NewScheme()
)

func init() {
    scheme.AddKnownTypes(GroupVersion, &Widget{}, &WidgetList{})
    metav1.AddToGroupVersion(scheme, GroupVersion)
}
```

## Reconciler Code: No Changes

Your reconciler code requires no changes. All standard client operations and error checks work:

```go
func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    widget := &Widget{}

    if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
        if errors.IsNotFound(err) {  // Works -- storectrl errors implement APIStatus
            return ctrl.Result{}, nil
        }
        return ctrl.Result{}, err
    }

    widget.Status.Phase = "Running"

    if err := r.Update(ctx, widget); err != nil {
        if errors.IsConflict(err) {  // Optimistic concurrency works
            return ctrl.Result{Requeue: true}, nil
        }
        return ctrl.Result{}, err
    }

    return ctrl.Result{}, nil
}
```

## Writing a Custom Store Backend

Implement the `Store` interface to connect storectrl to your backend:

```go
type Store interface {
    Get(ctx context.Context, key client.ObjectKey, obj client.Object) error
    List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
    Create(ctx context.Context, obj client.Object) error
    Update(ctx context.Context, obj client.Object) error
    Delete(ctx context.Context, obj client.Object) error
    Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (Watcher, error)
    Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error
}
```

See `memory/store.go` as a reference implementation. Key requirements:

- **Thread safety**: Store must be safe for concurrent use.
- **Global revision counter**: Every mutation gets a unique, monotonically increasing revision used as the object's ResourceVersion. Not per-object.
- **Optimistic concurrency**: Update must check ResourceVersion and return `ConflictError` on mismatch.
- **Deep copying**: Deep-copy objects before storing -- callers may mutate after Create/Update.
- **Watch resumption**: Check opts for `WatchFromRevision`, replay events from a bounded event log, return `RevisionTooOldError` if the revision has been compacted.
- **List revision**: Set the list's ResourceVersion to the current global revision so callers can start a watch without a gap.
- **Overflow handling**: When a watcher's channel buffer is full, close the watch (not silently drop events) so consumers can reconnect and replay.
- **UID generation**: Create must generate a unique UID if not already set.
- **Apply**: Implement server-side-apply-style merges if your backend supports it, or return a clear error.

## Cache Configuration

`NewCache` accepts `CacheOption` functional options. These mirror controller-runtime's `cache.Options` where applicable.

### Available Options

| Option | Description |
|---|---|
| `WithDefaultTransform(fn)` | Transform objects before caching. Reduces memory for large objects. |
| `WithDefaultUnsafeDisableDeepCopy(bool)` | Skip deep copy on cache reads (Get/List). Caller must DeepCopy before mutating. |
| `WithDefaultLabelSelector(sel)` | Filter objects entering the cache by label. Passed to Store.List/Watch. |
| `WithDefaultFieldSelector(sel)` | Filter by metadata fields (metadata.name, metadata.namespace). Passed to Store.List/Watch. |
| `WithDefaultEnableWatchBookmarks(bool)` | Request backends to send BOOKMARK events during Watch. Passed as `EnableWatchBookmarks` ListOption to Store.Watch. Backends honor or ignore. |
| `WithDefaultNamespaces([]string)` | Restrict cache to objects from listed namespaces. Use `AllNamespaces` ("") to include unlisted namespaces. Simplified version of CR's `map[string]cache.Config`. |
| `WithByObject(obj, config)` | Per-GVK overrides via `ByObjectConfig` (Transform, UnsafeDisableDeepCopy, Label, Field, EnableWatchBookmarks). |
| `WithReaderFailOnMissingInformer(bool)` | When true (default), Get/List return error for unregistered types. When false, auto-creates informers. |
| `WithSyncPeriod(d)` | Periodic resync: delivers synthetic OnUpdate events for all cached objects to event handlers. |

### Helpers

`TransformStripManagedFields()` returns a transform that strips `managedFields` from objects before caching. Useful when managedFields are not needed and you want to reduce memory:

```go
storectrl.NewCache(store, scheme,
    storectrl.WithDefaultTransform(storectrl.TransformStripManagedFields()),
)
```

### Per-GVK Configuration

Use `WithByObject` to override defaults for specific types:

```go
storectrl.NewCache(store, scheme,
    storectrl.WithDefaultUnsafeDisableDeepCopy(true),
    storectrl.WithByObject(&Widget{}, storectrl.ByObjectConfig{
        UnsafeDisableDeepCopy: ptr.To(false), // override: deep copy Widgets
        Label: labels.SelectorFromSet(labels.Set{"tier": "control-plane"}),
    }),
)
```

### Namespace Filtering

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

### Mix-and-Match: Custom Informer + Store Client

`NewCache` and `NewClient` are independent. You can combine storectrl components with controller-runtime's standard implementations:

```go
// storectrl client + CR standard cache (e.g., for types that need K8s API caching)
mgr, _ := ctrl.NewManager(restConfig, ctrl.Options{
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, ...) (client.Client, error) {
        return storectrl.NewClient(store, scheme), nil
    },
    // Uses CR's default cache backed by the API server
})
```

Controller-runtime's `NewInformer` factory can compose with storectrl via `StoreListerWatcher` — see [Alternative: StoreListerWatcher](#alternative-storelisterwatcher) below.

### Unsupported controller-runtime Options

These `cache.Options` fields are K8s-specific and have no Store equivalent:

- **HTTPClient, Mapper** -- K8s REST plumbing. storectrl replaces the informer stack entirely.
- **NewInformer** -- not used by `storectrl.NewCache`, but composable via `StoreListerWatcher` (see [below](#alternative-storelisterwatcher)).
- **WatchErrorHandler** -- storectrl handles watch errors internally with exponential backoff and automatic reconnection.

## Alternative: StoreListerWatcher

`StoreListerWatcher` adapts a `Store` into a client-go `ListerWatcher`, letting you use controller-runtime's full informer stack (Reflector, DeltaFIFO, SharedIndexInformer) against any Store backend.

This is opt-in. The default `NewCache` replaces the informer stack entirely. Use `StoreListerWatcher` when you want DeltaFIFO event coalescing, full multi-namespace support (per-namespace informers), or automatic adoption of future CR cache features — and accept the coupling to CR internals.

### Usage with CR's NewInformer

```go
mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
    Scheme: scheme,
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return cache.New(cfg, cache.Options{
            Scheme:     scheme,
            HTTPClient: http.DefaultClient,
            Mapper:     meta.NewDefaultRESTMapper([]schema.GroupVersion{}),
            NewInformer: func(lw toolscache.ListerWatcher, obj runtime.Object, d time.Duration, idx toolscache.Indexers) toolscache.SharedIndexInformer {
                // Substitute the REST-based lw with a Store-backed one.
                storeLW := storectrl.NewStoreListerWatcher(store, &WidgetList{})
                return toolscache.NewSharedIndexInformer(storeLW, obj, d, idx)
            },
        })
    },
})
```

### Trade-offs

| Aspect | NewCache (default) | StoreListerWatcher |
|---|---|---|
| CR feature parity | Manual catch-up | Automatic |
| Multi-namespace | Simplified (namespace filter) | Full (per-NS informer) |
| DeltaFIFO | No (direct event delivery) | Yes (coalesces rapid updates) |
| K8s coupling | Low | Higher (dummy restConfig, CR internals) |
| Debugging | Simple (our code) | Harder (CR internals + adapter) |

See [docs/listerwatcher-adapter.md](listerwatcher-adapter.md) for the full design evaluation.

## Limitations

storectrl provides `client.Client` and `cache.Cache` -- not a full Kubernetes API server. Some behavioral differences remain:

- **Subresources**: Only `status` is supported. Custom subresources like `scale` are no-ops.
- **Owner references / GC**: Setting owner references works, but cascade deletion is not automatic.
- **RESTMapper**: Stub implementation. Dynamic type discovery is not supported.
- **Field indexes**: Must be created via `cache.IndexField()` before starting the manager.

## Next Steps

1. Define your domain types using `storectrl.BaseObject` and `storectrl.BaseList`
2. Register them with a scheme
3. Implement a `Store` for your backend (or use `memory.NewStore` for testing)
4. Add the two factory overrides to your `ctrl.Options`
5. Write reconcilers using standard controller-runtime patterns

For a complete working example, see [example_test.go](../example_test.go).
