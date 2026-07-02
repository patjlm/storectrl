# Migrating to ctrlforge

## Overview

ctrlforge is a component library that provides `client.Client` and `cache.Cache` implementations backed by a pluggable `Store` interface. It does not replace controller-runtime -- it plugs into it. You keep your standard `ctrl.NewManager` setup and override two factory functions.

### When to Use ctrlforge

Use ctrlforge when you want controllers that reconcile against non-Kubernetes backends (SQL, GCP APIs, in-memory, etc.) while keeping the standard controller-runtime reconciler pattern, lifecycle management, and operational features.

## Migration: Two Factory Overrides

### Before: Standard controller-runtime

```go
mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
    Scheme: scheme,
})
```

### After: ctrlforge-backed

```go
store := memory.NewStore(scheme) // or your custom Store implementation

mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
    Scheme: scheme,
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return ctrlforge.NewCache(store, scheme), nil
    },
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, uncachedObjects ...client.Object) (client.Client, error) {
        return ctrlforge.NewClient(store, scheme), nil
    },
})
```

Everything else stays the same -- controller setup, reconciler code, `mgr.Start()`.

### What You Keep from controller-runtime

Because ctrlforge plugs into the standard manager, you get all of its operational features for free:

- Leader election via Kubernetes Leases
- Metrics server (Prometheus)
- Health probes (liveness/readiness)
- Event recording (real Kubernetes Events)
- Graceful shutdown
- All standard controller options (MaxConcurrentReconciles, RateLimiter, etc.)

## Making Your Types Compatible

Types must implement `client.Object`. ctrlforge provides helpers.

### Option A: Embed ctrlforge.BaseObject (Recommended)

```go
type Widget struct {
    ctrlforge.BaseObject `json:",inline"`

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

Every object type needs a corresponding list type. Embed `ctrlforge.BaseList`:

```go
type WidgetList struct {
    ctrlforge.BaseList `json:",inline"`
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
        if errors.IsNotFound(err) {  // Works -- ctrlforge errors implement APIStatus
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

Implement the `Store` interface to connect ctrlforge to your backend:

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

## Limitations

ctrlforge provides `client.Client` and `cache.Cache` -- not a full Kubernetes API server. Some behavioral differences remain:

- **Subresources**: Only `status` is supported. Custom subresources like `scale` are no-ops.
- **Owner references / GC**: Setting owner references works, but cascade deletion is not automatic.
- **RESTMapper**: Stub implementation. Dynamic type discovery is not supported.
- **Field indexes**: Must be created via `cache.IndexField()` before starting the manager.

## Next Steps

1. Define your domain types using `ctrlforge.BaseObject` and `ctrlforge.BaseList`
2. Register them with a scheme
3. Implement a `Store` for your backend (or use `memory.NewStore` for testing)
4. Add the two factory overrides to your `ctrl.Options`
5. Write reconcilers using standard controller-runtime patterns

For a complete working example, see [example_test.go](../example_test.go).
