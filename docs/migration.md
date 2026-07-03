# Migrating from controller-runtime

storectrl plugs into controller-runtime — it doesn't replace it. You keep your standard `ctrl.NewManager` setup and override two factory functions.

## Before

```go
mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
    Scheme: scheme,
})
```

## After

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

Everything else stays the same — controller setup, reconciler code, `mgr.Start()`.

`NewCache` accepts functional options for cache configuration. See [Cache configuration](usage.md) for all available options.

## Making your types compatible

Types must implement `client.Object`. Two options:

### Option A: Embed storectrl.BaseObject (recommended)

```go
type Widget struct {
    storectrl.BaseObject `json:",inline"`
    Spec   WidgetSpec   `json:"spec"`
    Status WidgetStatus `json:"status"`
}

func (w *Widget) DeepCopyObject() runtime.Object {
    out := &Widget{}
    w.BaseObject.DeepCopyInto(&out.BaseObject)
    out.Spec = w.Spec
    out.Status = w.Status
    return out
}
```

### Option B: Use existing metav1 embedding

If your types already embed `metav1.TypeMeta` and `metav1.ObjectMeta`, they work as-is.

### List types

Every object type needs a corresponding list type:

```go
type WidgetList struct {
    storectrl.BaseList `json:",inline"`
    Items              []Widget `json:"items"`
}

func (w *WidgetList) DeepCopyObject() runtime.Object {
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

### Register types with a scheme

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

## Reconciler code: no changes

Your reconciler code requires no changes. All standard client operations and error checks work:

```go
func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    widget := &Widget{}
    if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
        if errors.IsNotFound(err) {  // Works — storectrl errors implement APIStatus
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

## What you keep from controller-runtime

Because storectrl plugs into the standard manager, you get all operational features for free:

- Leader election via Kubernetes Leases
- Metrics server (Prometheus)
- Health probes (liveness/readiness)
- Event recording (real Kubernetes Events)
- Graceful shutdown
- All standard controller options (MaxConcurrentReconciles, RateLimiter, etc.)

## Limitations

- **Subresources**: Only `status` is supported. Custom subresources like `scale` are no-ops.
- **Owner references / GC**: Setting owner references works, but cascade deletion is not automatic.
- **RESTMapper**: Stub implementation. Dynamic type discovery is not supported.
- **Field indexes**: Must be created via `cache.IndexField()` before starting the manager.

## Next steps

- [Cache configuration & advanced usage](usage.md) — cache options, StoreListerWatcher, mix-and-match
- [Implementing a Store backend](backends.md) — Store interface, error contracts, watch resumption
- [example_test.go](../example_test.go) — complete working example
