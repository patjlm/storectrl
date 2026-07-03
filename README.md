# storectrl

A Go library that provides pluggable `cache.Cache` and `client.Client` implementations for [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime). Write controllers using the standard reconciler pattern — but store and watch data from any backend, not just the Kubernetes API server.

storectrl plugs into the standard controller-runtime manager via factory overrides. Everything else — leader election, metrics, health probes, graceful shutdown — works out of the box.

## When to use

- Building controllers that reconcile state in a **SQL database**, **GCP APIs**, or another non-Kubernetes datastore
- Prototyping controllers with a fast **in-memory backend** before wiring up a real store
- Running controller logic in **unit tests** without a real kube-apiserver or envtest

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  Your Controller                  │
│  Reconcile(ctx, req) → r.Get / r.Update / ...    │
└───────────────────────┬──────────────────────────┘
                        │ client.Client interface
┌───────────────────────┴──────────────────────────┐
│         Standard controller-runtime Manager       │
│  Leader election, metrics, health probes, ...     │
│                                                   │
│  ┌─────────────┐  ┌──────────┐                   │
│  │ storeClient │  │storeCache│  ← storectrl      │
│  │(client.Client│  │(cache.   │    components     │
│  │ impl)       │  │ Cache)   │                    │
│  └──────┬──────┘  └────┬─────┘                   │
└─────────┼──────────────┼─────────────────────────┘
          │              │
          └──────┬───────┘
                 │ storectrl.Store interface
    ┌────────────┴────────────────┐
    │    Backend Implementation    │
    │  memory │ SQL │ GCP │ ...   │
    └─────────────────────────────┘
```

## Quick start

### 1. Define your types

Embed `storectrl.BaseObject` and implement `DeepCopyObject`:

```go
type Cluster struct {
    storectrl.BaseObject `json:",inline"`
    Spec                ClusterSpec   `json:"spec"`
    Status              ClusterStatus `json:"status"`
}

func (c *Cluster) DeepCopyObject() runtime.Object {
    out := &Cluster{}
    c.BaseObject.DeepCopyInto(&out.BaseObject)
    out.Spec = c.Spec
    out.Status = c.Status
    return out
}

type ClusterList struct {
    storectrl.BaseList `json:",inline"`
    Items             []Cluster `json:"items"`
}

func (c *ClusterList) DeepCopyObject() runtime.Object {
    out := &ClusterList{}
    c.BaseList.DeepCopyInto(&out.BaseList)
    out.Items = make([]Cluster, len(c.Items))
    for i := range c.Items {
        out.Items[i] = *c.Items[i].DeepCopyObject().(*Cluster)
    }
    return out
}
```

### 2. Register types with a scheme

```go
var GroupVersion = schema.GroupVersion{Group: "myapp.example.com", Version: "v1"}

func NewScheme() *runtime.Scheme {
    s := runtime.NewScheme()
    s.AddKnownTypes(GroupVersion, &Cluster{}, &ClusterList{})
    metav1.AddToGroupVersion(s, GroupVersion)
    return s
}
```

### 3. Write a reconciler

Standard controller-runtime reconciler — no storectrl-specific code:

```go
type ClusterReconciler struct {
    client.Client
}

func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    cluster := &Cluster{}
    if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }
    // ... business logic ...
    return ctrl.Result{}, r.Status().Update(ctx, cluster)
}
```

### 4. Wire it up with a store

Override `NewCache` and `NewClient` in the standard manager options:

```go
scheme := NewScheme()
store := memory.NewStore(scheme)

mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
    Scheme: scheme,
    NewCache: func(cfg *rest.Config, opts cache.Options) (cache.Cache, error) {
        return storectrl.NewCache(store, scheme), nil
    },
    NewClient: func(c cache.Cache, cfg *rest.Config, opts client.Options, uncachedObjects ...client.Object) (client.Client, error) {
        return storectrl.NewClient(store, scheme), nil
    },
})

ctrl.NewControllerManagedBy(mgr).
    For(&Cluster{}).
    Complete(&ClusterReconciler{Client: mgr.GetClient()})

mgr.Start(ctrl.SetupSignalHandler())
```

For a complete working example, see [example_test.go](example_test.go).

## Included backends

| Backend | Package | Description |
|---------|---------|-------------|
| In-memory | `storectrl/memory` | Maps + channels, thread-safe, optimistic concurrency |
| Filesystem | `storectrl/filesystem` | JSON files, persisted revision counter |

## Limitations

- **No admission webhooks** — no API server to serve them
- **No API discovery** — RESTMapper is a stub
- **SubResources beyond `status`** — return unsupported error
- **Patch** — supports JSON Patch and Merge Patch; strategic merge patch treated as regular merge
- **Apply** — delegated to Store backend; backends that don't support it return an error

## Dependencies

- Go 1.24+
- `sigs.k8s.io/controller-runtime` v0.24+
- `k8s.io/apimachinery` v0.36+
- `k8s.io/client-go` v0.36+

## Further reading

- [Components & usage](docs/usage.md) — what each component provides vs CR, compositions, cache options
- [Implementing a Store backend](docs/backends.md) — Store interface, error contracts, watch resumption
- [Migrating from controller-runtime](docs/migration.md) — before/after comparison, type compatibility
- [Wiring examples](wiring_test.go) — runnable tests for each component and composition
