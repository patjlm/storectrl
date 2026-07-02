# reconkit

A Go library that provides the [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) reconciler pattern with pluggable backends. Write controllers using the same `client.Client`, `cache.Cache`, and `manager.Manager` interfaces you already know — but store and watch data from any backend, not just the Kubernetes API server.

## When to use

- Building controllers that reconcile state in a **SQL database**, **GCP APIs**, or another non-Kubernetes datastore
- Prototyping controllers with a fast **in-memory backend** before wiring up a real store
- Running controller logic in **unit tests** without a real kube-apiserver or envtest

## What stays the same

Your reconciler code is unchanged. `r.Get()`, `r.Update()`, `r.Status().Update()`, `apierrors.IsNotFound()`, `apierrors.IsConflict()` — all work identically.

```go
func (r *MyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    obj := &MyResource{}
    if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }
    // ... business logic ...
    return ctrl.Result{}, r.Status().Update(ctx, obj)
}
```

## Quick start

### 1. Define your types

Embed `reconkit.BaseObject` (which provides `TypeMeta` + `ObjectMeta`) and implement `DeepCopyObject`:

```go
package mycontroller

import (
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/runtime/schema"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "reconkit"
)

var GroupVersion = schema.GroupVersion{Group: "myapp.example.com", Version: "v1"}

type Cluster struct {
    reconkit.BaseObject `json:",inline"`
    Spec                ClusterSpec   `json:"spec"`
    Status              ClusterStatus `json:"status"`
}

type ClusterSpec struct {
    Region   string `json:"region"`
    NodeCount int   `json:"nodeCount"`
}

type ClusterStatus struct {
    Phase string `json:"phase"`
    Ready bool   `json:"ready"`
}

func (c *Cluster) DeepCopyObject() runtime.Object {
    out := &Cluster{}
    c.BaseObject.DeepCopyInto(&out.BaseObject)
    out.Spec = c.Spec
    out.Status = c.Status
    return out
}

type ClusterList struct {
    reconkit.BaseList `json:",inline"`
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
func NewScheme() *runtime.Scheme {
    s := runtime.NewScheme()
    s.AddKnownTypes(GroupVersion, &Cluster{}, &ClusterList{})
    metav1.AddToGroupVersion(s, GroupVersion)
    return s
}
```

### 3. Write the reconciler

Standard controller-runtime reconciler — no reconkit-specific code:

```go
type ClusterReconciler struct {
    client.Client
}

func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    cluster := &Cluster{}
    if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    // Reconcile desired vs actual state
    cluster.Status.Ready = cluster.Spec.NodeCount > 0
    if cluster.Status.Ready {
        cluster.Status.Phase = "Running"
    } else {
        cluster.Status.Phase = "Pending"
    }

    if err := r.Status().Update(ctx, cluster); err != nil {
        return ctrl.Result{}, err
    }
    return ctrl.Result{}, nil
}
```

### 4. Wire it up with a store

```go
package main

import (
    "context"
    "log"

    "reconkit"
    "reconkit/memory"
)

func main() {
    scheme := NewScheme()

    // Pick a backend — memory, SQL, GCP, etc.
    store := memory.NewStore(scheme)

    mgr, err := reconkit.NewManager(store, reconkit.ManagerOptions{
        Scheme: scheme,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Register the controller — same pattern as controller-runtime
    reconkit.NewControllerManagedBy(mgr).
        For(&Cluster{}).
        Complete(&ClusterReconciler{Client: mgr.GetClient()})

    // Run
    if err := mgr.Start(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

**Compare with standard controller-runtime:**

```go
// Only these two lines change:
// Before:  mgr, _ := ctrl.NewManager(restConfig, ctrl.Options{Scheme: scheme})
// After:   mgr, _ := reconkit.NewManager(store, reconkit.ManagerOptions{Scheme: scheme})
//
// Before:  ctrl.NewControllerManagedBy(mgr)
// After:   reconkit.NewControllerManagedBy(mgr)
```

## Implementing a custom Store

Implement the `Store` interface to connect any backend:

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

**Error contract:** Return `*reconkit.NotFoundError`, `*reconkit.AlreadyExistsError`, or `*reconkit.ConflictError` — they implement the `APIStatus` interface so `apierrors.IsNotFound()` etc. work transparently.

**Optimistic concurrency:** Check `ResourceVersion` on Update and return `ConflictError` on mismatch. Set `UID` and `ResourceVersion` on Create.

**Watch:** Return a `Watcher` that streams `Event{Type, Object}` on a channel. Implementation depends on the backend — Postgres LISTEN/NOTIFY, GCP Pub/Sub, polling, etc.

See `memory/store.go` for a complete reference implementation.

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  Your Controller                  │
│  Reconcile(ctx, req) → r.Get / r.Update / ...    │
└───────────────────────┬──────────────────────────┘
                        │ client.Client interface
┌───────────────────────┴──────────────────────────┐
│              reconkit.Manager                     │
│  ┌─────────────┐  ┌──────────┐  ┌─────────────┐ │
│  │ storeClient │  │storeCache│  │  Builder /   │ │
│  │(client.Client│  │(cache.   │  │ Controller  │ │
│  │ impl)       │  │ Cache)   │  │  lifecycle  │ │
│  └──────┬──────┘  └────┬─────┘  └─────────────┘ │
└─────────┼──────────────┼─────────────────────────┘
          │              │
          └──────┬───────┘
                 │ reconkit.Store interface
    ┌────────────┴────────────────┐
    │    Backend Implementation    │
    │  memory │ SQL │ GCP │ ...   │
    └─────────────────────────────┘
```

## Included backends

| Backend | Package | Status |
|---------|---------|--------|
| In-memory | `reconkit/memory` | Ready — maps + channels, thread-safe, optimistic concurrency |

## Limitations

- **No admission webhooks** — no API server to serve them
- **No leader election** — manager always considers itself elected
- **No API discovery** — RESTMapper is a stub
- **SubResources beyond `status`** — return unsupported error
- **Patch** — supports JSON Patch and Merge Patch; strategic merge patch treated as regular merge
- **Apply** — delegated to Store backend; backends that don't support it return an error (e.g. the in-memory store)

See [docs/migration.md](docs/migration.md) for a detailed migration guide.

## Dependencies

- Go 1.24+
- `sigs.k8s.io/controller-runtime` v0.24+
- `k8s.io/apimachinery` v0.36+
- `k8s.io/client-go` v0.36+
