# Migrating to ctrlforge

## Overview

ctrlforge is a Go library that brings the controller-runtime reconciliation pattern to any backend datastore, not just the Kubernetes API server. It implements controller-runtime's `client.Client`, `cache.Cache`, and `manager.Manager` interfaces backed by a pluggable `Store` interface.

This means you can write reconcilers using familiar controller-runtime patterns while persisting state to SQL databases, cloud APIs (GCP, AWS, Azure), or any other backend — not just Kubernetes. Your reconciler code stays largely unchanged; only the infrastructure setup differs.

### When to Use ctrlforge

Use ctrlforge when:
- You want to build controllers that reconcile against non-Kubernetes backends (SQL, GCP APIs, etc.)
- You need the reconciliation pattern but don't have a Kubernetes API server
- You want to reuse existing controller-runtime knowledge and patterns
- You're building control planes or operators for custom resources stored outside of Kubernetes

Don't use ctrlforge if:
- You're working with actual Kubernetes resources in a real cluster (use controller-runtime directly)
- You need admission webhooks or other Kubernetes-specific features

## Making Your Types Compatible

For your domain types to work with ctrlforge, they must implement the `client.Object` interface from controller-runtime. ctrlforge provides helpers to make this simple.

### Option A: Embed ctrlforge.BaseObject (Recommended)

The simplest approach is to embed `ctrlforge.BaseObject` in your types:

```go
package myapp

import (
    "k8s.io/apimachinery/pkg/runtime"
    "ctrlforge"
)

// Widget is your custom domain type
type Widget struct {
    ctrlforge.BaseObject `json:",inline"`
    
    Spec   WidgetSpec   `json:"spec"`
    Status WidgetStatus `json:"status"`
}

type WidgetSpec struct {
    Color string `json:"color"`
    Size  int    `json:"size"`
}

type WidgetStatus struct {
    Ready bool   `json:"ready"`
    Phase string `json:"phase"`
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

If your types already embed `metav1.TypeMeta` and `metav1.ObjectMeta`, they're already compatible:

```go
import (
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime"
)

type Widget struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    
    Spec   WidgetSpec   `json:"spec"`
    Status WidgetStatus `json:"status"`
}

func (w *Widget) DeepCopyObject() runtime.Object {
    if w == nil {
        return nil
    }
    out := &Widget{}
    out.TypeMeta = w.TypeMeta
    w.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
    out.Spec = w.Spec
    out.Status = w.Status
    return out
}
```

### List Types

Every object type needs a corresponding list type. Embed `ctrlforge.BaseList`:

```go
type WidgetList struct {
    ctrlforge.BaseList `json:",inline"`
    Items             []Widget `json:"items"`
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

Create a scheme and register your types:

```go
import (
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
    // Define a GroupVersion for your types
    GroupVersion = schema.GroupVersion{
        Group:   "myapp.example.com",
        Version: "v1",
    }
    
    // Create a new scheme
    scheme = runtime.NewScheme()
)

func init() {
    // Register types with the scheme
    scheme.AddKnownTypes(GroupVersion,
        &Widget{},
        &WidgetList{},
    )
    
    // Register TypeMeta information
    metav1.AddToGroupVersion(scheme, GroupVersion)
}
```

## Before/After Code Comparison

### Before: controller-runtime + Kubernetes API

```go
package main

import (
    "context"
    "k8s.io/apimachinery/pkg/runtime"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
    ctx := context.Background()
    
    // Setup with Kubernetes config
    mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
        Scheme: scheme,
    })
    if err != nil {
        panic(err)
    }
    
    // Create controller
    if err := ctrl.NewControllerManagedBy(mgr).
        For(&Widget{}).
        Complete(&WidgetReconciler{
            Client: mgr.GetClient(),
        }); err != nil {
        panic(err)
    }
    
    // Start manager
    if err := mgr.Start(ctx); err != nil {
        panic(err)
    }
}
```

### After: ctrlforge + Custom Backend

```go
package main

import (
    "context"
    "ctrlforge"
    "ctrlforge/memory"
)

func main() {
    ctx := context.Background()
    
    // Setup with custom store backend
    store := memory.NewStore(scheme)  // or sqlstore.New(db), gcpstore.New(...)
    
    mgr, err := ctrlforge.NewManager(store, ctrlforge.ManagerOptions{
        Scheme: scheme,
    })
    if err != nil {
        panic(err)
    }
    
    // Create controller (identical to before!)
    if err := ctrlforge.NewControllerManagedBy(mgr).
        For(&Widget{}).
        Complete(&WidgetReconciler{
            Client: mgr.GetClient(),
        }); err != nil {
        panic(err)
    }
    
    // Start manager
    if err := mgr.Start(ctx); err != nil {
        panic(err)
    }
}
```

The only difference: swap `ctrl.NewManager(restConfig, ...)` for `ctrlforge.NewManager(store, ...)`.

## Reconciler Changes

The great news: your reconciler code requires minimal to no changes.

### What Stays the Same

```go
package main

import (
    "context"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"
    "k8s.io/apimachinery/pkg/api/errors"
)

type WidgetReconciler struct {
    client.Client
}

func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    widget := &Widget{}
    
    // Get works the same
    if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
        if errors.IsNotFound(err) {  // Error checks work the same!
            return ctrl.Result{}, nil
        }
        return ctrl.Result{}, err
    }
    
    // Business logic
    widget.Status.Ready = widget.Spec.Size > 0
    widget.Status.Phase = "Running"
    
    // Update works the same
    if err := r.Update(ctx, widget); err != nil {
        if errors.IsConflict(err) {  // Optimistic concurrency works!
            return ctrl.Result{Requeue: true}, nil
        }
        return ctrl.Result{}, err
    }
    
    // Status updates work the same
    if err := r.Status().Update(ctx, widget); err != nil {
        return ctrl.Result{}, err
    }
    
    return ctrl.Result{}, nil
}
```

All the standard client operations work:
- `r.Get(ctx, key, obj)` — retrieve objects
- `r.List(ctx, list, opts...)` — list objects with label/field selectors
- `r.Create(ctx, obj)` — create new objects
- `r.Update(ctx, obj)` — update existing objects
- `r.Delete(ctx, obj)` — delete objects
- `r.Status().Update(ctx, obj)` — update status subresource

All the standard error checks work:
- `apierrors.IsNotFound(err)` — object doesn't exist
- `apierrors.IsAlreadyExists(err)` — object already exists
- `apierrors.IsConflict(err)` — optimistic concurrency conflict

### What Requires Attention

1. **Webhooks** — Admission webhooks are not supported. Validation and defaulting must be handled in reconcilers or application logic.

2. **Subresources** — Only the `status` subresource is supported. Custom subresources like `scale` are no-ops.

3. **Leader Election** — Not yet implemented. If you need HA controllers, you'll need to implement leader election separately.

4. **RESTMapper** — The RESTMapper is a stub implementation. Dynamic type discovery and REST mapping are not supported.

5. **Owner References & GC** — Garbage collection based on owner references is not automatic. You must handle cascade deletion in your reconciler logic.

6. **Field Indexes** — Must be created explicitly via `cache.IndexField()` before starting the manager. They are not auto-created from field selectors.

## Writing a Custom Store Backend

To connect ctrlforge to your backend (SQL, cloud APIs, etc.), implement the `Store` interface:

```go
type Store interface {
    Get(ctx context.Context, key client.ObjectKey, obj client.Object) error
    List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
    Create(ctx context.Context, obj client.Object) error
    Update(ctx context.Context, obj client.Object) error
    Delete(ctx context.Context, obj client.Object) error
    Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (Watcher, error)
}
```

### Example: SQL Backend (Pseudocode)

```go
package sqlstore

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    
    "k8s.io/apimachinery/pkg/runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"
    "ctrlforge"
)

type SQLStore struct {
    db     *sql.DB
    scheme *runtime.Scheme
    // watchers for change notifications
    watchers *watcherManager
}

func New(db *sql.DB, scheme *runtime.Scheme) *SQLStore {
    return &SQLStore{
        db:       db,
        scheme:   scheme,
        watchers: newWatcherManager(),
    }
}

func (s *SQLStore) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
    gvk, _ := s.gvkForObject(obj)
    
    var data []byte
    query := `SELECT data FROM objects WHERE gvk = ? AND namespace = ? AND name = ?`
    err := s.db.QueryRowContext(ctx, query, gvkString(gvk), key.Namespace, key.Name).Scan(&data)
    if err == sql.ErrNoRows {
        return &ctrlforge.NotFoundError{Key: key.String()}
    }
    if err != nil {
        return err
    }
    
    return json.Unmarshal(data, obj)
}

func (s *SQLStore) Create(ctx context.Context, obj client.Object) error {
    gvk, _ := s.gvkForObject(obj)
    key := client.ObjectKeyFromObject(obj)
    
    // Generate UID and ResourceVersion
    obj.SetUID(types.UID(uuid.New().String()))
    obj.SetResourceVersion("1")
    
    data, err := json.Marshal(obj)
    if err != nil {
        return err
    }
    
    query := `INSERT INTO objects (gvk, namespace, name, data, resource_version) VALUES (?, ?, ?, ?, ?)`
    _, err = s.db.ExecContext(ctx, query, gvkString(gvk), key.Namespace, key.Name, data, "1")
    if isDuplicateKeyError(err) {
        return &ctrlforge.AlreadyExistsError{Key: key.String()}
    }
    
    // Notify watchers
    s.watchers.Notify(gvk, ctrlforge.Event{Type: ctrlforge.EventAdded, Object: obj})
    return err
}

func (s *SQLStore) Update(ctx context.Context, obj client.Object) error {
    gvk, _ := s.gvkForObject(obj)
    key := client.ObjectKeyFromObject(obj)
    expectedRV := obj.GetResourceVersion()
    
    newRV := incrementResourceVersion(expectedRV)
    obj.SetResourceVersion(newRV)
    
    data, err := json.Marshal(obj)
    if err != nil {
        return err
    }
    
    // Optimistic concurrency: only update if ResourceVersion matches
    query := `UPDATE objects SET data = ?, resource_version = ? 
              WHERE gvk = ? AND namespace = ? AND name = ? AND resource_version = ?`
    result, err := s.db.ExecContext(ctx, query, data, newRV, 
                                    gvkString(gvk), key.Namespace, key.Name, expectedRV)
    if err != nil {
        return err
    }
    
    rows, _ := result.RowsAffected()
    if rows == 0 {
        return &ctrlforge.ConflictError{Key: key.String()}
    }
    
    // Notify watchers
    s.watchers.Notify(gvk, ctrlforge.Event{Type: ctrlforge.EventModified, Object: obj})
    return nil
}

func (s *SQLStore) Delete(ctx context.Context, obj client.Object) error {
    // Similar pattern...
    return nil
}

func (s *SQLStore) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (ctrlforge.Watcher, error) {
    gvk, _ := s.gvkForList(list)
    return s.watchers.NewWatcher(ctx, gvk, opts...)
}
```

### Implementation Notes

**Thread Safety**: Your Store implementation must be safe for concurrent use. Multiple reconcilers may call Store methods simultaneously. Use appropriate locking or rely on your backend's concurrency guarantees (e.g., SQL transactions).

**Optimistic Concurrency**: The ResourceVersion field implements optimistic concurrency control. Update operations should:
1. Check that the provided ResourceVersion matches the stored version
2. Increment the ResourceVersion on successful update
3. Return `ConflictError` if versions don't match

**Watching**: The Watch method must return a `Watcher` that streams change events. For SQL backends, you might use:
- Database triggers + LISTEN/NOTIFY (PostgreSQL)
- Polling with a timestamp column
- CDC (Change Data Capture) tools

**UID Generation**: Create operations should generate a unique UID if not already set. Use UUIDs or another guaranteed-unique scheme.

**Metadata Handling**: Preserve all metadata fields (labels, annotations, finalizers, etc.) through encode/decode cycles. JSON serialization handles this naturally.

## Limitations

ctrlforge provides core controller-runtime functionality but has several limitations:

### Not Supported

- **Admission Webhooks**: ValidatingWebhook, MutatingWebhook, and conversion webhooks are not available. Handle validation and defaulting in reconcilers.

- **Leader Election**: Not yet implemented. Running multiple replicas requires external leader election.

- **Server-Side Apply**: The three-way merge patch strategy is not available. Use standard Update operations.

- **API Discovery**: RESTMapper and API discovery are stub implementations. You cannot dynamically discover available types.

- **Advanced Subresources**: Only `status` is supported. Custom subresources like `scale`, `exec`, `logs` are no-ops.

- **Field Selectors on Non-Indexed Fields**: Efficient field selection requires creating indexes via `cache.IndexField()` before starting the manager.

- **Watch Resumption**: Watches do not support resume from a specific ResourceVersion. If a watch disconnects, it restarts from the current state.

### Behavioral Differences

- **Finalizers**: You can use finalizers, but there's no automatic garbage collection. Your reconciler must handle cleanup.

- **Owner References**: Setting owner references works, but cascade deletion is not automatic.

- **Events**: The Event recording API is not implemented. Use structured logging instead.

### Performance Considerations

- **Caching**: ctrlforge uses an in-memory cache just like controller-runtime. Ensure your Store's Watch implementation is efficient to keep the cache synchronized.

- **ResourceVersion**: ResourceVersion is a string to match Kubernetes semantics, but backends can use integers (as strings) for simpler increment logic.

- **List Operations**: Large list operations load all matching objects into memory. For huge datasets, consider pagination (not yet supported) or filtering at the Store level.

## Next Steps

1. **Define your domain types** using `ctrlforge.BaseObject` and `ctrlforge.BaseList`
2. **Create a scheme** and register your types
3. **Choose a backend**: start with `memory.NewStore(scheme)` for testing, then implement a Store for your production backend (SQL, GCP, etc.)
4. **Write reconcilers** using standard controller-runtime patterns
5. **Wire it up** with `ctrlforge.NewManager(store, opts)`

For a complete working example, see [example_test.go](../example_test.go).
