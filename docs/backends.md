# Implementing a Store Backend

## Store interface

The `Store` interface is the only thing backend authors implement. Everything else (Client, Cache) is built on top.

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

See `memory/store.go` for a complete reference implementation.

## Error contract

Return these error types so `apierrors.IsNotFound()` etc. work transparently:

| Error type | When to return | K8s equivalent |
|---|---|---|
| `*NotFoundError` | Get/Update/Delete of nonexistent object | 404 |
| `*AlreadyExistsError` | Create with existing key | 409 |
| `*ConflictError` | Update with stale ResourceVersion | 409 |
| `*RevisionTooOldError` | Watch from compacted revision | 410 Gone |

All error types implement `APIStatus`. This is critical — controller code universally uses `apierrors.IsNotFound(err)` and similar checks.

## Watcher interface

Returned by `Store.Watch`. Streams events on a channel.

```go
type Watcher interface {
    ResultChan() <-chan Event
    Stop()
}

type Event struct {
    Type   EventType
    Object client.Object
}
```

Backend decides how to implement — channels, polling, pub/sub, etc.

## Requirements

### Thread safety

Store must be safe for concurrent use. Use `sync.RWMutex` (reads are frequent, writes are rare).

### Global revision counter

Every mutation gets a unique, monotonically increasing revision used as the object's `ResourceVersion`. Use a global counter, not per-object. This is used for:

- Optimistic concurrency on Update
- Watch resumption from a known point
- List's ResourceVersion (set to current global revision so callers can start a watch without a gap)

### Optimistic concurrency

Update must check `ResourceVersion` and return `*ConflictError` on mismatch. This enables standard controller retry patterns.

### Deep copying

Deep-copy objects before storing — callers may mutate after Create/Update.

### UID generation

Create must generate a unique UID if not already set.

### Watch resumption

Backends must check opts for `WatchFromRevision`. When present:

1. Replay events that occurred after that revision before switching to live delivery
2. If the requested revision has been compacted out of the event log, return `*RevisionTooOldError` — the cache layer handles this by doing a full relist

Maintain a bounded event log for replay (configurable via `WithEventLogSize` or similar).

### Overflow handling

When a watcher's channel buffer is full, **close the watch** (not silently drop events). This lets consumers reconnect and replay from the event log.

### Revision persistence

If the backend survives restarts, persist the revision counter. See `filesystem/store.go` (`.revision` file) for an example.

### Apply

Backends that support server-side-apply-style merges implement real logic. Others return a clear error.

## Step-by-step guide

1. Create a new package (e.g., `storectrl/sql`)
2. Implement `storectrl.Store` — use `memory/store.go` as reference
3. Verify all requirements above are met
4. Add tests using the same patterns as `example_test.go`

## BaseObject and BaseList

storectrl provides convenience types for implementing `client.Object`:

- `BaseObject` = `metav1.TypeMeta` + `metav1.ObjectMeta` — types embedding it satisfy `client.Object` (except `DeepCopyObject` which each type must implement)
- `BaseList` = `metav1.TypeMeta` + `metav1.ListMeta` — for list types

If your types already embed `metav1.TypeMeta` and `metav1.ObjectMeta`, they work as-is.
