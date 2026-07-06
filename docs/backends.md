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
| `*FencedError` | Write when lease is lost (sharded backends) | 409 |

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

### ResourceVersion

ResourceVersion must be a numeric string (parseable via `strconv.ParseInt`) that increases monotonically with each mutation. The simplest approach is a global counter (memory, filesystem backends). Sharded backends like postgres may use per-object version numbers — see the `SkipGlobalRevisionMonotonicity` flag in the conformance suite.

Used for:

- Optimistic concurrency on Update
- Watch resumption from a known point
- List's ResourceVersion (set to current revision so callers can start a watch without a gap)
- Cache-level stale-event suppression (cache skips events with RV ≤ cached)

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

### Snapshot-consistent List

List must return a snapshot-consistent view — all returned objects reflect state at a single point in time. For database backends, use a repeatable-read transaction or equivalent isolation.

### No-op suppression (recommended)

When Update receives content identical to what's stored, skip the revision bump and event emission. This prevents unnecessary handler calls in scenarios where status appliers or reconcilers rewrite unchanged content. The memory and filesystem backends implement this via JSON marshal comparison.

### Revision persistence

If the backend survives restarts, persist the revision counter. See `filesystem/store.go` (`.revision` file) for an example.

### Apply

Backends that support server-side-apply-style merges implement real logic. Others return a clear error.

## Conformance test suite

Use `storetest.TestStore` to verify your backend satisfies all requirements:

```go
func TestMyBackend(t *testing.T) {
    storetest.TestStore(t, storetest.Config{
        NewStore: func(scheme *runtime.Scheme) storectrl.Store {
            return mybackend.New(scheme)
        },
        SkipApply: true, // if Apply is not supported
    })
}
```

The suite tests CRUD, watch, concurrency, and invariants (revision monotonicity, no-op suppression, snapshot consistency, watch resumption). See `storetest.Config` for skip flags when your backend has intentionally different semantics.

## Step-by-step guide

1. Create a new package (e.g., `storectrl/sql`)
2. Implement `storectrl.Store` — use `memory/store.go` as reference
3. Run the conformance suite to verify all requirements
4. Add backend-specific tests for features not covered by the suite

## BaseObject and BaseList

storectrl provides convenience types for implementing `client.Object`:

- `BaseObject` = `metav1.TypeMeta` + `metav1.ObjectMeta` — types embedding it satisfy `client.Object` (except `DeepCopyObject` which each type must implement)
- `BaseList` = `metav1.TypeMeta` + `metav1.ListMeta` — for list types

If your types already embed `metav1.TypeMeta` and `metav1.ObjectMeta`, they work as-is.
