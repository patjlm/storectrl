# reconkit — Agent Guide

Instructions for AI agents working on this codebase.

## What this is

reconkit is a Go library that reimplements controller-runtime's core interfaces (`client.Client`, `cache.Cache`, `manager.Manager`) with a pluggable `Store` backend instead of the Kubernetes API server. It lets developers write controllers using the standard reconciler pattern against any datastore.

## Module layout

```
reconkit/                 # Main package — all public API
├── store.go              # Store interface, WatchFromRevision option
├── watcher.go            # Watcher, Event, EventType (incl. EventBookmark)
├── errors.go             # NotFoundError, AlreadyExistsError, ConflictError, RevisionTooOldError
├── object.go             # BaseObject, BaseList (embed for client.Object compat)
├── client.go             # client.Client implementation wrapping Store
├── cache.go              # cache.Cache implementation (watch-backed in-memory cache)
├── source.go             # source.Source implementation (feeds watch events to work queue)
├── manager.go            # manager.Manager implementation (lifecycle orchestration)
├── builder.go            # Fluent controller builder (NewControllerManagedBy)
├── example_test.go       # CRUD, concurrency, and reconciler tests
├── docs/migration.md     # Migration guide from controller-runtime
├── memory/               # In-memory Store backend
│   ├── store.go           # MemoryStore — global revision, event log, watch resumption
│   └── watcher.go         # memoryWatcher — overflow-closes-watch for safe reconnect
└── filesystem/            # Filesystem Store backend
    ├── store.go           # FileStore — JSON files, persisted revision counter
    └── watcher.go         # fileWatcher — same overflow pattern as memory
```

## Key interfaces

### Store (store.go)

The **only interface backend authors implement**. Seven methods: Get, List, Create, Update, Delete, Watch, Apply. Everything else (Client, Cache, Manager) is built on top.

Error contract matters: return `*NotFoundError`, `*AlreadyExistsError`, `*ConflictError`, `*RevisionTooOldError` so that `apierrors.IsNotFound()` etc. work. These types implement `APIStatus`.

**Watch resumption contract:** backends must check opts for `WatchFromRevision`. When present, replay events that occurred after that revision before switching to live delivery. If the requested revision has been compacted out of the backend's event history, return `*RevisionTooOldError` (410 Gone equivalent) — the cache layer handles this by doing a full relist. Backends own their revision counter and event log; see `memory/store.go` as reference. Key points:

- Use a global monotonic revision counter (not per-object) — every mutation gets a unique revision used as the object's ResourceVersion
- Maintain a bounded event log for replay (configurable via `WithEventLogSize`)
- `List` must set the list's ResourceVersion to the current global revision so callers can start a watch from that point without a gap
- When a watcher's channel buffer overflows, close the watch (not silently drop events) so the consumer can reconnect and replay from the event log

Apply is backend-decided: backends that support server-side-apply-style merges implement real logic; others return a clear error.

### Watcher (watcher.go)

Returned by `Store.Watch`. Streams `Event{Type, Object}` on `ResultChan()`. Caller calls `Stop()` when done. Backend decides how to implement (channels, polling, pub/sub).

### BaseObject / BaseList (object.go)

Convenience embeds. `BaseObject` = `metav1.TypeMeta` + `metav1.ObjectMeta`. Types embedding it satisfy `client.Object` (except `DeepCopyObject` which each type must implement).

## Design decisions

1. **Reuse controller-runtime types, not reinvent them.** We import `client.Object`, `client.ObjectKey`, `cache.Cache`, `manager.Manager`, etc. directly. The goal is minimal code change for controller authors — only the manager creation and builder import change.

2. **Store is deliberately simple.** It mirrors `client.Reader` + `client.Writer` but without Patch, DeleteAllOf, or SubResources. The `client.Client` wrapper adds those on top (Patch = Get + apply + Update, DeleteAllOf = List + Delete each, Status = just Update). Apply is on Store directly because support varies by backend — each backend explicitly decides.

3. **Errors implement `APIStatus`.** This is critical for transparent swapping. Controller code universally uses `apierrors.IsNotFound(err)` — our errors must satisfy that check without code changes.

4. **Cache is watch-backed.** `storeCache` calls `Store.List` for initial sync, then `Store.Watch` for incremental updates. Reads go to the in-memory cache, not the store. Field indexers work the same as controller-runtime.

5. **Manager stubs K8s-specific features.** Webhooks panic, leader election is always-elected, RESTMapper is empty, EventRecorder logs only. This is intentional — reconkit targets non-K8s backends.

## Working with this code

### Building and testing

Always use the Makefile for validation. `make check` runs all checks (fmt, vet, lint, test with `-race`).

```bash
make check    # run all: fmt, vet, lint, test
make test     # go test -race ./...
make vet      # go vet ./...
make fmt      # check formatting
make lint     # golangci-lint (if installed)
```

Do not run `go test`, `go vet`, or `gofmt` directly — the Makefile targets include the correct flags (e.g., `-race` on tests).

### Adding a new Store backend

1. Create a new package (e.g., `reconkit/sql`)
2. Implement `reconkit.Store` — see `memory/store.go` as reference
3. Pay attention to:
   - Thread safety (Store must be safe for concurrent use)
   - Setting `UID` and `ResourceVersion` on Create (use a global monotonic revision counter, not per-object)
   - Checking `ResourceVersion` on Update (optimistic concurrency)
   - Deep-copying objects before storing (callers may mutate after Create/Update)
   - Watch resumption: check opts for `WatchFromRevision`, replay events from an event log, return `*RevisionTooOldError` if the revision is compacted
   - List must set the list's `ResourceVersion` to the current global revision
   - When a watcher's channel buffer is full, close the watch (not silently drop) so consumers can reconnect and replay
   - Persist the revision counter if the backend survives restarts (see `filesystem/store.go` `.revision` file)
4. Add tests using the same patterns as `example_test.go`

### Modifying controller-runtime interface implementations

The interfaces we implement come from controller-runtime and evolve across versions. When bumping the CR dependency:

1. Run `go build ./...` — the compiler will tell you which methods are missing
2. Check the new interface definitions in `$GOMODCACHE/sigs.k8s.io/controller-runtime@vX.Y.Z/`
3. Add stub methods for new K8s-specific methods (return "not supported" or no-op)
4. The critical interfaces to check: `client.Client`, `client.Writer`, `client.SubResourceWriter`, `cache.Cache`, `cache.Informer`, `manager.Manager`, `cluster.Cluster`, `toolscache.ResourceEventHandlerRegistration`

### Code style

- No comments except when the **why** is non-obvious
- Errors use `fmt.Errorf` for simple cases, named error types for API-boundary errors
- Thread safety via `sync.RWMutex` (reads are frequent, writes are rare)
- JSON round-trip for deep copying between concrete types when type is not known at compile time

## Version compatibility

| Dependency | Version | Notes |
|---|---|---|
| Go | 1.24+ | Uses generics from controller-runtime types |
| controller-runtime | v0.24+ | Interface methods change across versions |
| k8s.io/apimachinery | v0.36+ | Matched to CR version |
| k8s.io/client-go | v0.36+ | Matched to CR version |

## What NOT to do

- Don't add K8s API server dependencies (rest.Config, discovery, etc.) to the Store interface
- Don't add `Patch` or `DeleteAllOf` to Store — those are composed in `client.go` (Apply is different — it's on Store because backend support varies)
- Don't break `apierrors.Is*` compatibility — always test error types implement `APIStatus`
- Don't cache in the Store — that's the cache layer's job
- Don't add leader election tied to K8s — it should be pluggable too (future work)
