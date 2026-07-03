# storectrl — Agent Guide

Instructions for AI agents working on this codebase.

## What this is

storectrl is a Go component library that provides `client.Client` and `cache.Cache` implementations backed by a pluggable `Store` interface instead of the Kubernetes API server. It lets developers write controllers using the standard reconciler pattern against any datastore, wired into controller-runtime's standard `manager.Manager` via factory overrides.

Must scale from small development setups to production workloads with large object counts (100K+ objects). Performance-sensitive areas: relist after watch reconnection, handler call overhead under high event rates, and cache memory footprint with remote backends.

## Module layout

```
storectrl/                 # Main package — all public API
├── store.go              # Store interface, WatchFromRevision option
├── watcher.go            # Watcher, Event, EventType (incl. EventBookmark)
├── errors.go             # NotFoundError, AlreadyExistsError, ConflictError, RevisionTooOldError
├── object.go             # BaseObject, BaseList (embed for client.Object compat)
├── client.go             # client.Client implementation wrapping Store
├── cache.go              # cache.Cache implementation (watch-backed, async event queue, CacheOption config)
├── listerwatcher.go      # StoreListerWatcher adapter (Store → client-go ListerWatcher)
├── example_test.go       # CRUD, concurrency, and reconciler tests
├── memory/               # In-memory Store backend
│   ├── store.go           # MemoryStore — global revision, event log, watch resumption
│   └── watcher.go         # memoryWatcher — overflow-closes-watch for safe reconnect
└── filesystem/            # Filesystem Store backend
    ├── store.go           # FileStore — JSON files, persisted revision counter
    └── watcher.go         # fileWatcher — same overflow pattern as memory
```

User-facing docs in `docs/`:
- [usage.md](docs/usage.md) — components vs CR, compositions, cache options
- [backends.md](docs/backends.md) — Store interface, error contracts, watch resumption
- [migration.md](docs/migration.md) — before/after comparison, type compatibility
- [wiring_test.go](wiring_test.go) — runnable examples for each component and composition

## Design decisions

1. **Reuse controller-runtime types, not reinvent them.** We import `client.Object`, `client.ObjectKey`, `cache.Cache`, etc. directly. Goal: minimal code change for controller authors.

2. **Store is deliberately simple.** Mirrors `client.Reader` + `client.Writer` without Patch, DeleteAllOf, or SubResources. The `client.Client` wrapper composes those (Patch = Get + apply + Update, DeleteAllOf = List + Delete each, Status = just Update). Apply is on Store directly because support varies by backend.

3. **Errors implement `APIStatus`.** Critical for transparent swapping — `apierrors.IsNotFound(err)` must work without code changes.

4. **Cache is watch-backed with async event processing.** List for initial sync, Watch for incremental updates. Reads go to in-memory cache, not store. Two scalability features:
   - **Async event queue** — per-key coalescing queue decouples handler calls from watch goroutine. Prevents slow handlers from blocking watch channel and triggering overflow→relist cascades. Coalescing rules: Add+Update→Add, Add+Delete→cancel, Update+Update→merge, Update+Delete→Delete, Delete+Add→preserve both. Initial snapshot delivered synchronously to new handlers.
   - **Smart relist diff** — On relist (`RevisionTooOldError`), diffs old vs new by ResourceVersion. Unchanged objects produce no handler call. Reduces relist cost from O(N) to O(changed).

5. **Component library, not a manager replacement.** Provides `NewCache` and `NewClient` factory functions wired via `manager.Options`. Manager lifecycle, leader election, health probes, controller builder stay with controller-runtime.

6. **Cache options match controller-runtime where applicable.** Functional options mirror `cache.Options`. See [docs/usage.md](docs/usage.md) for full list. Not supported (K8s-specific): `HTTPClient`, `Mapper`, `NewInformer`, `WatchErrorHandler`.

## Building and testing

Always use the Makefile. Do not run `go test`, `go vet`, or `gofmt` directly.

```bash
make check    # run all: fmt, vet, lint, test
make test     # go test -race ./...
make vet      # go vet ./...
make fmt      # check formatting
make lint     # golangci-lint (if installed)
```

## Modifying controller-runtime interface implementations

When bumping the CR dependency:

1. Run `go build ./...` — compiler tells you which methods are missing
2. Check new interface definitions in `$GOMODCACHE/sigs.k8s.io/controller-runtime@vX.Y.Z/`
3. Add stub methods for new K8s-specific methods (return "not supported" or no-op)
4. Critical interfaces: `client.Client`, `client.Writer`, `client.SubResourceWriter`, `cache.Cache`, `cache.Informer`, `toolscache.ResourceEventHandlerRegistration`

## Code style

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
- Don't add a custom Manager, Builder, or Source — those belong to controller-runtime
