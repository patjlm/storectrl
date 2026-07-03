# Feature: Store backend conformance test suite

## Summary

Provide a `storetest` subpackage with a reusable test suite that any Store backend can run to validate its implementation against the Store contract. Backend authors import it and call a single entry point from their own tests.

## Usage

```go
func TestMyBackend(t *testing.T) {
    storetest.TestStore(t, storetest.Config{
        NewStore: func(scheme *runtime.Scheme) storectrl.Store {
            return mybackend.New(scheme, db)
        },
        SupportsApply: false,
        // Per-backend config for optional features
    })
}
```

## Design

### Package: `storetest/`

Self-contained — provides its own test types (embeds `BaseObject`/`BaseList`) so backend authors don't need to define any. Factory pattern: each subtest gets a fresh store via `Config.NewStore`.

### Config struct

Backend authors declare what their backend supports. Tests for unsupported features are skipped, not failed.

```go
type Config struct {
    NewStore      func(*runtime.Scheme) storectrl.Store
    SupportsApply bool
    // Future: SupportsFieldSelectors, SupportsWatchBookmarks, etc.
}
```

### Test coverage

**Core CRUD:**
- Create: sets UID + ResourceVersion, returns `AlreadyExistsError` on duplicate
- Get: retrieves by key, returns `NotFoundError` on missing
- Update: bumps ResourceVersion, returns `ConflictError` on stale RV, returns `NotFoundError` on missing
- Delete: removes object, returns `NotFoundError` on missing
- All errors implement `APIStatus` (`apierrors.IsNotFound`, `apierrors.IsConflict`, etc.)

**List:**
- Lists all objects of a type
- Filters by namespace
- Filters by label selector (`MatchingLabelsSelector`)
- Sets ResourceVersion on list metadata

**Watch:**
- Streams Add/Update/Delete events
- `WatchFromRevision` replays past events
- `RevisionTooOldError` when revision is compacted (if applicable)
- Watch label selector filtering (replay filtered by backends, live filtered by StoreListerWatcher adapter)

**Apply (optional):**
- Basic apply works if `SupportsApply` is true
- Skipped otherwise

**Concurrency/stress:**
- Multiple goroutines creating/updating/deleting concurrently
- Single watcher verifying all events arrive (no lost events)
- Validates thread safety (`-race`)

## Implementation notes

- Use `t.Run` subtests so failures are isolated and easy to identify
- Each subtest calls `NewStore` for isolation
- Provide internal test types: `testWidget`, `testWidgetList` with a `testScheme()` helper
- Pattern: `testing/fstest.TestFS`, `hashicorp/raft` conformance suites

## Existing backends to validate against

- `memory/` — should pass all tests
- `filesystem/` — should pass all tests except Apply

## Severity

Enhancement. Lowers barrier for backend authors and catches contract violations early.
