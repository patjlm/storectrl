# F14. Store interface redesign — spec/status split, generation, stronger contracts

**Type:** Feature (breaking redesign)
**Priority:** High
**Supersedes:** B3, F5, F9 (partially), F10

## Motivation

The current Store interface has a single `Update()` that writes the entire object.
This breaks under concurrent spec+status writers (B3), prevents generation tracking (F5),
and makes the Apply method vestigial (F10). Since we have zero legacy consumers,
redesign the interface to enforce correct patterns from day one.

## New Store interface

```go
type Store interface {
    Get(ctx context.Context, key client.ObjectKey, obj client.Object) error
    List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
    Create(ctx context.Context, obj client.Object) error
    Update(ctx context.Context, obj client.Object) error
    UpdateStatus(ctx context.Context, obj client.Object) error
    Delete(ctx context.Context, obj client.Object) error
    Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (Watcher, error)
}
```

### Changes from current interface

- **Added `UpdateStatus`** — mandatory for all backends
- **Removed `Apply`** — move to client wrapper (Get + merge + Update) or drop entirely
- **Tightened `Update` semantics** — writes spec + metadata only, silently preserves stored status

## Method contracts

### Update(ctx, obj)

- Writes spec + metadata from input object
- **Silently preserves stored status** — status fields in the input are ignored
  (not rejected — matches K8s behavior where concurrent status changes don't
  block spec updates)
- Bumps ResourceVersion
- Increments `metadata.generation` when spec content changes (no-op update = no increment)
- Returns ConflictError on stale ResourceVersion
- Returns NotFoundError if object doesn't exist

### UpdateStatus(ctx, obj)

- Writes status from input object
- **Silently preserves stored spec** — spec fields in the input are ignored
- Bumps ResourceVersion
- Does NOT increment `metadata.generation`
- Returns ConflictError on stale ResourceVersion
- Returns NotFoundError if object doesn't exist

### Create(ctx, obj)

- Sets `metadata.generation = 1`
- Sets UID if empty
- Sets ResourceVersion

### Delete(ctx, obj)

- Works with or without ResourceVersion (empty RV = unconditional delete)
- Returns NotFoundError if object doesn't exist

## How backends implement spec/status split

The Store knows which fields are "spec" and "status" by convention: objects have
`Spec` and `Status` struct fields (standard K8s resource shape). Backends use
JSON marshal/unmarshal or reflection to split.

For simple backends (memory, filesystem): on Update, deep-copy input, overwrite
spec+metadata from input but keep status from stored object. On UpdateStatus,
deep-copy input, overwrite status from input but keep spec+metadata from stored.

For postgres: maps to upstream's `WriteObject` (spec) and `WriteStatus` (status),
which are already separate stored proc paths.

## Client wrapper mapping

```
client.Update()          → store.Update()         (spec + metadata)
client.Status().Update() → store.UpdateStatus()   (status only)
client.Patch()           → Get + merge + store.Update()
client.Status().Patch()  → Get + merge + store.UpdateStatus()
```

Apply: Get + merge + Update in client wrapper, or return "not supported."

## Conformance test additions

### Mandatory (no skip flags)

```
TestUpdatePreservesStatus
    Create with status. Update spec only. Get → status unchanged.

TestUpdateStatusPreservesSpec
    Create with spec. UpdateStatus. Get → spec unchanged.

TestUpdateIgnoresInputStatus
    Create. Concurrently UpdateStatus (set status.ready=true).
    Then Update with stale status (ready=false) but new spec.
    Get → status.ready=true (stored value preserved, input ignored).

TestGenerationOnCreate
    Create → generation == 1.

TestGenerationOnSpecChange
    Create. Update with spec change → generation == 2.

TestGenerationUnchangedOnStatusUpdate
    Create. UpdateStatus → generation still 1.

TestGenerationUnchangedOnNoopUpdate
    Create. Update with same spec → generation still 1.

TestConcurrentSpecAndStatusWriters
    10 goroutines updating spec, 10 updating status.
    No ConflictErrors between spec and status paths.
    Final object has last spec writer's spec AND last status writer's status.

TestDeepCopyIsolation
    Create widget. Mutate caller's object. Get → returns original values.

TestDeleteWithoutResourceVersion
    Create. Clear RV on object. Delete succeeds.

TestDeleteWithResourceVersion
    Create. Delete with correct RV succeeds.

TestNoopUpdateSuppressesEvent (moved from optional to mandatory)
    Update with identical content. RV unchanged. No watch event.

TestNoopUpdateStatusSuppressesEvent
    UpdateStatus with identical status. RV unchanged. No watch event.

TestUpdateStatusConflict
    Same OCC pattern as Update — stale RV returns ConflictError.

TestUpdateStatusNotFound
    UpdateStatus on nonexistent object returns NotFoundError.
```

### Remaining skip flags (genuinely backend-dependent)

```go
type Config struct {
    NewStore              func(*runtime.Scheme) storectrl.Store
    NewSmallEventLogStore func(*runtime.Scheme) storectrl.Store

    SkipWatchOverflow              bool  // poll-based backends never overflow
    SkipWatchEventHistory          bool  // poll-based only see latest state
    SkipConcurrencyWatchCount      bool  // consequence of polling
    SkipGlobalRevisionMonotonicity bool  // per-object RV (postgres)
}
```

Removed: `SkipApply` (Apply gone from interface), `SkipNoopSuppression` (now mandatory).

## Impact on backends

| Backend | Work |
|---|---|
| **Memory** | Add UpdateStatus. Split write logic: Update copies spec+metadata from input + keeps stored status. UpdateStatus copies status from input + keeps stored spec. Add generation tracking. ~50-80 lines. |
| **Filesystem** | Same as memory. |
| **Postgres** | Simplifies wrapper — maps directly to upstream's WriteObject / WriteStatus. Generation already tracked upstream. |
| **Future backends** | Clear contract from day one. No ambiguity about what Update vs UpdateStatus does. |

## Impact on existing files

- `store.go` — new interface definition (remove Apply, add UpdateStatus)
- `client.go` — SubResourceWriter routes to store.UpdateStatus() instead of store.Update()
- `cache.go` — no change (watches events regardless of update type)
- `memory/store.go` — implement UpdateStatus, generation tracking, spec/status split
- `filesystem/store.go` — same
- `postgres/store.go` — map UpdateStatus to upstream WriteStatus
- `storetest/storetest.go` — add new tests, remove SkipApply/SkipNoopSuppression
- `errors.go` — no change
- `docs/backends.md` — update contract documentation
- `docs/usage.md` — update component descriptions

## Open questions

- **Finalizer lifecycle (F9)**: should Delete check finalizers and set DeletionTimestamp
  instead of hard-deleting? Or leave that to client wrapper? Separate from this redesign
  but related. Could be a follow-up: if obj has finalizers, Delete sets DeletionTimestamp +
  calls Update (object becomes "dying"), and only hard-deletes when finalizers are empty.
- **How does Store know which fields are spec vs status?** Convention (struct field names)?
  Schema registration? JSON path? Simplest: backends marshal to JSON, split at top-level
  "spec"/"status" keys. Works for all standard resource shapes.
