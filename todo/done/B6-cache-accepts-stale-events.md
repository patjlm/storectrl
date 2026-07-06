# B6. Cache accepts stale/duplicate watch events

**Type:** Bug
**Priority:** High
**File:** `cache.go:1040-1085`

`storeInformer.update()` always enqueues events regardless of whether the object
actually changed. If a backend sends a duplicate Modified event (same or older RV),
the cache stores it and fires handler calls.

`replaceAll()` already does RV comparison for relist (line 1204) but the live watch
path doesn't. Backends with no-op suppression (postgres) mask this, but others
(bridge scenarios, re-appliers) will hammer handlers with content-identical updates.

Fix: compare incoming `GetResourceVersion()` against cached object's RV in `update()`.
Skip if not newer.
