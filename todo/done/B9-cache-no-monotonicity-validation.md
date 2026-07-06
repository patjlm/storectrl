# B9. Cache has no watch monotonicity validation

**Type:** Bug
**Priority:** Medium
**File:** `cache.go:662-700`

`watchInformer` tracks `lastRV` via max but never validates ordering. If a broken
backend delivers events out of order, the cache silently accepts stale data — could
overwrite a newer object with an older version.

Fix: in `storeInformer.update()`, warn (log) when incoming event's RV is older than
cached object's RV for the same key. Combined with B6 fix (skip stale), this becomes
detection + prevention.
