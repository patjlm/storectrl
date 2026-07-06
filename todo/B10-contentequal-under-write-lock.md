# B10: contentEqual runs under exclusive write lock

## Problem

`MemoryStore.Update()` calls `contentEqual()` while holding `s.mu.Lock()`. This does `DeepCopyObject` + 2x `json.Marshal` + `bytes.Equal` under the exclusive lock, blocking all concurrent reads and writes (~50-150us per 10KB object).

The no-op path is worse: `contentEqual` marshals the stored object, discards the bytes, then `copyObject` re-marshals the same stored object.

`FileStore` has the same pattern but disk I/O already dominates its critical section, so the marginal cost is low.

## Fix options

1. **Store content hash alongside objects** — compute hash on write, compare O(1) on update. Adds ~32 bytes per object.
2. **Return marshaled bytes from contentEqual** — reuse on the no-op path to avoid redundant marshal. Couples the API slightly.
3. **Pre-snapshot under read lock** — take a read lock to snapshot the stored object, release, compare outside lock, re-acquire write lock and recheck RV before proceeding. TOCTOU risk requires careful RV guard.

Option 1 is cleanest for the common case (content differs — short-circuit without any marshal).

## Impact

Medium — only matters under high update concurrency with the in-memory backend. Production postgres backend is unaffected (no-op suppression is in the stored procedure).
