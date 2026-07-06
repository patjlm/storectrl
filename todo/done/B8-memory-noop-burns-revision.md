# B8. Memory backend burns revisions on no-op writes

**Type:** Bug
**Priority:** Medium
**File:** `memory/store.go:206-258`

`Update()` always increments `s.revision` and emits a Modified event even when
content is identical to what's already stored. At scale this causes:

- Wasted handler calls (controller re-reconciles for nothing)
- Faster event log compaction (revision counter advances unnecessarily)
- Misleading ResourceVersion (looks like churn when nothing changed)

Fix: compare spec+status content before mutation. If unchanged, return without
incrementing revision or emitting event.
