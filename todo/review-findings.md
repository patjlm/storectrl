# storectrl Review Findings — Index

Full repo review — bugs, missing features, code quality. Prioritized by impact.

## Recommended Priority

1. **F1** — Add pagination before more backends are written
2. **B1-B3** — Fix correctness bugs (small changes, high impact)
3. **Q1-Q2** — Deduplicate backends, fix JSON deep copy
4. **F2** — Add metrics
5. **F5** — Generation tracking
6. **F3** — SQL backend (large effort, validates design)

## Bugs

- [B1](B1-isobjectnamespaced-always-true.md) — `IsObjectNamespaced` always returns `true`
- [B2](B2-strategic-merge-patch-wrong.md) — StrategicMergePatch treated as regular MergePatch
- [B3](B3-status-update-no-subresource.md) — Status update identical to spec update
- [B4](B4-patch-non-atomic.md) — Patch is non-atomic (Get then Update)
- [B5](B5-client-ignores-options.md) — Client silently ignores all options

## Features — High

- [F1](F1-pagination-continue.md) — No pagination/Continue in Store.List
- [F2](F2-metrics-observability.md) — No metrics/observability
- [F3](F3-sql-database-backend.md) — No SQL/database backend
- [F4](F4-context-respect-in-stores.md) — No context.Context respect in stores

## Features — Medium

- [F5](F5-generation-tracking.md) — No generation tracking
- [F6](F6-field-selector-in-store-list.md) — No field selector in Store.List
- [F7](F7-watch-bookmarks-from-backends.md) — No watch bookmarks from backends
- [F8](F8-event-log-not-persisted.md) — Event log not persisted (filesystem)
- [F9](F9-finalizer-deletion-workflow.md) — No finalizer/deletion workflow

## Features — Low

- [F10](F10-apply-implementation.md) — No Apply implementation
- [F11](F11-file-locking.md) — No file locking (filesystem)
- [F12](F12-ttl-expiry.md) — No TTL/expiry
- [F13](F13-ownerreference-gc.md) — No OwnerReference / garbage collection

## Code Quality

- [Q1](Q1-backend-code-duplication.md) — Heavy duplication between backends (~200 lines)
- [Q2](Q2-json-roundtrip-deep-copy.md) — JSON round-trip for deep copy
