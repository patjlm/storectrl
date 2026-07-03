# F8. Event log not persisted (filesystem backend)

**Type:** Feature
**Priority:** Medium

Filesystem store persists objects and revision counter, but event log is in-memory.
Process restart = all watchers must full-relist.
