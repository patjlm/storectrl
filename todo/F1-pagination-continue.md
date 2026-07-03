# F1. No pagination/Continue in Store.List

**Type:** Feature
**Priority:** High
**Impact:** Breaking interface change if deferred

`Store.List` returns everything in one shot. At 100K+ objects (stated scale goal),
this is a memory bomb.

No Store interface change needed — `Limit` and `Continue` flow through existing
`client.ListOption` types. Changes needed:

- **Backend implementations** honor `Limit` from `client.ListOptions`, set `Continue`
  and `RemainingItemCount` on list's `ListMeta`
- **Cache layer** loops internally during `startInformer`/`relistInformer` until
  `Continue` is empty. Cache reads from in-memory map — no pagination needed there
- **Client layer** already passes through all opts to Store

Backends that don't support pagination return everything and leave `Continue` empty.
