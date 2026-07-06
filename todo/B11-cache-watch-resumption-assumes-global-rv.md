# B11: cache watch resumption assumes global ResourceVersion

## Problem

`cache.watchInformer` tracks `lastRV = max(object ResourceVersions)` from watch events and passes it to `WatchFromRevision` on reconnect. This works when object RV and the event-log position share the same number line (global counter), but breaks conceptually when they don't.

`docs/backends.md` now allows per-object versioning ("Sharded backends like postgres may use per-object version numbers"), but the cache code was not updated to handle the two-number-line model.

## Current state

The postgres backend is safe by accident: `object_version <= bucket_seq` always holds (bucket_seq counts all writes in the bucket, object_version counts writes to one object). So `WatchFromRevision(max(object_versions))` resumes from an earlier-than-actual position, causing harmless replays caught by the stale-event guard. No events are skipped.

## Risk

A future backend following the docs could implement per-object RV where `object_version > event_log_position` is possible, causing the cache to skip events on reconnect.

## Fix options

1. **Require backends to set List.ResourceVersion to the event-log cursor, not max(object RVs)** — already the case for postgres. Document this explicitly as a contract requirement. The cache should track lastRV from List.RV (which it does during relist) and from bookmark events, but NOT from individual object RVs.
2. **Add bookmark support to backends** — have Watch emit periodic bookmark events carrying the event-log cursor. Cache tracks lastRV from bookmarks only.
3. **Clarify docs** — tighten the per-object versioning docs to state that WatchFromRevision must always be interpretable as an event-log position, not a per-object counter.

Option 3 is the minimum viable fix. Option 1 requires changing how `watchInformer` updates `lastRV`.

## Impact

High design importance, low current risk (postgres is safe, memory/filesystem use global counters).
