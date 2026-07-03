# Bug: StoreListerWatcher missing bookmark events breaks SharedIndexInformer sync

## Summary

`StoreListerWatcher` does not send bookmark events after replaying initial events. client-go v0.36+ defaults to `watchList` mode in its Reflector, which requires a bookmark event to signal the end of initial events. Without it, `SharedIndexInformer` waits 10+ seconds before falling back to traditional List+Watch — making initial sync unacceptably slow.

## Reproduction

```go
store := memory.NewStore(scheme)
slw := storectrl.NewStoreListerWatcher(store, &WidgetList{})
informer := toolscache.NewSharedIndexInformer(slw, &Widget{}, 0, toolscache.Indexers{})

go informer.Run(ctx.Done())
// This blocks for 10+ seconds instead of syncing immediately:
toolscache.WaitForCacheSync(ctx.Done(), informer.HasSynced)
```

Reflector logs:
```
"Warning: event bookmark expired" err="awaiting required bookmark event for initial events stream, no events received for 10.000666715s"
```

## Root cause

client-go's Reflector in `watchList` mode:
1. Sends a Watch with `SendInitialEvents=true` and `ResourceVersion=""`
2. Expects all existing objects as ADDED events
3. Expects a BOOKMARK event with the current ResourceVersion to signal end of initial events

`StoreListerWatcher.WatchWithContext` translates `ResourceVersion` to `WatchFromRevision` but ignores `SendInitialEvents`. The underlying Store.Watch never sends a bookmark event, so the Reflector times out and retries.

Relevant code in `listerwatcher.go:62-78`:
```go
func (s *StoreListerWatcher) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
    // ...
    // Only translates ResourceVersion — SendInitialEvents, AllowWatchBookmarks are ignored
    if options.ResourceVersion != "" {
        rv, _ := strconv.ParseInt(options.ResourceVersion, 10, 64)
        opts = append(opts, WatchFromRevision(rv))
    }
    watcher, err := s.store.Watch(ctx, listCopy, opts...)
    // ...
}
```

## Fix options

### Option A: Send bookmark in watchAdapter after Store replay completes

After the Store's Watch replays initial events (from `WatchFromRevision(0)` or `WatchFromRevision(rv)`), the `watchAdapter` sends a synthetic BOOKMARK event with the current ResourceVersion. This requires knowing when replay ends and live events begin — the Store/Watcher interface would need to signal this (e.g., via an `EventBookmark` event from the Store itself).

### Option B: Handle `SendInitialEvents` in StoreListerWatcher

When `options.SendInitialEvents` is true:
1. Call `Store.List` to get all objects and the current ResourceVersion
2. Send each object as an ADDED event on the watch channel
3. Send a BOOKMARK event with the list's ResourceVersion
4. Start `Store.Watch` from that ResourceVersion for live events

This is more self-contained — no Store interface changes needed.

### Option C: Let Store backends send bookmarks natively

Add bookmark support to the Store.Watch contract: after replaying events from a revision, backends send an `EventBookmark` with the current revision. The `watchAdapter` already translates `EventBookmark` → `watch.Bookmark`. This would require updating existing backends (memory, filesystem) to send bookmarks.

## Recommendation

Option B is simplest — it's contained within StoreListerWatcher, needs no Store interface changes, and directly addresses the `watchList` protocol. Option C is more correct long-term (backends already define `EventBookmark`) but requires updating all backends.

## Impact

- StoreListerWatcher + SharedIndexInformer has 10+ second sync delay on every startup/reconnect
- Affects any user following the StoreListerWatcher composition pattern from docs/usage.md
- Does not affect NewCache (which doesn't use client-go's Reflector)

## Versions

- client-go v0.36.2 (watchList is default)
- storectrl current main branch
