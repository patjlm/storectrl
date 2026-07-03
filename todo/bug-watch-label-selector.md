# Bug: Store.Watch backends ignore label selector option

## Summary

`WatchWithContext` parses `LabelSelector` from `metav1.ListOptions` and passes `MatchingLabelsSelector` to `Store.Watch`, but neither the memory nor filesystem backend extracts or applies it. Live watch events are unfiltered regardless of selector.

## Impact

In the `watchWithInitialEvents` path (used by client-go v0.36+ Reflector), the initial `List` is correctly filtered by label selector, but the subsequent `Watch` relays all events unfiltered. If a caller uses a label selector, the cache starts with the correct filtered set but then accumulates events for non-matching objects — violating the selector contract.

The normal Watch path (non-SendInitialEvents) has the same gap: selector is parsed and passed through, but the backend ignores it.

## Reproduction

```go
store := memory.NewStore(scheme)
// Create objects with different labels
blue := newWidget("w1", "blue", 1)
blue.SetLabels(map[string]string{"color": "blue"})
store.Create(ctx, blue)

red := newWidget("w2", "red", 2)
red.SetLabels(map[string]string{"color": "red"})
store.Create(ctx, red)

slw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

sendInitial := true
watcher, _ := slw.WatchWithContext(ctx, metav1.ListOptions{
    LabelSelector:     "color=blue",
    SendInitialEvents: &sendInitial,
})

// First event: ADDED for blue widget (correct, from filtered List)
// Bookmark event (correct)
// Then create another red widget:
red2 := newWidget("w3", "red", 3)
red2.SetLabels(map[string]string{"color": "red"})
store.Create(ctx, red2)
// Watch receives ADDED for red2 — should have been filtered out
```

## Root cause

`listerwatcher.go:69-73` parses the label selector and appends `MatchingLabelsSelector` to the options passed to `Store.Watch`. But:

- `memory/store.go:315-320` — Watch only checks for `WatchFromRevision`, ignores all other options
- `filesystem/store.go` — same behavior

The adapter correctly passes the option through. The gap is in the backends.

## Fix options

### Option A: Filter in backends

Add `MatchingLabelsSelector` extraction to `MemoryStore.Watch` and `FileStore.Watch`. Filter replayed events and live events by selector before sending to the watcher channel.

Pros: correct at the source, all consumers benefit.
Cons: every backend must implement filtering.

### Option B: Filter in the adapter

Add label filtering in `watchAdapter.relay()` and `initialEventsAdapter.run()` — check each event's labels against the parsed selector before forwarding.

Pros: backend-agnostic, single implementation point.
Cons: unnecessary event traffic from store to adapter.

### Option C: Filter in backends for replay, adapter for live

Backends filter replay events (they already iterate the event log). Live events are filtered by the watcher itself (match labels before sending to channel).

## Recommendation

Option A is most consistent with how `Store.List` already handles `MatchingLabelsSelector`. The backends iterate options for `WatchFromRevision` already — adding `MatchingLabelsSelector` is straightforward.

## Severity

Low-medium. Only affects users combining StoreListerWatcher with label selectors. Standard usage (no selector) is unaffected. The initial sync is always correct; the issue is only with live events after sync.
