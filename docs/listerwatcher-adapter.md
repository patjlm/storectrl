# Store-Backed ListerWatcher Adapter

## Idea

Implement a `ListerWatcher` adapter that bridges storectrl's `Store` interface to client-go's `toolscache.ListerWatcher`. This lets CR's standard `SharedIndexInformer` + `Reflector` + `DeltaFIFO` stack drive List/Watch against any Store backend instead of the Kubernetes API server.

## Why

Our current `storeCache`/`storeInformer` reimplements the informer stack from scratch. A ListerWatcher adapter would reuse CR's battle-tested informer infrastructure, giving us:

- **DeltaFIFO** ordering guarantees (coalesces rapid updates, proper event sequencing)
- **Full multi-namespace support** (CR creates one informer per namespace, each with independent config)
- **Reflector** edge-case handling (pagination, watch restart, resource version tracking, backoff)
- **SyncPeriod** via the standard resync mechanism
- **All future CR cache features** automatically

## Interface Gap

```
ListerWatcher (what CR needs):
  List(metav1.ListOptions) -> (runtime.Object, error)
  Watch(metav1.ListOptions) -> (watch.Interface, error)

watch.Interface:
  Stop()
  ResultChan() -> <-chan watch.Event{Type watch.EventType, Object runtime.Object}

Store (what we have):
  List(ctx, list client.ObjectList, ...client.ListOption) -> error
  Watch(ctx, list client.ObjectList, ...client.ListOption) -> (Watcher, error)

Our Watcher:
  Stop()
  ResultChan() -> <-chan Event{Type EventType, Object client.Object}
```

Key differences to bridge:
1. `metav1.ListOptions` (label selector string, field selector string, resource version) → `client.ListOption` (typed selectors, `WatchFromRevision`)
2. `watch.Interface` vs our `Watcher` — both have `Stop()`/`ResultChan()` but different `Event` types (`runtime.Object` vs `client.Object`, `watch.EventType` vs `storectrl.EventType`)
3. CR's ListerWatcher has no context parameter (deprecated path) — `ListerWatcherWithContext` does
4. CR's `List` returns `runtime.Object`; our `Store.List` fills in an `ObjectList` parameter

## Proposed Adapter

```go
package storectrl

import (
    "context"

    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/labels"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/watch"
    toolscache "k8s.io/client-go/tools/cache"
    "sigs.k8s.io/controller-runtime/pkg/client"
)

// StoreListerWatcher adapts a Store into a ListerWatcher for a single GVK.
// It implements both ListerWatcher and ListerWatcherWithContext.
type StoreListerWatcher struct {
    store    Store
    scheme   *runtime.Scheme
    listObj  client.ObjectList  // prototype for creating list instances
    ctx      context.Context    // fallback context for non-context methods
}

func NewStoreListerWatcher(store Store, listObj client.ObjectList, scheme *runtime.Scheme) *StoreListerWatcher {
    return &StoreListerWatcher{
        store:   store,
        scheme:  scheme,
        listObj: listObj,
        ctx:     context.Background(),
    }
}

func (s *StoreListerWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
    return s.ListWithContext(s.ctx, options)
}

func (s *StoreListerWatcher) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
    // Create a fresh list instance
    listCopy := s.listObj.DeepCopyObject().(client.ObjectList)

    // Translate metav1.ListOptions to client.ListOption
    var opts []client.ListOption
    if options.LabelSelector != "" {
        sel, err := labels.Parse(options.LabelSelector)
        if err != nil {
            return nil, err
        }
        opts = append(opts, client.MatchingLabelsSelector{Selector: sel})
    }
    // ResourceVersion is used by Reflector for consistency but our Store
    // doesn't support it on List — Reflector handles this gracefully.

    if err := s.store.List(ctx, listCopy, opts...); err != nil {
        return nil, err
    }
    return listCopy, nil
}

func (s *StoreListerWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
    return s.WatchWithContext(s.ctx, options)
}

func (s *StoreListerWatcher) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
    listCopy := s.listObj.DeepCopyObject().(client.ObjectList)

    var opts []client.ListOption
    if options.ResourceVersion != "" {
        rv, err := strconv.ParseInt(options.ResourceVersion, 10, 64)
        if err == nil {
            opts = append(opts, WatchFromRevision(rv))
        }
    }

    watcher, err := s.store.Watch(ctx, listCopy, opts...)
    if err != nil {
        return nil, err
    }
    return newWatchAdapter(watcher), nil
}

// watchAdapter wraps our Watcher as a watch.Interface.
type watchAdapter struct {
    inner  Watcher
    ch     chan watch.Event
    stopCh chan struct{}
}

func newWatchAdapter(inner Watcher) *watchAdapter {
    a := &watchAdapter{
        inner:  inner,
        ch:     make(chan watch.Event, 100),
        stopCh: make(chan struct{}),
    }
    go a.relay()
    return a
}

func (a *watchAdapter) relay() {
    defer close(a.ch)
    for {
        select {
        case <-a.stopCh:
            return
        case evt, ok := <-a.inner.ResultChan():
            if !ok {
                return
            }
            watchEvt := watch.Event{
                Object: evt.Object,
            }
            switch evt.Type {
            case EventAdded:
                watchEvt.Type = watch.Added
            case EventModified:
                watchEvt.Type = watch.Modified
            case EventDeleted:
                watchEvt.Type = watch.Deleted
            case EventBookmark:
                watchEvt.Type = watch.Bookmark
            }
            select {
            case a.ch <- watchEvt:
            case <-a.stopCh:
                return
            }
        }
    }
}

func (a *watchAdapter) Stop() {
    select {
    case <-a.stopCh:
    default:
        close(a.stopCh)
    }
    a.inner.Stop()
}

func (a *watchAdapter) ResultChan() <-chan watch.Event {
    return a.ch
}
```

## Plugging Into CR's Cache

```go
import (
    "net/http"
    "k8s.io/apimachinery/pkg/api/meta"
    "k8s.io/client-go/rest"
    toolscache "k8s.io/client-go/tools/cache"
    "sigs.k8s.io/controller-runtime/pkg/cache"
)

func NewCRCache(store Store, scheme *runtime.Scheme) (cache.Cache, error) {
    // cache.New requires a non-nil *rest.Config — use a dummy one.
    // Provide HTTPClient and Mapper so it doesn't try to derive them
    // from the config.
    dummyConfig := &rest.Config{}

    return cache.New(dummyConfig, cache.Options{
        Scheme:     scheme,
        HTTPClient: http.DefaultClient,
        Mapper:     meta.NewDefaultRESTMapper([]schema.GroupVersion{}),
        NewInformer: func(lw toolscache.ListerWatcher, obj runtime.Object, d time.Duration, idx toolscache.Indexers) toolscache.SharedIndexInformer {
            // IGNORE the REST-based lw that CR created from dummyConfig.
            // Substitute our Store-backed ListerWatcher.
            listObj := /* create list type from obj's GVK + "List" */
            storeLW := NewStoreListerWatcher(store, listObj, scheme)
            return toolscache.NewSharedIndexInformer(storeLW, obj, d, idx)
        },
    })
}
```

## Open Questions

### 1. Dummy restConfig fragility

`cache.New` calls `rest.CopyConfig(config)` unconditionally. An empty `&rest.Config{}` works today. If CR adds validation or uses restConfig in new ways, this breaks silently. Mitigation: pin CR version, test on upgrade.

### 2. metav1.ListOptions translation completeness

The Reflector passes options like `ResourceVersion`, `ResourceVersionMatch`, `SendInitialEvents`, `AllowWatchBookmarks`, `TimeoutSeconds`, `Limit`, `Continue`. Our Store ignores most of these. Need to verify the Reflector handles unrecognized options gracefully (it should — it's designed for partial server support).

### 3. Error type translation

CR's Reflector expects `apierrors` from List/Watch (e.g., `IsGone` for 410 → triggers relist). Our Store returns `*RevisionTooOldError` which already implements `APIStatus` with 410 status code. Should work, but needs testing.

### 4. Object type in watch.Event

`watch.Event.Object` is `runtime.Object`. Our `Event.Object` is `client.Object` which embeds `runtime.Object`, so assignment works. But the SharedIndexInformer may cast it back to the concrete type — needs verification that our BaseObject-based types survive the round-trip through DeltaFIFO.

### 5. Pagination

CR's Reflector uses `Continue` tokens for paginated lists. Our Store doesn't support pagination. The Reflector should fall back to unpaginated listing when the response has no `Continue` token, but worth verifying.

### 6. Context propagation

`ListerWatcher.List/Watch` (deprecated) has no context. `ListerWatcherWithContext` does. The adapter should implement both. The Reflector prefers the WithContext variant when available.

## Trade-Off Summary

| Aspect | Current storeCache | ListerWatcher Adapter |
|---|---|---|
| CR feature parity | Manual catch-up per feature | Automatic |
| Multi-namespace | Simplified (namespace filter) | Full (per-NS informer + config) |
| DeltaFIFO | No (direct event delivery) | Yes (proper ordering) |
| Event coalescing | No | Yes (DeltaFIFO coalesces rapid updates) |
| K8s coupling | Low | High (dummy restConfig, internal assumptions) |
| Upgrade risk | None | CR internals may change |
| Code ownership | Full control | Adapter + CR internals |
| Debugging | Simple (our code) | Harder (CR internals + adapter) |

## Recommendation

Offer **both** approaches:
1. Keep `storectrl.NewCache` (current) as the default — clean, decoupled, no K8s baggage
2. Add `storectrl.NewStoreListerWatcher` as an opt-in adapter for users who want full CR cache features and accept the coupling

The two are composable: a user could use `NewClient(store, scheme)` with either cache implementation.

## Implementation Plan

1. Add `storectrl/listerwatcher.go` — `StoreListerWatcher` + `watchAdapter`
2. Add `storectrl/listerwatcher_test.go` — test List/Watch translation, event type mapping, ResourceVersion handling
3. Add example showing `cache.New` with `NewInformer` override
4. Document trade-offs in migration.md
