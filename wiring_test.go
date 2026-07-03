package storectrl_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/storectrl"
	"github.com/patjlm/storectrl/memory"
)

// All examples use the same setup: a memory-backed Store with Widget types.
// In production, swap memory.NewStore for your backend (SQL, GCP, filesystem, etc.).

func newTestStore() (*memory.MemoryStore, *runtime.Scheme) {
	scheme := testScheme()
	store := memory.NewStore(scheme)
	return store, scheme
}

// TestWiringClientOnly demonstrates using NewClient standalone.
// NewClient wraps a Store into client.Client for reads + writes.
// Use this when you only need a client (e.g., in tests, scripts, or
// combined with CR's default cache for the read side).
func TestWiringClientOnly(t *testing.T) {
	ctx := context.Background()
	store, scheme := newTestStore()
	c := storectrl.NewClient(store, scheme)

	// Create
	w := newWidget("w1", "blue", 10)
	if err := c.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}
	if w.GetUID() == "" || w.GetResourceVersion() == "" {
		t.Fatal("UID and ResourceVersion should be set after create")
	}

	// Get
	got := &Widget{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Spec.Color != "blue" {
		t.Errorf("expected blue, got %s", got.Spec.Color)
	}

	// Update
	got.Spec.Color = "red"
	if err := c.Update(ctx, got); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Status update
	got.Status.Ready = true
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatalf("status update: %v", err)
	}

	// List
	list := &WidgetList{}
	if err := c.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("expected 1, got %d", len(list.Items))
	}

	// Delete
	if err := c.Delete(ctx, got); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !errors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(w), &Widget{})) {
		t.Error("expected NotFound after delete")
	}
}

// TestWiringCacheOnly demonstrates using NewCache standalone.
// NewCache provides a watch-backed in-memory cache with event handlers
// and field indexers. Writes go directly to the Store; reads come from
// the cache. Use this when you only need the cache side (e.g., combined
// with CR's default client for writes to the API server).
func TestWiringCacheOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, scheme := newTestStore()
	c := storectrl.NewCache(store, scheme)

	// Register informer before starting
	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Write directly to Store (cache is read-only)
	w := newWidget("w1", "blue", 10)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("store create: %v", err)
	}

	// Poll until cache reflects the write
	got := &Widget{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got.Spec.Color != "blue" {
		t.Fatalf("cache never saw the widget")
	}

	// Updates propagate through watch
	w.Spec.Color = "red"
	if err := store.Update(ctx, w); err != nil {
		t.Fatalf("store update: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got = &Widget{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil && got.Spec.Color == "red" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got.Spec.Color != "red" {
		t.Fatal("cache did not reflect update")
	}
}

// TestWiringFullStoreBacked demonstrates the recommended setup:
// NewCache + NewClient, both backed by the same Store.
// This is what you'd wire into ctrl.Options{NewCache, NewClient}.
// Controllers use standard reconciler patterns — no storectrl-specific code.
func TestWiringFullStoreBacked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, scheme := newTestStore()
	c := storectrl.NewClient(store, scheme)
	sc := storectrl.NewCache(store, scheme)

	if _, err := sc.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = sc.Start(ctx) }()
	if !sc.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Write via client
	w := newWidget("w1", "blue", 42)
	if err := c.Create(ctx, w); err != nil {
		t.Fatalf("client create: %v", err)
	}

	// Read via cache (eventually consistent)
	got := &Widget{}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := sc.Get(ctx, client.ObjectKeyFromObject(w), got); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got.Spec.Color != "blue" {
		t.Fatal("cache never saw widget created via client")
	}

	// Reconciler works against both components
	reconciler := &WidgetReconciler{Client: c}
	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(w),
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.RequeueAfter > 0 {
		t.Error("unexpected requeue")
	}

	// Verify reconciler's status update propagated
	updated := &Widget{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(w), updated); err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}
	if !updated.Status.Ready || updated.Status.Phase != "Active" {
		t.Errorf("expected ready=true phase=Active, got ready=%v phase=%s",
			updated.Status.Ready, updated.Status.Phase)
	}
}

// TestWiringStoreListerWatcher demonstrates the StoreListerWatcher adapter.
// It wraps a Store into client-go's ListerWatcher interface, which feeds
// into CR's informer stack. Combine with NewClient for writes.
func TestWiringStoreListerWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, scheme := newTestStore()
	_ = scheme // used in production for scheme registration

	// Pre-populate store
	w1 := newWidget("w1", "blue", 10)
	w2 := newWidget("w2", "red", 20)
	if err := store.Create(ctx, w1); err != nil {
		t.Fatalf("create w1: %v", err)
	}
	if err := store.Create(ctx, w2); err != nil {
		t.Fatalf("create w2: %v", err)
	}

	// Create the adapter
	slw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	// List via adapter — returns runtime.Object
	listObj, err := slw.List(metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	wl := listObj.(*WidgetList)
	if len(wl.Items) != 2 {
		t.Errorf("expected 2, got %d", len(wl.Items))
	}

	// Watch via adapter — returns watch.Interface
	watcher, err := slw.Watch(metav1.ListOptions{
		ResourceVersion: wl.GetResourceVersion(),
	})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer watcher.Stop()

	// Create a new widget — should appear on the watch channel
	w3 := newWidget("w3", "green", 30)
	if err := store.Create(ctx, w3); err != nil {
		t.Fatalf("create w3: %v", err)
	}

	select {
	case evt := <-watcher.ResultChan():
		obj := evt.Object.(*Widget)
		if obj.Name != "w3" {
			t.Errorf("expected w3, got %s", obj.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watch event")
	}
}

// NOTE: StoreListerWatcher + SharedIndexInformer is not tested here because
// client-go v0.36+ uses watchList by default, which requires the backend to
// send bookmark events to signal end of initial events. StoreListerWatcher
// does not currently implement this. The adapter works for direct List/Watch
// usage (see TestWiringStoreListerWatcher) but needs updates for full
// SharedIndexInformer compatibility with newer client-go versions.
