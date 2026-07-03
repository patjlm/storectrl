package storectrl_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	stderrors "errors"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/storectrl"
	"github.com/patjlm/storectrl/memory"
)

type Widget struct {
	storectrl.BaseObject `json:",inline"`
	Spec                 WidgetSpec   `json:"spec"`
	Status               WidgetStatus `json:"status"`
}

type WidgetSpec struct {
	Color string `json:"color"`
	Size  int    `json:"size"`
}

type WidgetStatus struct {
	Ready bool   `json:"ready"`
	Phase string `json:"phase"`
}

func (w *Widget) DeepCopyObject() runtime.Object {
	if w == nil {
		return nil
	}
	out := &Widget{}
	w.BaseObject.DeepCopyInto(&out.BaseObject)
	out.Spec = w.Spec
	out.Status = w.Status
	return out
}

type WidgetList struct {
	storectrl.BaseList `json:",inline"`
	Items              []Widget `json:"items"`
}

func (w *WidgetList) DeepCopyObject() runtime.Object {
	if w == nil {
		return nil
	}
	out := &WidgetList{}
	w.BaseList.DeepCopyInto(&out.BaseList)
	if w.Items != nil {
		out.Items = make([]Widget, len(w.Items))
		for i := range w.Items {
			out.Items[i] = *w.Items[i].DeepCopyObject().(*Widget)
		}
	}
	return out
}

type Gadget struct {
	storectrl.BaseObject `json:",inline"`
}

func (g *Gadget) DeepCopyObject() runtime.Object {
	if g == nil {
		return nil
	}
	out := &Gadget{}
	g.BaseObject.DeepCopyInto(&out.BaseObject)
	return out
}

type GadgetList struct {
	storectrl.BaseList `json:",inline"`
	Items              []Gadget `json:"items"`
}

func (g *GadgetList) DeepCopyObject() runtime.Object {
	if g == nil {
		return nil
	}
	out := &GadgetList{}
	g.BaseList.DeepCopyInto(&out.BaseList)
	if g.Items != nil {
		out.Items = make([]Gadget, len(g.Items))
		for i := range g.Items {
			out.Items[i] = *g.Items[i].DeepCopyObject().(*Gadget)
		}
	}
	return out
}

var gadgetGV = schema.GroupVersion{Group: "gadget.storectrl.dev", Version: "v1"}
var widgetGV = schema.GroupVersion{Group: "example.storectrl.dev", Version: "v1"}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypes(widgetGV, &Widget{}, &WidgetList{})
	metav1.AddToGroupVersion(s, widgetGV)
	return s
}

func newWidget(name string, color string, size int) *Widget {
	w := &Widget{
		Spec: WidgetSpec{Color: color, Size: size},
	}
	w.Name = name
	w.Namespace = "default"
	w.APIVersion = widgetGV.String()
	w.Kind = "Widget"
	return w
}

func TestWidgetCRUD(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewClient(store, scheme)

	widget := newWidget("my-widget", "blue", 42)

	if err := c.Create(ctx, widget); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if widget.GetUID() == "" {
		t.Error("UID not set after create")
	}
	if widget.GetResourceVersion() == "" {
		t.Error("ResourceVersion not set after create")
	}
	originalRV := widget.GetResourceVersion()

	// Get
	retrieved := &Widget{}
	key := client.ObjectKey{Namespace: "default", Name: "my-widget"}
	if err := c.Get(ctx, key, retrieved); err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if retrieved.Spec.Color != "blue" || retrieved.Spec.Size != 42 {
		t.Errorf("wrong spec: color=%s size=%d", retrieved.Spec.Color, retrieved.Spec.Size)
	}

	// Update
	retrieved.Spec.Color = "red"
	retrieved.Status.Ready = true
	if err := c.Update(ctx, retrieved); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if retrieved.GetResourceVersion() == originalRV {
		t.Error("ResourceVersion should change after update")
	}

	// List
	list := &WidgetList{}
	if err := c.List(ctx, list, client.InNamespace("default")); err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("expected 1 widget, got %d", len(list.Items))
	}

	// Create second widget
	widget2 := newWidget("another-widget", "green", 100)
	if err := c.Create(ctx, widget2); err != nil {
		t.Fatalf("create second failed: %v", err)
	}

	list = &WidgetList{}
	if err := c.List(ctx, list, client.InNamespace("default")); err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(list.Items) != 2 {
		t.Errorf("expected 2 widgets, got %d", len(list.Items))
	}

	// Delete
	if err := c.Delete(ctx, widget); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	err := c.Get(ctx, key, &Widget{})
	if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound after delete, got: %v", err)
	}
}

func TestOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewClient(store, scheme)

	widget := newWidget("concurrent-widget", "blue", 10)
	if err := c.Create(ctx, widget); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reader1, reader2 := &Widget{}, &Widget{}
	key := client.ObjectKeyFromObject(widget)

	if err := c.Get(ctx, key, reader1); err != nil {
		t.Fatalf("get reader1: %v", err)
	}
	if err := c.Get(ctx, key, reader2); err != nil {
		t.Fatalf("get reader2: %v", err)
	}

	// Reader1 succeeds
	reader1.Spec.Size = 20
	if err := c.Update(ctx, reader1); err != nil {
		t.Fatalf("reader1 update: %v", err)
	}

	// Reader2 gets conflict (stale RV)
	reader2.Spec.Size = 30
	err := c.Update(ctx, reader2)
	if !errors.IsConflict(err) {
		t.Errorf("expected conflict for reader2, got: %v", err)
	}

	// Re-fetch and retry
	if err := c.Get(ctx, key, reader2); err != nil {
		t.Fatalf("re-fetch reader2: %v", err)
	}
	reader2.Spec.Size = 30
	if err := c.Update(ctx, reader2); err != nil {
		t.Fatalf("reader2 retry: %v", err)
	}

	final := &Widget{}
	if err := c.Get(ctx, key, final); err != nil {
		t.Fatalf("get final: %v", err)
	}
	if final.Spec.Size != 30 {
		t.Errorf("expected size=30, got %d", final.Spec.Size)
	}
}

type WidgetReconciler struct {
	client.Client
}

func (r *WidgetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	widget := &Widget{}
	if err := r.Get(ctx, req.NamespacedName, widget); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	widget.Status.Ready = widget.Spec.Size > 0
	if widget.Status.Ready {
		widget.Status.Phase = "Active"
	} else {
		widget.Status.Phase = "Pending"
	}

	if err := r.Update(ctx, widget); err != nil {
		if errors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func TestWidgetReconciler(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewClient(store, scheme)

	widget := newWidget("test-widget", "blue", 42)
	if err := c.Create(ctx, widget); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	reconciler := &WidgetReconciler{Client: c}
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "default", Name: "test-widget"}}

	result, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if result.RequeueAfter > 0 {
		t.Error("unexpected requeue")
	}

	updated := &Widget{}
	if err := c.Get(ctx, req.NamespacedName, updated); err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}
	if !updated.Status.Ready {
		t.Error("expected ready=true")
	}
	if updated.Status.Phase != "Active" {
		t.Errorf("expected phase=Active, got %s", updated.Status.Phase)
	}
}

func TestWatchResume(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(100))

	// Create widgets — each gets a unique global revision
	w1 := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w1); err != nil {
		t.Fatalf("create w1: %v", err)
	}
	w2 := newWidget("w2", "red", 2)
	if err := store.Create(ctx, w2); err != nil {
		t.Fatalf("create w2: %v", err)
	}

	// Watch from revision 0 — should replay both creates
	watcher, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch from 0: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != storectrl.EventAdded {
				t.Errorf("replay %d: expected ADDED, got %s", i, evt.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for replay event %d", i)
		}
	}

	// Create a third widget — should arrive as live event
	w3 := newWidget("w3", "green", 3)
	if err := store.Create(ctx, w3); err != nil {
		t.Fatalf("create w3: %v", err)
	}

	select {
	case evt := <-watcher.ResultChan():
		if evt.Type != storectrl.EventAdded || evt.Object.GetName() != "w3" {
			t.Errorf("expected ADDED w3, got %s %s", evt.Type, evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for live event")
	}
	watcher.Stop()

	// Resume from revision 2 (after w2 create) — should replay only w3
	watcher2, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(2))
	if err != nil {
		t.Fatalf("watch from 2: %v", err)
	}

	select {
	case evt := <-watcher2.ResultChan():
		if evt.Type != storectrl.EventAdded || evt.Object.GetName() != "w3" {
			t.Errorf("expected ADDED w3, got %s %s", evt.Type, evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for resumed event")
	}
	watcher2.Stop()
}

func TestWatchResumeNoGap(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(100))

	// Create initial widget
	w1 := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w1); err != nil {
		t.Fatalf("create w1: %v", err)
	}

	// Resume watch from revision 1 (w1's create)
	watcher, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(1))
	if err != nil {
		t.Fatalf("watch from 1: %v", err)
	}

	// Create w2 after watch starts — this should arrive as live event
	w2 := newWidget("w2", "red", 2)
	if err := store.Create(ctx, w2); err != nil {
		t.Fatalf("create w2: %v", err)
	}

	select {
	case evt := <-watcher.ResultChan():
		if evt.Object.GetName() != "w2" {
			t.Errorf("expected w2, got %s", evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout — missed live event after resume")
	}
	watcher.Stop()
}

func TestWatchRevisionTooOld(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(5))

	// Create 10 widgets — log only retains last 5
	for i := 0; i < 10; i++ {
		w := newWidget(fmt.Sprintf("w%d", i), "blue", i)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create w%d: %v", i, err)
		}
	}

	// Revision 1 is gone (log has revisions 6-10)
	_, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(1))
	var rvErr *storectrl.RevisionTooOldError
	if !stderrors.As(err, &rvErr) {
		t.Fatalf("expected RevisionTooOldError, got %v", err)
	}
	if rvErr.RequestedRevision != 1 {
		t.Errorf("expected requested=1, got %d", rvErr.RequestedRevision)
	}
	if !errors.IsGone(err) {
		t.Error("RevisionTooOldError should satisfy apierrors.IsGone()")
	}

	// Revision 5 should still work (oldest in log is 6, and 5+1 >= 6)
	watcher, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(5))
	if err != nil {
		t.Fatalf("watch from 5 should work: %v", err)
	}
	// Should get 5 replay events (revisions 6-10)
	for i := 0; i < 5; i++ {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != storectrl.EventAdded {
				t.Errorf("event %d: expected ADDED, got %s", i, evt.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout at replay event %d", i)
		}
	}
	watcher.Stop()
}

func TestWatchOverflowClosesWatch(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(1000))

	// Start a watch — channel buffer is 100
	watcher, err := store.Watch(ctx, &WidgetList{})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Create 200 widgets without reading — should overflow
	for i := 0; i < 200; i++ {
		w := newWidget(fmt.Sprintf("overflow-%d", i), "blue", i)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	// Drain remaining buffered events, then channel should close
	count := 0
	for range watcher.ResultChan() {
		count++
	}
	// Channel closed — consumer got some events then EOF
	if count == 0 {
		t.Error("expected at least some events before close")
	}
	if count >= 200 {
		t.Error("expected channel to close before all 200 events")
	}

	// Can resume from last seen revision using event log
	watcher2, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(int64(count)))
	if err != nil {
		t.Fatalf("resume after overflow: %v", err)
	}
	watcher2.Stop()
}

func TestWatchResumeWithMixedEventTypes(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(100))

	w := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}
	w.Spec.Color = "red"
	if err := store.Update(ctx, w); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := store.Delete(ctx, w); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Replay all 3 events
	watcher, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	expected := []storectrl.EventType{storectrl.EventAdded, storectrl.EventModified, storectrl.EventDeleted}
	for i, want := range expected {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != want {
				t.Errorf("event %d: expected %s, got %s", i, want, evt.Type)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout at event %d", i)
		}
	}
	watcher.Stop()
}

func TestWatchResumeFiltersGVK(t *testing.T) {
	scheme := testScheme()
	scheme.AddKnownTypes(gadgetGV, &Gadget{}, &GadgetList{})
	metav1.AddToGroupVersion(scheme, gadgetGV)

	ctx := context.Background()
	store := memory.NewStore(scheme, memory.WithEventLogSize(100))

	// Create one Widget and one Gadget — interleaved in event log
	w := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create widget: %v", err)
	}
	g := &Gadget{}
	g.Name = "g1"
	g.Namespace = "default"
	g.APIVersion = "gadget.storectrl.dev/v1"
	g.Kind = "Gadget"
	if err := store.Create(ctx, g); err != nil {
		t.Fatalf("create gadget: %v", err)
	}

	// Watch Widgets from revision 0 — should only see Widget, not Gadget
	watcher, err := store.Watch(ctx, &WidgetList{}, storectrl.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	select {
	case evt := <-watcher.ResultChan():
		if evt.Object.GetName() != "w1" {
			t.Errorf("expected w1, got %s", evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for widget event")
	}

	// No more events should be buffered (gadget was filtered)
	select {
	case evt := <-watcher.ResultChan():
		t.Errorf("unexpected event: %s %s", evt.Type, evt.Object.GetName())
	case <-time.After(50 * time.Millisecond):
		// Expected — no gadget event
	}
	watcher.Stop()
}

func TestListResourceVersion(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme)

	// Empty list should have revision 0
	list := &WidgetList{}
	if err := store.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}
	if list.GetResourceVersion() != "0" {
		t.Errorf("expected RV=0 on empty list, got %s", list.GetResourceVersion())
	}

	// After a create, list RV should advance
	w := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	list = &WidgetList{}
	if err := store.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}
	if list.GetResourceVersion() != "1" {
		t.Errorf("expected RV=1, got %s", list.GetResourceVersion())
	}
}

func TestCacheGetAndList(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewCache(store, scheme)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w1 := newWidget("w1", "blue", 10)
	w2 := newWidget("w2", "red", 20)
	if err := store.Create(ctx, w1); err != nil {
		t.Fatalf("create w1: %v", err)
	}
	if err := store.Create(ctx, w2); err != nil {
		t.Fatalf("create w2: %v", err)
	}

	// Poll until cache sees both objects
	deadline := time.Now().Add(2 * time.Second)
	var list *WidgetList
	for time.Now().Before(deadline) {
		list = &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 widgets in cache, got %d", len(list.Items))
	}

	got := &Widget{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err != nil {
		t.Fatalf("get w1: %v", err)
	}
	if got.Spec.Color != "blue" || got.Spec.Size != 10 {
		t.Errorf("wrong w1 spec: color=%s size=%d", got.Spec.Color, got.Spec.Size)
	}

	got = &Widget{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w2"}, got); err != nil {
		t.Fatalf("get w2: %v", err)
	}
	if got.Spec.Color != "red" || got.Spec.Size != 20 {
		t.Errorf("wrong w2 spec: color=%s size=%d", got.Spec.Color, got.Spec.Size)
	}
}

func TestCacheWatchUpdates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewCache(store, scheme)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w := newWidget("w1", "blue", 10)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Poll until cache sees the object
	deadline := time.Now().Add(2 * time.Second)
	got := &Widget{}
	for time.Now().Before(deadline) {
		got = &Widget{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got.Spec.Color != "blue" {
		t.Fatal("cache never saw created widget")
	}

	w.Spec.Color = "red"
	w.Spec.Size = 99
	if err := store.Update(ctx, w); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Poll until cache reflects the update
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got = &Widget{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil && got.Spec.Color == "red" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got.Spec.Color != "red" || got.Spec.Size != 99 {
		t.Fatalf("cache did not reflect update: color=%s size=%d", got.Spec.Color, got.Spec.Size)
	}

	if err := store.Delete(ctx, w); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Poll until cache reflects the deletion
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{})) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !errors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{})) {
		t.Error("cache did not reflect deletion")
	}
}

func TestCacheFieldIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewCache(store, scheme)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	if err := c.IndexField(ctx, &Widget{}, ".spec.color", func(obj client.Object) []string {
		return []string{obj.(*Widget).Spec.Color}
	}); err != nil {
		t.Fatalf("index field: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	for _, w := range []*Widget{
		newWidget("w-blue-1", "blue", 1),
		newWidget("w-blue-2", "blue", 2),
		newWidget("w-red-1", "red", 3),
		newWidget("w-green-1", "green", 4),
	} {
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create %s: %v", w.Name, err)
		}
	}

	// Poll until all 4 widgets appear in the cache
	deadline := time.Now().Add(2 * time.Second)
	var all *WidgetList
	for time.Now().Before(deadline) {
		all = &WidgetList{}
		if err := c.List(ctx, all); err == nil && len(all.Items) == 4 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(all.Items) != 4 {
		t.Fatalf("expected 4 widgets, got %d", len(all.Items))
	}

	blues := &WidgetList{}
	if err := c.List(ctx, blues, client.MatchingFields{".spec.color": "blue"}); err != nil {
		t.Fatalf("list by color blue: %v", err)
	}
	if len(blues.Items) != 2 {
		t.Errorf("expected 2 blue widgets, got %d", len(blues.Items))
	}
	for _, w := range blues.Items {
		if w.Spec.Color != "blue" {
			t.Errorf("expected blue, got %s", w.Spec.Color)
		}
	}

	reds := &WidgetList{}
	if err := c.List(ctx, reds, client.MatchingFields{".spec.color": "red"}); err != nil {
		t.Fatalf("list by color red: %v", err)
	}
	if len(reds.Items) != 1 {
		t.Errorf("expected 1 red widget, got %d", len(reds.Items))
	}

	purples := &WidgetList{}
	if err := c.List(ctx, purples, client.MatchingFields{".spec.color": "purple"}); err != nil {
		t.Fatalf("list by color purple: %v", err)
	}
	if len(purples.Items) != 0 {
		t.Errorf("expected 0 purple widgets, got %d", len(purples.Items))
	}
}

func TestClientPatch(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewClient(store, scheme)

	widget := newWidget("patch-widget", "blue", 10)
	if err := c.Create(ctx, widget); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	base := widget.DeepCopyObject().(client.Object)
	widget.Spec.Color = "green"
	if err := c.Patch(ctx, widget, client.MergeFrom(base)); err != nil {
		t.Fatalf("merge patch failed: %v", err)
	}
	got := &Widget{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(widget), got); err != nil {
		t.Fatalf("get after merge patch: %v", err)
	}
	if got.Spec.Color != "green" {
		t.Errorf("expected color=green after merge patch, got %s", got.Spec.Color)
	}
	if got.Spec.Size != 10 {
		t.Errorf("expected size=10 preserved after merge patch, got %d", got.Spec.Size)
	}

	jsonPatch := `[{"op":"replace","path":"/spec/size","value":99}]`
	if err := c.Patch(ctx, widget, client.RawPatch(types.JSONPatchType, []byte(jsonPatch))); err != nil {
		t.Fatalf("json patch failed: %v", err)
	}
	got = &Widget{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(widget), got); err != nil {
		t.Fatalf("get after json patch: %v", err)
	}
	if got.Spec.Size != 99 {
		t.Errorf("expected size=99 after json patch, got %d", got.Spec.Size)
	}

	err := c.Patch(ctx, widget, client.RawPatch(types.ApplyPatchType, []byte("{}")))
	if err == nil || !strings.Contains(err.Error(), "unsupported patch type") {
		t.Errorf("expected error containing 'unsupported patch type', got: %v", err)
	}

	ghost := newWidget("ghost", "purple", 1)
	err = c.Patch(ctx, ghost, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"color":"yellow"}}`)))
	if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound for nonexistent patch, got: %v", err)
	}
}

func TestCacheRelistRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(5))
	c := storectrl.NewCache(store, scheme)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w1 := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w1); err != nil {
		t.Fatalf("create w1: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := &Widget{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{}); err != nil {
		t.Fatalf("cache never saw w1: %v", err)
	}

	for i := 2; i <= 11; i++ {
		w := newWidget(fmt.Sprintf("w%d", i), "red", i)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create w%d: %v", i, err)
		}
	}

	deadline = time.Now().Add(5 * time.Second)
	var list *WidgetList
	for time.Now().Before(deadline) {
		list = &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) == 11 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if list == nil || len(list.Items) != 11 {
		count := 0
		if list != nil {
			count = len(list.Items)
		}
		t.Fatalf("expected 11 widgets in cache after relist recovery, got %d", count)
	}

	got := &Widget{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w7"}, got); err != nil {
		t.Fatalf("get w7 from cache: %v", err)
	}
	if got.Spec.Color != "red" || got.Spec.Size != 7 {
		t.Errorf("wrong w7 spec: color=%s size=%d", got.Spec.Color, got.Spec.Size)
	}
}

func TestCacheWithDefaultTransform(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	// Transform that zeroes Size field
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultTransform(func(in any) (any, error) {
			if w, ok := in.(*Widget); ok {
				w.Spec.Size = 0
			}
			return in, nil
		}),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w := newWidget("w1", "blue", 42)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	got := &Widget{}
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got.Spec.Size != 0 {
		t.Errorf("expected size=0 after transform, got %d", got.Spec.Size)
	}
	if got.Spec.Color != "blue" {
		t.Errorf("expected color=blue preserved, got %s", got.Spec.Color)
	}
}

func TestCacheWithTransformStripManagedFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultTransform(storectrl.TransformStripManagedFields()),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w := newWidget("w1", "blue", 1)
	w.ManagedFields = []metav1.ManagedFieldsEntry{
		{Manager: "test", Operation: metav1.ManagedFieldsOperationApply},
	}
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	got := &Widget{}
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got.ManagedFields != nil {
		t.Errorf("expected managedFields stripped, got %v", got.ManagedFields)
	}
}

func TestCacheWithUnsafeDisableDeepCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultUnsafeDisableDeepCopy(true),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w := newWidget("w1", "blue", 10)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	got1 := &Widget{}
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got1); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got2 := &Widget{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got2); err != nil {
		t.Fatalf("get2: %v", err)
	}

	// With UnsafeDisableDeepCopy, both should reference same underlying data
	// Mutating got1 should affect got2 on next read
	if got1.Spec.Color != "blue" {
		t.Errorf("expected blue, got %s", got1.Spec.Color)
	}

	// List should also return without deep copy
	list := &WidgetList{}
	if err := c.List(ctx, list); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 widget, got %d", len(list.Items))
	}
}

func TestCacheWithLabelSelector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	sel := labels.SelectorFromSet(labels.Set{"env": "prod"})
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultLabelSelector(sel),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Create widget with matching label
	wProd := newWidget("w-prod", "blue", 1)
	wProd.Labels = map[string]string{"env": "prod"}
	if err := store.Create(ctx, wProd); err != nil {
		t.Fatalf("create prod: %v", err)
	}

	// Create widget without matching label
	wDev := newWidget("w-dev", "red", 2)
	wDev.Labels = map[string]string{"env": "dev"}
	if err := store.Create(ctx, wDev); err != nil {
		t.Fatalf("create dev: %v", err)
	}

	// Wait for cache to process events
	deadline := time.Now().Add(2 * time.Second)
	var list *WidgetList
	for time.Now().Before(deadline) {
		list = &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Only prod widget should be cached
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 widget (prod only), got %d", len(list.Items))
	}
	if list.Items[0].Name != "w-prod" {
		t.Errorf("expected w-prod, got %s", list.Items[0].Name)
	}

	// Dev widget should not be found
	err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w-dev"}, &Widget{})
	if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound for w-dev, got: %v", err)
	}
}

func TestCacheWithByObject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	// Default transform zeroes size, but per-object transform for Widget zeroes color
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultTransform(func(in any) (any, error) {
			if w, ok := in.(*Widget); ok {
				w.Spec.Size = 0
			}
			return in, nil
		}),
		storectrl.WithByObject(&Widget{}, storectrl.ByObjectConfig{
			Transform: func(in any) (any, error) {
				if w, ok := in.(*Widget); ok {
					w.Spec.Color = ""
				}
				return in, nil
			},
		}),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w := newWidget("w1", "blue", 42)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	got := &Widget{}
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Per-object transform should have zeroed color, NOT size
	if got.Spec.Color != "" {
		t.Errorf("expected empty color from per-object transform, got %s", got.Spec.Color)
	}
	if got.Spec.Size != 42 {
		t.Errorf("expected size=42 preserved (default transform overridden), got %d", got.Spec.Size)
	}
}

func TestCacheReaderFailOnMissingInformer(t *testing.T) {
	scheme := testScheme()
	store := memory.NewStore(scheme)

	t.Run("default behavior fails on missing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := storectrl.NewCache(store, scheme)
		go func() { _ = c.Start(ctx) }()

		err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{})
		if err == nil {
			t.Fatal("expected error for unregistered type")
		}
		if !strings.Contains(err.Error(), "no informer registered") {
			t.Errorf("expected 'no informer registered' error, got: %v", err)
		}
	})

	t.Run("auto-create when disabled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := storectrl.NewCache(store, scheme,
			storectrl.WithReaderFailOnMissingInformer(false),
		)
		go func() { _ = c.Start(ctx) }()
		time.Sleep(50 * time.Millisecond)

		w := newWidget("auto-w1", "green", 5)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Get should auto-create informer and eventually find the object
		deadline := time.Now().Add(2 * time.Second)
		got := &Widget{}
		var lastErr error
		for time.Now().Before(deadline) {
			lastErr = c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "auto-w1"}, got)
			if lastErr == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if lastErr != nil {
			t.Fatalf("expected auto-created informer to find object, got: %v", lastErr)
		}
		if got.Spec.Color != "green" {
			t.Errorf("expected green, got %s", got.Spec.Color)
		}
	})
}

func TestCacheSyncPeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)
	c := storectrl.NewCache(store, scheme,
		storectrl.WithSyncPeriod(100*time.Millisecond),
	)

	informer, err := c.GetInformer(ctx, &Widget{})
	if err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	w := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Wait for cache to see the object
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{}); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Register an event handler and count update events
	var updateCount atomic.Int32
	_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			updateCount.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("add event handler: %v", err)
	}

	// Wait for at least 2 sync period ticks to fire synthetic updates
	time.Sleep(350 * time.Millisecond)

	count := updateCount.Load()
	if count < 2 {
		t.Errorf("expected at least 2 sync period updates, got %d", count)
	}
}

func TestCacheUpdateRemovesOnSelectorMismatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	sel := labels.SelectorFromSet(labels.Set{"env": "prod"})
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultLabelSelector(sel),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Create widget with matching label
	w := newWidget("w1", "blue", 1)
	w.Labels = map[string]string{"env": "prod"}
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Wait for cache
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{}); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Update label to no longer match selector
	w.Labels = map[string]string{"env": "dev"}
	if err := store.Update(ctx, w); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Wait for cache to reflect removal
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{})) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !errors.IsNotFound(c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, &Widget{})) {
		t.Error("expected widget removed from cache after label mismatch")
	}
}

func TestCacheWithDefaultNamespaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultNamespaces([]string{"prod", "staging"}),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Create widgets in different namespaces
	wProd := newWidget("w-prod", "blue", 1)
	wProd.Namespace = "prod"
	if err := store.Create(ctx, wProd); err != nil {
		t.Fatalf("create prod: %v", err)
	}

	wStaging := newWidget("w-staging", "green", 2)
	wStaging.Namespace = "staging"
	if err := store.Create(ctx, wStaging); err != nil {
		t.Fatalf("create staging: %v", err)
	}

	wDev := newWidget("w-dev", "red", 3)
	wDev.Namespace = "dev"
	if err := store.Create(ctx, wDev); err != nil {
		t.Fatalf("create dev: %v", err)
	}

	// Wait for cache to process events
	deadline := time.Now().Add(2 * time.Second)
	var list *WidgetList
	for time.Now().Before(deadline) {
		list = &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Only prod and staging should be cached
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 widgets (prod+staging), got %d", len(list.Items))
	}

	// Dev widget should not be found
	err := c.Get(ctx, client.ObjectKey{Namespace: "dev", Name: "w-dev"}, &Widget{})
	if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound for dev namespace widget, got: %v", err)
	}
}

func TestCacheWithDefaultNamespacesAllNamespaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	// AllNamespaces catches everything not explicitly listed
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultNamespaces([]string{"prod", storectrl.AllNamespaces}),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	wProd := newWidget("w-prod", "blue", 1)
	wProd.Namespace = "prod"
	if err := store.Create(ctx, wProd); err != nil {
		t.Fatalf("create prod: %v", err)
	}

	wDev := newWidget("w-dev", "red", 2)
	wDev.Namespace = "dev"
	if err := store.Create(ctx, wDev); err != nil {
		t.Fatalf("create dev: %v", err)
	}

	// Both should be cached — AllNamespaces covers dev
	deadline := time.Now().Add(2 * time.Second)
	var list *WidgetList
	for time.Now().Before(deadline) {
		list = &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 widgets (AllNamespaces includes all), got %d", len(list.Items))
	}
}

func TestCacheWithEnableWatchBookmarks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme)

	// Verify EnableWatchBookmarks is accepted and passed through
	c := storectrl.NewCache(store, scheme,
		storectrl.WithDefaultEnableWatchBookmarks(true),
	)

	if _, err := c.GetInformer(ctx, &Widget{}); err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Cache should work normally with bookmarks enabled
	w := newWidget("w1", "blue", 1)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	got := &Widget{}
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got.Spec.Color != "blue" {
		t.Errorf("expected blue, got %s", got.Spec.Color)
	}
}

func TestSmartReplaceAllSkipsUnchanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(5))
	c := storectrl.NewCache(store, scheme)

	informer, err := c.GetInformer(ctx, &Widget{})
	if err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Create 5 widgets
	for i := 0; i < 5; i++ {
		w := newWidget(fmt.Sprintf("w%d", i), "blue", i)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create w%d: %v", i, err)
		}
	}

	// Wait for cache to see all 5
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		list := &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) == 5 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Register handler AFTER initial sync to count only relist events
	var addCount, updateCount, deleteCount atomic.Int32
	_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { addCount.Add(1) },
		UpdateFunc: func(_, _ interface{}) { updateCount.Add(1) },
		DeleteFunc: func(obj interface{}) { deleteCount.Add(1) },
	})
	if err != nil {
		t.Fatalf("add handler: %v", err)
	}

	// Wait for AddEventHandler's snapshot delivery to settle
	time.Sleep(100 * time.Millisecond)
	addCount.Store(0)
	updateCount.Store(0)
	deleteCount.Store(0)

	// Update only 1 of the 5 widgets, then create enough events to
	// overflow the event log and force a relist
	w0 := &Widget{}
	if err := store.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w0"}, w0); err != nil {
		t.Fatalf("get w0: %v", err)
	}
	w0.Spec.Color = "red"
	if err := store.Update(ctx, w0); err != nil {
		t.Fatalf("update w0: %v", err)
	}

	// Flood event log to trigger RevisionTooOldError → relist
	for i := 10; i < 20; i++ {
		w := newWidget(fmt.Sprintf("flood-%d", i), "gray", i)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create flood: %v", err)
		}
	}

	// Wait for relist recovery to complete
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		list := &WidgetList{}
		if err := c.List(ctx, list); err == nil && len(list.Items) == 15 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	list := &WidgetList{}
	if err := c.List(ctx, list); err != nil || len(list.Items) != 15 {
		t.Fatalf("expected 15 widgets after relist, got %d", len(list.Items))
	}

	// Wait for async event processing to finish
	time.Sleep(200 * time.Millisecond)

	// Smart replaceAll: w0 changed → OnUpdate(1), flood objects new → OnAdd(10),
	// w1-w4 unchanged → no event. Without smart diff, we'd see 15 OnAdd calls.
	adds := addCount.Load()
	updates := updateCount.Load()
	deletes := deleteCount.Load()

	if adds != 10 {
		t.Errorf("expected 10 adds (flood objects), got %d", adds)
	}
	if deletes != 0 {
		t.Errorf("expected 0 deletes, got %d", deletes)
	}
	// w0 changed RV → should be OnUpdate, not OnAdd
	if updates < 1 {
		t.Errorf("expected at least 1 update (w0 changed), got %d", updates)
	}
}

func TestAsyncEventCoalescing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(1000))
	c := storectrl.NewCache(store, scheme)

	informer, err := c.GetInformer(ctx, &Widget{})
	if err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	// Track handler calls per object
	var totalUpdates atomic.Int32
	_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		UpdateFunc: func(_, _ interface{}) {
			totalUpdates.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("add handler: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	w := newWidget("burst", "blue", 1)
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Wait for cache to see the create
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "burst"}, &Widget{}); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	totalUpdates.Store(0)

	// Rapid-fire 50 updates — coalescing should reduce handler calls
	for i := 0; i < 50; i++ {
		w.Spec.Size = i + 100
		if err := store.Update(ctx, w); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	// Wait for processing to settle
	time.Sleep(500 * time.Millisecond)

	updates := totalUpdates.Load()
	// With coalescing, we should see fewer than 50 update handler calls.
	// Without coalescing, each update triggers a handler call = 50.
	if updates >= 50 {
		t.Errorf("expected coalescing to reduce updates below 50, got %d", updates)
	}
	if updates == 0 {
		t.Error("expected at least some updates to be delivered")
	}

	// Cache should reflect the final state
	got := &Widget{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "burst"}, got); err != nil {
		t.Fatalf("get after burst: %v", err)
	}
	if got.Spec.Size != 149 {
		t.Errorf("expected final size=149, got %d", got.Spec.Size)
	}
}

func TestDeleteAddPreservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	scheme := testScheme()
	store := memory.NewStore(scheme, memory.WithEventLogSize(1000))
	c := storectrl.NewCache(store, scheme)

	informer, err := c.GetInformer(ctx, &Widget{})
	if err != nil {
		t.Fatalf("get informer: %v", err)
	}

	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache never synced")
	}

	phoenix := newWidget("phoenix", "blue", 1)
	if err := store.Create(ctx, phoenix); err != nil {
		t.Fatalf("create phoenix: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "phoenix"}, &Widget{}); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "phoenix"}, &Widget{}); err != nil {
		t.Fatalf("cache never saw phoenix: %v", err)
	}

	var mu sync.Mutex
	var events []string
	_, err = informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(_ interface{}) {
			mu.Lock()
			events = append(events, "add")
			mu.Unlock()
		},
		UpdateFunc: func(_, _ interface{}) {
			mu.Lock()
			events = append(events, "update")
			mu.Unlock()
		},
		DeleteFunc: func(_ interface{}) {
			mu.Lock()
			events = append(events, "delete")
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("add handler: %v", err)
	}

	// Wait for snapshot delivery, then reset
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	events = nil
	mu.Unlock()

	// Delete then immediately re-create with different attributes
	if err := store.Delete(ctx, phoenix); err != nil {
		t.Fatalf("delete phoenix: %v", err)
	}
	phoenix2 := newWidget("phoenix", "red", 2)
	if err := store.Create(ctx, phoenix2); err != nil {
		t.Fatalf("re-create phoenix: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	got := make([]string, len(events))
	copy(got, events)
	mu.Unlock()

	hasDelete := false
	hasAdd := false
	for _, e := range got {
		switch e {
		case "delete":
			hasDelete = true
		case "add":
			hasAdd = true
		}
	}
	if !hasDelete || !hasAdd {
		t.Errorf("expected both delete and add events, got %v", got)
	}

	// Verify cache has the new widget with updated attributes
	final := &Widget{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "phoenix"}, final); err != nil {
		t.Fatalf("get phoenix after re-create: %v", err)
	}
	if final.Spec.Color != "red" {
		t.Errorf("expected color=red, got %s", final.Spec.Color)
	}
	if final.Spec.Size != 2 {
		t.Errorf("expected size=2, got %d", final.Spec.Size)
	}
}
