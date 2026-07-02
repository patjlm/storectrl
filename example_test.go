package ctrlforge_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	stderrors "errors"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/ctrlforge"
	"github.com/patjlm/ctrlforge/memory"
)

type Widget struct {
	ctrlforge.BaseObject `json:",inline"`
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
	ctrlforge.BaseList `json:",inline"`
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
	ctrlforge.BaseObject `json:",inline"`
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
	ctrlforge.BaseList `json:",inline"`
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

var gadgetGV = schema.GroupVersion{Group: "gadget.ctrlforge.dev", Version: "v1"}
var widgetGV = schema.GroupVersion{Group: "example.ctrlforge.dev", Version: "v1"}

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
	c := ctrlforge.NewClient(store, scheme)

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
	c := ctrlforge.NewClient(store, scheme)

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
	c := ctrlforge.NewClient(store, scheme)

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
	watcher, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch from 0: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != ctrlforge.EventAdded {
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
		if evt.Type != ctrlforge.EventAdded || evt.Object.GetName() != "w3" {
			t.Errorf("expected ADDED w3, got %s %s", evt.Type, evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for live event")
	}
	watcher.Stop()

	// Resume from revision 2 (after w2 create) — should replay only w3
	watcher2, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(2))
	if err != nil {
		t.Fatalf("watch from 2: %v", err)
	}

	select {
	case evt := <-watcher2.ResultChan():
		if evt.Type != ctrlforge.EventAdded || evt.Object.GetName() != "w3" {
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
	watcher, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(1))
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
	_, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(1))
	var rvErr *ctrlforge.RevisionTooOldError
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
	watcher, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(5))
	if err != nil {
		t.Fatalf("watch from 5 should work: %v", err)
	}
	// Should get 5 replay events (revisions 6-10)
	for i := 0; i < 5; i++ {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != ctrlforge.EventAdded {
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
	watcher2, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(int64(count)))
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
	watcher, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	expected := []ctrlforge.EventType{ctrlforge.EventAdded, ctrlforge.EventModified, ctrlforge.EventDeleted}
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
	g.APIVersion = "gadget.ctrlforge.dev/v1"
	g.Kind = "Gadget"
	if err := store.Create(ctx, g); err != nil {
		t.Fatalf("create gadget: %v", err)
	}

	// Watch Widgets from revision 0 — should only see Widget, not Gadget
	watcher, err := store.Watch(ctx, &WidgetList{}, ctrlforge.WatchFromRevision(0))
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
	c := ctrlforge.NewCache(store, scheme)

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
	c := ctrlforge.NewCache(store, scheme)

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
	c := ctrlforge.NewCache(store, scheme)

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
