package reconkit_test

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

	"reconkit"
	"reconkit/memory"
)

type Widget struct {
	reconkit.BaseObject `json:",inline"`
	Spec                WidgetSpec   `json:"spec"`
	Status              WidgetStatus `json:"status"`
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
	reconkit.BaseList `json:",inline"`
	Items             []Widget `json:"items"`
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
	reconkit.BaseObject `json:",inline"`
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
	reconkit.BaseList `json:",inline"`
	Items             []Gadget `json:"items"`
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

var gadgetGV = schema.GroupVersion{Group: "gadget.reconkit.dev", Version: "v1"}
var widgetGV = schema.GroupVersion{Group: "example.reconkit.dev", Version: "v1"}

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
	c := reconkit.NewClient(store, scheme)

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
	c := reconkit.NewClient(store, scheme)

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
	c := reconkit.NewClient(store, scheme)

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
	watcher, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch from 0: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != reconkit.EventAdded {
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
		if evt.Type != reconkit.EventAdded || evt.Object.GetName() != "w3" {
			t.Errorf("expected ADDED w3, got %s %s", evt.Type, evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for live event")
	}
	watcher.Stop()

	// Resume from revision 2 (after w2 create) — should replay only w3
	watcher2, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(2))
	if err != nil {
		t.Fatalf("watch from 2: %v", err)
	}

	select {
	case evt := <-watcher2.ResultChan():
		if evt.Type != reconkit.EventAdded || evt.Object.GetName() != "w3" {
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
	watcher, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(1))
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
	_, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(1))
	var rvErr *reconkit.RevisionTooOldError
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
	watcher, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(5))
	if err != nil {
		t.Fatalf("watch from 5 should work: %v", err)
	}
	// Should get 5 replay events (revisions 6-10)
	for i := 0; i < 5; i++ {
		select {
		case evt := <-watcher.ResultChan():
			if evt.Type != reconkit.EventAdded {
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
	watcher2, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(int64(count)))
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
	watcher, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	expected := []reconkit.EventType{reconkit.EventAdded, reconkit.EventModified, reconkit.EventDeleted}
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
	g.APIVersion = "gadget.reconkit.dev/v1"
	g.Kind = "Gadget"
	if err := store.Create(ctx, g); err != nil {
		t.Fatalf("create gadget: %v", err)
	}

	// Watch Widgets from revision 0 — should only see Widget, not Gadget
	watcher, err := store.Watch(ctx, &WidgetList{}, reconkit.WatchFromRevision(0))
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
