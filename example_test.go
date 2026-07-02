package reconkit_test

import (
	"context"
	"testing"
	"time"

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
