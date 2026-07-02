package filesystem_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"reconkit"
	"reconkit/filesystem"
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

func newWidget(name, color string, size int) *Widget {
	w := &Widget{
		Spec: WidgetSpec{Color: color, Size: size},
	}
	w.Name = name
	w.Namespace = "default"
	w.APIVersion = widgetGV.String()
	w.Kind = "Widget"
	return w
}

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := filesystem.NewStore(t.TempDir(), scheme)
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
	originalRV := retrieved.GetResourceVersion()
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

	// Create second
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
	store := filesystem.NewStore(t.TempDir(), scheme)
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

	reader1.Spec.Size = 20
	if err := c.Update(ctx, reader1); err != nil {
		t.Fatalf("reader1 update: %v", err)
	}

	reader2.Spec.Size = 30
	err := c.Update(ctx, reader2)
	if !errors.IsConflict(err) {
		t.Errorf("expected conflict for reader2, got: %v", err)
	}

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

func TestPersistenceAcrossInstances(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	root := t.TempDir()

	store1 := filesystem.NewStore(root, scheme)
	c1 := reconkit.NewClient(store1, scheme)

	widget := newWidget("persist-me", "purple", 7)
	if err := c1.Create(ctx, widget); err != nil {
		t.Fatalf("create: %v", err)
	}

	store2 := filesystem.NewStore(root, scheme)
	c2 := reconkit.NewClient(store2, scheme)

	got := &Widget{}
	if err := c2.Get(ctx, client.ObjectKeyFromObject(widget), got); err != nil {
		t.Fatalf("get from second store instance: %v", err)
	}
	if got.Spec.Color != "purple" || got.Spec.Size != 7 {
		t.Errorf("wrong spec from second store: color=%s size=%d", got.Spec.Color, got.Spec.Size)
	}
}

func TestFileLayout(t *testing.T) {
	root := t.TempDir()
	scheme := testScheme()
	store := filesystem.NewStore(root, scheme)
	c := reconkit.NewClient(store, scheme)

	widget := newWidget("layout-check", "gold", 1)
	if err := c.Create(context.Background(), widget); err != nil {
		t.Fatalf("create: %v", err)
	}

	expected := filepath.Join(root, "example.reconkit.dev", "v1", "Widget", "default", "layout-check.json")
	if _, err := os.Stat(expected); err != nil {
		t.Errorf("expected file at %s: %v", expected, err)
	}
}

func TestWatch(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := filesystem.NewStore(t.TempDir(), scheme)

	w, err := store.Watch(ctx, &WidgetList{})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	widget := newWidget("watched", "cyan", 5)
	if err := store.Create(ctx, widget); err != nil {
		t.Fatalf("create: %v", err)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != reconkit.EventAdded {
			t.Errorf("expected ADDED, got %s", event.Type)
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for watch event")
	}
}

func TestAlreadyExists(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := filesystem.NewStore(t.TempDir(), scheme)
	c := reconkit.NewClient(store, scheme)

	widget := newWidget("dupe", "red", 1)
	if err := c.Create(ctx, widget); err != nil {
		t.Fatalf("first create: %v", err)
	}

	widget2 := newWidget("dupe", "blue", 2)
	err := c.Create(ctx, widget2)
	if !errors.IsAlreadyExists(err) {
		t.Errorf("expected AlreadyExists, got: %v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := filesystem.NewStore(t.TempDir(), scheme)
	c := reconkit.NewClient(store, scheme)

	widget := newWidget("ghost", "invisible", 0)
	err := c.Delete(ctx, widget)
	if !errors.IsNotFound(err) {
		t.Errorf("expected NotFound, got: %v", err)
	}
}

func TestListEmptyStore(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	store := filesystem.NewStore(t.TempDir(), scheme)
	c := reconkit.NewClient(store, scheme)

	list := &WidgetList{}
	if err := c.List(ctx, list); err != nil {
		t.Fatalf("list empty: %v", err)
	}
	if len(list.Items) != 0 {
		t.Errorf("expected 0 items, got %d", len(list.Items))
	}
}
