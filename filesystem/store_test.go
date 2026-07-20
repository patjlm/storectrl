package filesystem_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/storectrl"
	"github.com/patjlm/storectrl/filesystem"
	"github.com/patjlm/storectrl/storetest"
)

// --- conformance suite ---

func TestStore(t *testing.T) {
	storetest.TestStore(t, storetest.Config{
		NewStore: func(scheme *runtime.Scheme) storectrl.Store {
			return filesystem.NewStore(t.TempDir(), scheme)
		},
		NewSmallEventLogStore: func(scheme *runtime.Scheme) storectrl.Store {
			return filesystem.NewStore(t.TempDir(), scheme, filesystem.WithEventLogSize(5))
		},
	})
}

// --- filesystem-specific tests ---

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

var widgetGV = schema.GroupVersion{Group: "example.storectrl.dev", Version: "v1"}

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

func TestPersistenceAcrossInstances(t *testing.T) {
	ctx := context.Background()
	scheme := testScheme()
	root := t.TempDir()

	store1 := filesystem.NewStore(root, scheme)
	c1 := storectrl.NewClient(store1, scheme)

	widget := newWidget("persist-me", "purple", 7)
	if err := c1.Create(ctx, widget); err != nil {
		t.Fatalf("create: %v", err)
	}

	store2 := filesystem.NewStore(root, scheme)
	c2 := storectrl.NewClient(store2, scheme)

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
	c := storectrl.NewClient(store, scheme)

	widget := newWidget("layout-check", "gold", 1)
	if err := c.Create(context.Background(), widget); err != nil {
		t.Fatalf("create: %v", err)
	}

	expected := filepath.Join(root, "example.storectrl.dev", "v1", "Widget", "default", "layout-check.json")
	if _, err := os.Stat(expected); err != nil {
		t.Errorf("expected file at %s: %v", expected, err)
	}
}
