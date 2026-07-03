package storectrl

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type testItem struct {
	BaseObject `json:",inline"`
	Spec       string `json:"spec"`
}

func (t *testItem) DeepCopyObject() runtime.Object {
	cp := *t
	return &cp
}

type otherItem struct {
	BaseObject `json:",inline"`
	Value      int `json:"value"`
}

func (o *otherItem) DeepCopyObject() runtime.Object {
	cp := *o
	return &cp
}

type itemsList struct {
	BaseList `json:",inline"`
	Items    []testItem `json:"items"`
}

func (l *itemsList) DeepCopyObject() runtime.Object {
	cp := *l
	return &cp
}

type otherItemsList struct {
	BaseList `json:",inline"`
	Items    []otherItem `json:"items"`
}

func (l *otherItemsList) DeepCopyObject() runtime.Object {
	cp := *l
	return &cp
}

type noItemsList struct {
	BaseList `json:",inline"`
}

func (l *noItemsList) DeepCopyObject() runtime.Object {
	cp := *l
	return &cp
}

type ptrItemsList struct {
	BaseList `json:",inline"`
	Items    []*testItem `json:"items"`
}

func (l *ptrItemsList) DeepCopyObject() runtime.Object {
	cp := *l
	return &cp
}

func TestBookmarkForList(t *testing.T) {
	t.Run("returns typed object with ResourceVersion and annotation", func(t *testing.T) {
		obj := bookmarkForList(&itemsList{}, "42")
		if obj == nil {
			t.Fatal("expected non-nil bookmark")
		}
		bo, ok := obj.(*testItem)
		if !ok {
			t.Fatalf("expected *testItem, got %T", obj)
		}
		if bo.GetResourceVersion() != "42" {
			t.Fatalf("expected RV 42, got %s", bo.GetResourceVersion())
		}
		if bo.GetAnnotations()[metav1.InitialEventsAnnotationKey] != "true" {
			t.Fatal("missing InitialEventsAnnotationKey annotation")
		}
	})

	t.Run("returns correct concrete type per list", func(t *testing.T) {
		obj := bookmarkForList(&otherItemsList{}, "1")
		if obj == nil {
			t.Fatal("expected non-nil bookmark")
		}
		if _, ok := obj.(*otherItem); !ok {
			t.Fatalf("expected *otherItem, got %T", obj)
		}
	})

	t.Run("returned object is zero-value except RV and annotation", func(t *testing.T) {
		obj := bookmarkForList(&itemsList{}, "5")
		bo := obj.(*testItem)
		if bo.GetName() != "" {
			t.Fatalf("expected empty name, got %q", bo.GetName())
		}
		if bo.GetNamespace() != "" {
			t.Fatalf("expected empty namespace, got %q", bo.GetNamespace())
		}
		if bo.Spec != "" {
			t.Fatalf("expected empty spec, got %q", bo.Spec)
		}
		if len(bo.GetLabels()) != 0 {
			t.Fatalf("expected no labels, got %v", bo.GetLabels())
		}
	})

	t.Run("empty resource version", func(t *testing.T) {
		obj := bookmarkForList(&itemsList{}, "")
		if obj == nil {
			t.Fatal("expected non-nil bookmark")
		}
		bo := obj.(*testItem)
		if bo.GetResourceVersion() != "" {
			t.Fatalf("expected empty RV, got %q", bo.GetResourceVersion())
		}
	})

	t.Run("returns nil when list has no Items field", func(t *testing.T) {
		obj := bookmarkForList(&noItemsList{}, "1")
		if obj != nil {
			t.Fatalf("expected nil, got %v", obj)
		}
	})

	t.Run("panics on pointer-element Items slice", func(t *testing.T) {
		// Items []*T causes reflect.New to create **T, which doesn't
		// implement client.Object — the type assertion panics.
		// All standard K8s list types use value slices, so this
		// documents the contract rather than a real failure path.
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic for pointer-element Items")
			}
		}()
		bookmarkForList(&ptrItemsList{}, "1")
	})

	t.Run("implements runtime.Object", func(t *testing.T) {
		obj := bookmarkForList(&itemsList{}, "1")
		iface := reflect.TypeOf((*runtime.Object)(nil)).Elem()
		if !reflect.TypeOf(obj).Implements(iface) {
			t.Fatalf("%T does not implement runtime.Object", obj)
		}
	})
}
