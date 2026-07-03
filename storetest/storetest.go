package storetest

import (
	"context"
	stderrors "errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/storectrl"
)

// Config declares what a Store backend supports. Tests for unsupported
// features are skipped, not failed.
type Config struct {
	// NewStore creates a fresh Store instance for each subtest.
	// The provided scheme has the suite's internal test types registered.
	NewStore func(*runtime.Scheme) storectrl.Store

	// NewSmallEventLogStore creates a Store with a small event log (~5 entries)
	// so the suite can test RevisionTooOldError. If nil, those tests are skipped.
	NewSmallEventLogStore func(*runtime.Scheme) storectrl.Store

	// SkipApply declares that the backend does not implement Apply.
	// When true, Apply tests only verify the backend returns an error.
	// When false (default), Apply conformance tests run.
	SkipApply bool
}

// TestStore runs the full Store conformance suite against the given backend.
// Backend authors call this from their own test files:
//
//	func TestMyBackend(t *testing.T) {
//	    storetest.TestStore(t, storetest.Config{
//	        NewStore: func(scheme *runtime.Scheme) storectrl.Store {
//	            return mybackend.New(scheme)
//	        },
//	    })
//	}
func TestStore(t *testing.T, cfg Config) {
	t.Run("Create", func(t *testing.T) { testCreate(t, cfg) })
	t.Run("Get", func(t *testing.T) { testGet(t, cfg) })
	t.Run("Update", func(t *testing.T) { testUpdate(t, cfg) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, cfg) })
	t.Run("List", func(t *testing.T) { testList(t, cfg) })
	t.Run("Watch", func(t *testing.T) { testWatch(t, cfg) })
	t.Run("Apply", func(t *testing.T) { testApply(t, cfg) })
	t.Run("Concurrency", func(t *testing.T) { testConcurrency(t, cfg) })
}

// --- internal test types ---

var widgetGV = schema.GroupVersion{Group: "widget.storetest.dev", Version: "v1"}
var gadgetGV = schema.GroupVersion{Group: "gadget.storetest.dev", Version: "v1"}

type testWidget struct {
	storectrl.BaseObject `json:",inline"`
	Spec                 testWidgetSpec   `json:"spec"`
	Status               testWidgetStatus `json:"status"`
}

type testWidgetSpec struct {
	Color string `json:"color"`
	Size  int    `json:"size"`
}

type testWidgetStatus struct {
	Ready bool   `json:"ready"`
	Phase string `json:"phase"`
}

func (w *testWidget) DeepCopyObject() runtime.Object {
	if w == nil {
		return nil
	}
	out := &testWidget{}
	w.BaseObject.DeepCopyInto(&out.BaseObject)
	out.Spec = w.Spec
	out.Status = w.Status
	return out
}

type testWidgetList struct {
	storectrl.BaseList `json:",inline"`
	Items              []testWidget `json:"items"`
}

func (w *testWidgetList) DeepCopyObject() runtime.Object {
	if w == nil {
		return nil
	}
	out := &testWidgetList{}
	w.BaseList.DeepCopyInto(&out.BaseList)
	if w.Items != nil {
		out.Items = make([]testWidget, len(w.Items))
		for i := range w.Items {
			out.Items[i] = *w.Items[i].DeepCopyObject().(*testWidget)
		}
	}
	return out
}

type testGadget struct {
	storectrl.BaseObject `json:",inline"`
}

func (g *testGadget) DeepCopyObject() runtime.Object {
	if g == nil {
		return nil
	}
	out := &testGadget{}
	g.BaseObject.DeepCopyInto(&out.BaseObject)
	return out
}

type testGadgetList struct {
	storectrl.BaseList `json:",inline"`
	Items              []testGadget `json:"items"`
}

func (g *testGadgetList) DeepCopyObject() runtime.Object {
	if g == nil {
		return nil
	}
	out := &testGadgetList{}
	g.BaseList.DeepCopyInto(&out.BaseList)
	if g.Items != nil {
		out.Items = make([]testGadget, len(g.Items))
		for i := range g.Items {
			out.Items[i] = *g.Items[i].DeepCopyObject().(*testGadget)
		}
	}
	return out
}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypes(widgetGV, &testWidget{}, &testWidgetList{})
	metav1.AddToGroupVersion(s, widgetGV)
	s.AddKnownTypes(gadgetGV, &testGadget{}, &testGadgetList{})
	metav1.AddToGroupVersion(s, gadgetGV)
	return s
}

func newTestWidget(name, color string, size int) *testWidget {
	w := &testWidget{
		Spec: testWidgetSpec{Color: color, Size: size},
	}
	w.Name = name
	w.Namespace = "default"
	w.APIVersion = widgetGV.String()
	w.Kind = "Widget"
	return w
}

// --- Create ---

func testCreate(t *testing.T, cfg Config) {
	t.Run("sets_UID_and_ResourceVersion", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		if w.GetUID() == "" {
			t.Error("UID not set after create")
		}
		if w.GetResourceVersion() == "" {
			t.Error("ResourceVersion not set after create")
		}
	})

	t.Run("duplicate_returns_AlreadyExistsError", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("first create: %v", err)
		}

		w2 := newTestWidget("w1", "red", 2)
		err := store.Create(ctx, w2)

		var alreadyExists *storectrl.AlreadyExistsError
		if !stderrors.As(err, &alreadyExists) {
			t.Fatalf("expected AlreadyExistsError, got %v", err)
		}
		if !errors.IsAlreadyExists(err) {
			t.Error("AlreadyExistsError must satisfy apierrors.IsAlreadyExists()")
		}
	})
}

// --- Get ---

func testGet(t *testing.T, cfg Config) {
	t.Run("retrieves_by_key", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKey{Namespace: "default", Name: "w1"}, got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Spec.Color != "blue" || got.Spec.Size != 42 {
			t.Errorf("wrong spec: color=%s size=%d", got.Spec.Color, got.Spec.Size)
		}
	})

	t.Run("missing_returns_NotFoundError", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		err := store.Get(ctx, client.ObjectKey{Namespace: "default", Name: "ghost"}, &testWidget{})

		var notFound *storectrl.NotFoundError
		if !stderrors.As(err, &notFound) {
			t.Fatalf("expected NotFoundError, got %v", err)
		}
		if !errors.IsNotFound(err) {
			t.Error("NotFoundError must satisfy apierrors.IsNotFound()")
		}
	})
}

// --- Update ---

func testUpdate(t *testing.T, cfg Config) {
	t.Run("bumps_ResourceVersion", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		originalRV := w.GetResourceVersion()

		w.Spec.Color = "red"
		if err := store.Update(ctx, w); err != nil {
			t.Fatalf("update: %v", err)
		}
		if w.GetResourceVersion() == originalRV {
			t.Error("ResourceVersion should change after update")
		}

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get after update: %v", err)
		}
		if got.Spec.Color != "red" {
			t.Errorf("expected color=red after update, got %s", got.Spec.Color)
		}
	})

	t.Run("stale_ResourceVersion_returns_ConflictError", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		r1, r2 := &testWidget{}, &testWidget{}
		key := client.ObjectKeyFromObject(w)
		if err := store.Get(ctx, key, r1); err != nil {
			t.Fatalf("get r1: %v", err)
		}
		if err := store.Get(ctx, key, r2); err != nil {
			t.Fatalf("get r2: %v", err)
		}

		r1.Spec.Size = 20
		if err := store.Update(ctx, r1); err != nil {
			t.Fatalf("r1 update: %v", err)
		}

		r2.Spec.Size = 30
		err := store.Update(ctx, r2)

		var conflict *storectrl.ConflictError
		if !stderrors.As(err, &conflict) {
			t.Fatalf("expected ConflictError, got %v", err)
		}
		if !errors.IsConflict(err) {
			t.Error("ConflictError must satisfy apierrors.IsConflict()")
		}

		got := &testWidget{}
		if err := store.Get(ctx, key, got); err != nil {
			t.Fatalf("get after conflict: %v", err)
		}
		if got.Spec.Size != 20 {
			t.Errorf("expected winner's value Size=20, got %d", got.Spec.Size)
		}
	})

	t.Run("missing_returns_NotFoundError", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("ghost", "blue", 1)
		w.SetResourceVersion("1")
		err := store.Update(ctx, w)

		if !errors.IsNotFound(err) {
			t.Errorf("expected NotFound, got %v", err)
		}
	})
}

// --- Delete ---

func testDelete(t *testing.T, cfg Config) {
	t.Run("removes_object", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		if err := store.Delete(ctx, w); err != nil {
			t.Fatalf("delete: %v", err)
		}

		err := store.Get(ctx, client.ObjectKeyFromObject(w), &testWidget{})
		if !errors.IsNotFound(err) {
			t.Errorf("expected NotFound after delete, got %v", err)
		}
	})

	t.Run("missing_returns_NotFoundError", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("ghost", "blue", 1)
		err := store.Delete(ctx, w)

		var notFound *storectrl.NotFoundError
		if !stderrors.As(err, &notFound) {
			t.Fatalf("expected NotFoundError, got %v", err)
		}
		if !errors.IsNotFound(err) {
			t.Error("NotFoundError must satisfy apierrors.IsNotFound()")
		}
	})
}

// --- List ---

func testList(t *testing.T, cfg Config) {
	t.Run("all_objects", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		for i := 0; i < 3; i++ {
			w := newTestWidget(fmt.Sprintf("w%d", i), "blue", i)
			if err := store.Create(ctx, w); err != nil {
				t.Fatalf("create w%d: %v", i, err)
			}
		}

		list := &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(list.Items) != 3 {
			t.Errorf("expected 3 items, got %d", len(list.Items))
		}
	})

	t.Run("empty_store", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		list := &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(list.Items) != 0 {
			t.Errorf("expected 0 items, got %d", len(list.Items))
		}
	})

	t.Run("by_namespace", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w1 := newTestWidget("w1", "blue", 1)
		w1.Namespace = "ns-a"
		if err := store.Create(ctx, w1); err != nil {
			t.Fatalf("create w1: %v", err)
		}

		w2 := newTestWidget("w2", "red", 2)
		w2.Namespace = "ns-b"
		if err := store.Create(ctx, w2); err != nil {
			t.Fatalf("create w2: %v", err)
		}

		list := &testWidgetList{}
		if err := store.List(ctx, list, client.InNamespace("ns-a")); err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(list.Items) != 1 {
			t.Errorf("expected 1 item in ns-a, got %d", len(list.Items))
		}
	})

	t.Run("by_label_selector", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w1 := newTestWidget("w1", "blue", 1)
		w1.Labels = map[string]string{"color": "blue"}
		if err := store.Create(ctx, w1); err != nil {
			t.Fatalf("create w1: %v", err)
		}

		w2 := newTestWidget("w2", "red", 2)
		w2.Labels = map[string]string{"color": "red"}
		if err := store.Create(ctx, w2); err != nil {
			t.Fatalf("create w2: %v", err)
		}

		sel, err := labels.Parse("color=blue")
		if err != nil {
			t.Fatalf("parse selector: %v", err)
		}

		list := &testWidgetList{}
		if err := store.List(ctx, list, client.MatchingLabelsSelector{Selector: sel}); err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(list.Items) != 1 {
			t.Errorf("expected 1 blue item, got %d", len(list.Items))
		}
	})

	t.Run("sets_ResourceVersion_on_metadata", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		list := &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}
		if list.GetResourceVersion() != "0" {
			t.Errorf("expected RV=0 on empty list, got %s", list.GetResourceVersion())
		}

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		list = &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}
		if list.GetResourceVersion() != "1" {
			t.Errorf("expected RV=1, got %s", list.GetResourceVersion())
		}
	})
}

// --- Watch ---

func testWatch(t *testing.T, cfg Config) {
	t.Run("streams_events", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		watcher, err := store.Watch(ctx, &testWidgetList{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventAdded, "w1")

		w.Spec.Color = "red"
		if err := store.Update(ctx, w); err != nil {
			t.Fatalf("update: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventModified, "w1")

		if err := store.Delete(ctx, w); err != nil {
			t.Fatalf("delete: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventDeleted, "w1")
	})

	t.Run("WatchFromRevision_replays", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w1 := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w1); err != nil {
			t.Fatalf("create w1: %v", err)
		}
		w2 := newTestWidget("w2", "red", 2)
		if err := store.Create(ctx, w2); err != nil {
			t.Fatalf("create w2: %v", err)
		}

		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(0))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		for i := 0; i < 2; i++ {
			select {
			case evt := <-watcher.ResultChan():
				if evt.Type != storectrl.EventAdded {
					t.Errorf("replay %d: expected ADDED, got %s", i, evt.Type)
				}
			case <-time.After(time.Second):
				t.Fatalf("timeout at replay event %d", i)
			}
		}

		w3 := newTestWidget("w3", "green", 3)
		if err := store.Create(ctx, w3); err != nil {
			t.Fatalf("create w3: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventAdded, "w3")
		watcher.Stop()

		// Resume from intermediate revision — should replay only w3
		watcher2, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(2))
		if err != nil {
			t.Fatalf("watch from 2: %v", err)
		}
		defer watcher2.Stop()

		select {
		case evt := <-watcher2.ResultChan():
			if evt.Type != storectrl.EventAdded || evt.Object.GetName() != "w3" {
				t.Errorf("expected ADDED w3, got %s %s", evt.Type, evt.Object.GetName())
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for resumed event")
		}
	})

	t.Run("WatchFromRevision_no_gap", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w1 := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w1); err != nil {
			t.Fatalf("create w1: %v", err)
		}

		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(1))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		w2 := newTestWidget("w2", "red", 2)
		if err := store.Create(ctx, w2); err != nil {
			t.Fatalf("create w2: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventAdded, "w2")
	})

	t.Run("replay_mixed_event_types", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
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

		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(0))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

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
	})

	t.Run("replay_filters_by_GVK", func(t *testing.T) {
		scheme := testScheme()
		store := cfg.NewStore(scheme)
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create widget: %v", err)
		}

		g := &testGadget{}
		g.Name = "g1"
		g.Namespace = "default"
		g.APIVersion = gadgetGV.String()
		g.Kind = "Gadget"
		if err := store.Create(ctx, g); err != nil {
			t.Fatalf("create gadget: %v", err)
		}

		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(0))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		expectEvent(t, watcher, storectrl.EventAdded, "w1")

		select {
		case evt := <-watcher.ResultChan():
			t.Errorf("unexpected event: %s %s", evt.Type, evt.Object.GetName())
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("label_selector_replay", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		blue := newTestWidget("w-blue", "blue", 1)
		blue.SetLabels(map[string]string{"color": "blue"})
		if err := store.Create(ctx, blue); err != nil {
			t.Fatalf("create blue: %v", err)
		}

		red := newTestWidget("w-red", "red", 2)
		red.SetLabels(map[string]string{"color": "red"})
		if err := store.Create(ctx, red); err != nil {
			t.Fatalf("create red: %v", err)
		}

		blue2 := newTestWidget("w-blue2", "blue", 3)
		blue2.SetLabels(map[string]string{"color": "blue"})
		if err := store.Create(ctx, blue2); err != nil {
			t.Fatalf("create blue2: %v", err)
		}

		sel, err := labels.Parse("color=blue")
		if err != nil {
			t.Fatalf("parse selector: %v", err)
		}

		t.Run("filtered", func(t *testing.T) {
			watcher, err := store.Watch(ctx, &testWidgetList{},
				storectrl.WatchFromRevision(0),
				client.MatchingLabelsSelector{Selector: sel},
			)
			if err != nil {
				t.Fatalf("watch: %v", err)
			}
			defer watcher.Stop()

			var names []string
			for {
				select {
				case evt := <-watcher.ResultChan():
					names = append(names, evt.Object.GetName())
				case <-time.After(100 * time.Millisecond):
					goto doneFiltered
				}
			}
		doneFiltered:
			if len(names) != 2 {
				t.Errorf("expected 2 blue replay events, got %d: %v", len(names), names)
			}
			for _, name := range names {
				if name != "w-blue" && name != "w-blue2" {
					t.Errorf("replay contained non-blue object: %s", name)
				}
			}
		})

		t.Run("no_selector_replays_all", func(t *testing.T) {
			watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(0))
			if err != nil {
				t.Fatalf("watch: %v", err)
			}
			defer watcher.Stop()

			count := 0
			for {
				select {
				case <-watcher.ResultChan():
					count++
				case <-time.After(100 * time.Millisecond):
					goto doneAll
				}
			}
		doneAll:
			if count != 3 {
				t.Errorf("expected 3 replay events without selector, got %d", count)
			}
		})
	})

	t.Run("RevisionTooOldError", func(t *testing.T) {
		if cfg.NewSmallEventLogStore == nil {
			t.Skip("NewSmallEventLogStore not configured")
		}

		store := cfg.NewSmallEventLogStore(testScheme())
		ctx := context.Background()

		for i := 0; i < 10; i++ {
			w := newTestWidget(fmt.Sprintf("w%d", i), "blue", i)
			if err := store.Create(ctx, w); err != nil {
				t.Fatalf("create w%d: %v", i, err)
			}
		}

		_, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(1))
		var rvErr *storectrl.RevisionTooOldError
		if !stderrors.As(err, &rvErr) {
			t.Fatalf("expected RevisionTooOldError, got %v", err)
		}
		if rvErr.RequestedRevision != 1 {
			t.Errorf("expected RequestedRevision=1, got %d", rvErr.RequestedRevision)
		}
		if !errors.IsGone(err) {
			t.Error("RevisionTooOldError must satisfy apierrors.IsGone()")
		}

		// Boundary: revision 5 should still work (oldest in log is 6, 5+1 >= 6)
		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(5))
		if err != nil {
			t.Fatalf("watch from boundary revision should work: %v", err)
		}
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
	})

	t.Run("overflow_closes_watch", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		watcher, err := store.Watch(ctx, &testWidgetList{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		for i := 0; i < 200; i++ {
			w := newTestWidget(fmt.Sprintf("overflow-%d", i), "blue", i)
			if err := store.Create(ctx, w); err != nil {
				t.Fatalf("create: %v", err)
			}
		}

		count := 0
		var lastRV int64
		for evt := range watcher.ResultChan() {
			count++
			rv, _ := strconv.ParseInt(evt.Object.GetResourceVersion(), 10, 64)
			lastRV = rv
		}
		if count == 0 {
			t.Error("expected at least some events before close")
		}
		if count >= 200 {
			t.Error("expected channel to close before all 200 events")
		}

		// Resume from last seen revision after overflow
		watcher2, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(lastRV))
		if err != nil {
			t.Fatalf("resume after overflow: %v", err)
		}
		watcher2.Stop()
	})
}

// --- Apply ---

func testApply(t *testing.T, cfg Config) {
	if cfg.SkipApply {
		t.Run("returns_error_when_unsupported", func(t *testing.T) {
			store := cfg.NewStore(testScheme())
			ctx := context.Background()
			err := store.Apply(ctx, nil)
			if err == nil {
				t.Error("expected error from unsupported Apply")
			}
		})
		return
	}
	t.Fatal("Apply conformance tests not yet implemented — set SkipApply: true if this backend does not support Apply")
}

// --- Concurrency ---

func testConcurrency(t *testing.T, cfg Config) {
	t.Run("concurrent_CRUD_with_watcher", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		watcher, err := store.Watch(ctx, &testWidgetList{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		var eventCount atomic.Int32
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range watcher.ResultChan() {
				eventCount.Add(1)
			}
		}()

		const goroutines = 10
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func(id int) {
				defer wg.Done()
				w := newTestWidget(fmt.Sprintf("concurrent-%d", id), "blue", id)
				if err := store.Create(ctx, w); err != nil {
					t.Errorf("create %d: %v", id, err)
					return
				}
				w.Spec.Color = "red"
				if err := store.Update(ctx, w); err != nil {
					t.Errorf("update %d: %v", id, err)
					return
				}
				if err := store.Delete(ctx, w); err != nil {
					t.Errorf("delete %d: %v", id, err)
				}
			}(i)
		}
		wg.Wait()
		watcher.Stop()
		<-done

		count := eventCount.Load()
		expected := int32(goroutines * 3)
		if count != expected {
			t.Errorf("expected %d events, got %d", expected, count)
		}
	})
}

// --- helpers ---

func expectEvent(t *testing.T, watcher storectrl.Watcher, wantType storectrl.EventType, wantName string) {
	t.Helper()
	select {
	case evt, ok := <-watcher.ResultChan():
		if !ok {
			t.Fatalf("watcher channel closed unexpectedly while waiting for %s event (name=%s)", wantType, wantName)
		}
		if evt.Type != wantType {
			t.Errorf("expected event type %s, got %s", wantType, evt.Type)
		}
		if wantName != "" && evt.Object.GetName() != wantName {
			t.Errorf("expected object name %s, got %s", wantName, evt.Object.GetName())
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s event (name=%s)", wantType, wantName)
	}
}
