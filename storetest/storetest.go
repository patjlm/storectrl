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

	// SkipWatchOverflow declares the backend watch channel does not close on
	// buffer overflow. Poll-based backends (e.g. postgres) never overflow.
	SkipWatchOverflow bool

	// SkipWatchEventHistory declares the backend does not retain intermediate
	// mutation history between polls — only the latest object state is visible.
	// Poll-based backends replay current state, not individual Add/Modify/Delete
	// transitions that happened between two polls.
	SkipWatchEventHistory bool

	// SkipConcurrencyWatchCount skips the exact event-count assertion in the
	// concurrent CRUD+watch test. Poll-based backends that do not retain
	// per-mutation history cannot guarantee one event per mutation — they deliver
	// at most one event per object (current state) per poll cycle. Set this flag
	// together with SkipWatchEventHistory when using a polling backend.
	SkipConcurrencyWatchCount bool

	// SkipGlobalRevisionMonotonicity declares the backend uses per-object
	// versioning instead of a global revision counter. Watch events may have
	// non-monotonic ResourceVersions across different objects. Backends like
	// postgres use object_version for ResourceVersion, which is monotonic
	// within a single object but not globally.
	SkipGlobalRevisionMonotonicity bool
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
	t.Run("UpdateStatus", func(t *testing.T) { testUpdateStatus(t, cfg) })
	t.Run("Generation", func(t *testing.T) { testGeneration(t, cfg) })
	t.Run("SpecStatusSplit", func(t *testing.T) { testSpecStatusSplit(t, cfg) })
	t.Run("Concurrency", func(t *testing.T) { testConcurrency(t, cfg) })
	t.Run("Invariants", func(t *testing.T) { testInvariants(t, cfg) })
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
		if cfg.SkipWatchEventHistory {
			t.Skip("backend does not retain per-mutation history; only current state polled")
		}
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
		if cfg.SkipWatchOverflow {
			t.Skip("backend watch does not close on buffer overflow")
		}
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

// --- UpdateStatus ---

func testUpdateStatus(t *testing.T, cfg Config) {
	t.Run("writes_status_only", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Set status via UpdateStatus
		w.Status.Ready = true
		w.Status.Phase = "running"
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if !got.Status.Ready || got.Status.Phase != "running" {
			t.Errorf("status not updated: ready=%v phase=%s", got.Status.Ready, got.Status.Phase)
		}
		if got.Spec.Color != "blue" || got.Spec.Size != 42 {
			t.Errorf("spec should be preserved: color=%s size=%d", got.Spec.Color, got.Spec.Size)
		}
	})

	t.Run("preserves_spec", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Modify both spec and status in the object, then UpdateStatus
		w.Spec.Color = "red" // this should be IGNORED
		w.Status.Ready = true
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Spec.Color != "blue" {
			t.Errorf("UpdateStatus should preserve spec: got color=%s, want blue", got.Spec.Color)
		}
		if !got.Status.Ready {
			t.Errorf("status should be updated: got ready=%v", got.Status.Ready)
		}
	})

	t.Run("bumps_ResourceVersion", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		rvBefore := w.GetResourceVersion()

		w.Status.Ready = true
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}
		if w.GetResourceVersion() == rvBefore {
			t.Error("ResourceVersion should change after status update")
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

		r1.Status.Ready = true
		if err := store.UpdateStatus(ctx, r1); err != nil {
			t.Fatalf("r1 update status: %v", err)
		}

		r2.Status.Phase = "failed"
		err := store.UpdateStatus(ctx, r2)
		var conflict *storectrl.ConflictError
		if !stderrors.As(err, &conflict) {
			t.Fatalf("expected ConflictError, got %v", err)
		}
	})

	t.Run("missing_returns_NotFoundError", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("ghost", "blue", 1)
		w.SetResourceVersion("1")
		err := store.UpdateStatus(ctx, w)
		if !errors.IsNotFound(err) {
			t.Errorf("expected NotFound, got %v", err)
		}
	})

	t.Run("noop_suppresses_event", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("noop-status", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(1))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		rvBefore := w.GetResourceVersion()
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("noop update status: %v", err)
		}
		if w.GetResourceVersion() != rvBefore {
			t.Errorf("no-op UpdateStatus should preserve RV: before=%s after=%s", rvBefore, w.GetResourceVersion())
		}

		select {
		case evt := <-watcher.ResultChan():
			t.Errorf("no-op UpdateStatus should not emit event, got %s", evt.Type)
		case <-time.After(100 * time.Millisecond):
		}
	})
}

// --- Generation ---

func testGeneration(t *testing.T, cfg Config) {
	t.Run("create_sets_generation_1", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		if w.GetGeneration() != 1 {
			t.Errorf("expected generation=1 after create, got %d", w.GetGeneration())
		}
	})

	t.Run("spec_change_increments", func(t *testing.T) {
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
		if w.GetGeneration() != 2 {
			t.Errorf("expected generation=2 after spec change, got %d", w.GetGeneration())
		}
	})

	t.Run("status_update_unchanged", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		w.Status.Ready = true
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.GetGeneration() != 1 {
			t.Errorf("expected generation=1 after status update, got %d", got.GetGeneration())
		}
	})

	t.Run("noop_update_unchanged", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		if err := store.Update(ctx, w); err != nil {
			t.Fatalf("noop update: %v", err)
		}
		if w.GetGeneration() != 1 {
			t.Errorf("expected generation=1 after noop update, got %d", w.GetGeneration())
		}
	})
}

// --- SpecStatusSplit ---

func testSpecStatusSplit(t *testing.T, cfg Config) {
	t.Run("update_preserves_status", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Set status first
		w.Status.Ready = true
		w.Status.Phase = "running"
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}

		// Now update spec — status should be preserved
		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		got.Spec.Color = "red"
		if err := store.Update(ctx, got); err != nil {
			t.Fatalf("update spec: %v", err)
		}

		final := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), final); err != nil {
			t.Fatalf("get final: %v", err)
		}
		if final.Spec.Color != "red" {
			t.Errorf("spec should be updated: got color=%s", final.Spec.Color)
		}
		if !final.Status.Ready || final.Status.Phase != "running" {
			t.Errorf("status should be preserved: ready=%v phase=%s", final.Status.Ready, final.Status.Phase)
		}
	})

	t.Run("update_ignores_input_status", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Set status via UpdateStatus
		w.Status.Ready = true
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}

		// Update with stale status but new spec
		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		got.Spec.Color = "red"
		got.Status.Ready = false // this should be IGNORED by Update
		if err := store.Update(ctx, got); err != nil {
			t.Fatalf("update: %v", err)
		}

		final := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), final); err != nil {
			t.Fatalf("get final: %v", err)
		}
		if !final.Status.Ready {
			t.Errorf("Update should ignore input status: got ready=%v, want true", final.Status.Ready)
		}
	})

	t.Run("update_status_preserves_spec", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// UpdateStatus with modified spec — spec should be ignored
		w.Spec.Color = "red"
		w.Status.Ready = true
		if err := store.UpdateStatus(ctx, w); err != nil {
			t.Fatalf("update status: %v", err)
		}

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Spec.Color != "blue" {
			t.Errorf("UpdateStatus should preserve spec: got color=%s, want blue", got.Spec.Color)
		}
	})

	t.Run("deep_copy_isolation", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Mutate caller's object after create
		w.Spec.Color = "MUTATED"

		got := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Spec.Color != "blue" {
			t.Errorf("store should deep copy: got color=%s, want blue", got.Spec.Color)
		}
	})

	t.Run("delete_without_resource_version", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Clear RV and delete — should succeed (unconditional)
		w.SetResourceVersion("")
		if err := store.Delete(ctx, w); err != nil {
			t.Fatalf("delete without RV should succeed: %v", err)
		}

		err := store.Get(ctx, client.ObjectKeyFromObject(w), &testWidget{})
		if !errors.IsNotFound(err) {
			t.Errorf("expected NotFound after delete, got %v", err)
		}
	})

	t.Run("delete_with_resource_version", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		// Delete with correct RV succeeds
		if err := store.Delete(ctx, w); err != nil {
			t.Fatalf("delete with correct RV: %v", err)
		}
	})

	t.Run("concurrent_spec_and_status_writers", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("w1", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		const writers = 10
		var wg sync.WaitGroup
		var specErrors, statusErrors atomic.Int32

		// Spec writers
		wg.Add(writers)
		for i := 0; i < writers; i++ {
			go func(id int) {
				defer wg.Done()
				for retry := 0; retry < 10; retry++ {
					got := &testWidget{}
					if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
						continue
					}
					got.Spec.Color = fmt.Sprintf("color-%d", id)
					err := store.Update(ctx, got)
					if err == nil {
						return
					}
					var conflict *storectrl.ConflictError
					if !stderrors.As(err, &conflict) {
						specErrors.Add(1)
						return
					}
				}
			}(i)
		}

		// Status writers
		wg.Add(writers)
		for i := 0; i < writers; i++ {
			go func(id int) {
				defer wg.Done()
				for retry := 0; retry < 10; retry++ {
					got := &testWidget{}
					if err := store.Get(ctx, client.ObjectKeyFromObject(w), got); err != nil {
						continue
					}
					got.Status.Phase = fmt.Sprintf("phase-%d", id)
					err := store.UpdateStatus(ctx, got)
					if err == nil {
						return
					}
					var conflict *storectrl.ConflictError
					if !stderrors.As(err, &conflict) {
						statusErrors.Add(1)
						return
					}
				}
			}(i)
		}

		wg.Wait()

		if specErrors.Load() > 0 {
			t.Errorf("spec writers had %d non-conflict errors", specErrors.Load())
		}
		if statusErrors.Load() > 0 {
			t.Errorf("status writers had %d non-conflict errors", statusErrors.Load())
		}

		// Final object should have some spec and some status set
		final := &testWidget{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(w), final); err != nil {
			t.Fatalf("get final: %v", err)
		}
		if final.Spec.Color == "blue" {
			t.Error("expected spec to have been updated by at least one writer")
		}
		if final.Status.Phase == "" {
			t.Error("expected status to have been updated by at least one writer")
		}
	})
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
		if cfg.SkipConcurrencyWatchCount {
			// Polling backends have a debounce window before the first poll fires.
			// Wait up to 3 seconds for at least one event instead of a fixed sleep.
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) && eventCount.Load() == 0 {
				time.Sleep(50 * time.Millisecond)
			}
		}
		watcher.Stop()
		<-done

		count := eventCount.Load()
		if cfg.SkipConcurrencyWatchCount {
			if count == 0 {
				t.Errorf("expected at least 1 event from concurrent CRUD, got 0 (watcher appears dead)")
			}
		} else {
			expected := int32(goroutines * 3)
			if count != expected {
				t.Errorf("expected %d events, got %d", expected, count)
			}
		}
	})
}

// --- Invariants ---

func testInvariants(t *testing.T, cfg Config) {
	t.Run("revision_monotonicity_in_watch", func(t *testing.T) {
		if cfg.SkipGlobalRevisionMonotonicity {
			t.Skip("backend uses per-object versioning, not global revision monotonicity")
		}
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		watcher, err := store.Watch(ctx, &testWidgetList{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		for i := 0; i < 10; i++ {
			w := newTestWidget(fmt.Sprintf("mono-%d", i), "blue", i)
			if err := store.Create(ctx, w); err != nil {
				t.Fatalf("create %d: %v", i, err)
			}
		}

		var prevRV int64
		for i := 0; i < 10; i++ {
			select {
			case evt := <-watcher.ResultChan():
				rv, err := strconv.ParseInt(evt.Object.GetResourceVersion(), 10, 64)
				if err != nil {
					t.Fatalf("event %d: non-numeric ResourceVersion %q", i, evt.Object.GetResourceVersion())
				}
				if rv <= prevRV {
					t.Errorf("event %d: ResourceVersion %d <= previous %d (monotonicity violated)", i, rv, prevRV)
				}
				prevRV = rv
			case <-time.After(time.Second):
				t.Fatalf("timeout at event %d", i)
			}
		}
	})

	t.Run("noop_update_suppresses_event", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("noop", "blue", 42)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		watcher, err := store.Watch(ctx, &testWidgetList{},
			storectrl.WatchFromRevision(1))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		rvBefore := w.GetResourceVersion()

		// Update with identical content
		if err := store.Update(ctx, w); err != nil {
			t.Fatalf("noop update: %v", err)
		}

		rvAfter := w.GetResourceVersion()
		if rvAfter != rvBefore {
			t.Errorf("no-op update should preserve ResourceVersion: before=%s after=%s", rvBefore, rvAfter)
		}

		// No event should be emitted
		select {
		case evt := <-watcher.ResultChan():
			t.Errorf("no-op update should not emit event, got %s for %s", evt.Type, evt.Object.GetName())
		case <-time.After(100 * time.Millisecond):
			// expected — no event
		}
	})

	t.Run("list_returns_consistent_snapshot", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		for i := 0; i < 5; i++ {
			w := newTestWidget(fmt.Sprintf("snap-%d", i), "blue", i)
			if err := store.Create(ctx, w); err != nil {
				t.Fatalf("create %d: %v", i, err)
			}
		}

		list := &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}

		// List RV should be >= all object RVs (consistent snapshot)
		listRV, err := strconv.ParseInt(list.GetResourceVersion(), 10, 64)
		if err != nil {
			t.Fatalf("list RV non-numeric: %s", list.GetResourceVersion())
		}

		for _, item := range list.Items {
			objRV, err := strconv.ParseInt(item.GetResourceVersion(), 10, 64)
			if err != nil {
				t.Fatalf("object RV non-numeric: %s", item.GetResourceVersion())
			}
			if objRV > listRV {
				t.Errorf("object %s RV=%d > list RV=%d (inconsistent snapshot)", item.Name, objRV, listRV)
			}
		}
	})

	t.Run("watch_resumption_no_gap", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w1 := newTestWidget("gap-1", "blue", 1)
		if err := store.Create(ctx, w1); err != nil {
			t.Fatalf("create w1: %v", err)
		}
		w2 := newTestWidget("gap-2", "red", 2)
		if err := store.Create(ctx, w2); err != nil {
			t.Fatalf("create w2: %v", err)
		}

		// Watch from revision 1 — should see w2 (created at revision 2)
		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(1))
		if err != nil {
			t.Fatalf("watch: %v", err)
		}

		select {
		case evt := <-watcher.ResultChan():
			if evt.Object.GetName() != "gap-2" {
				t.Errorf("expected gap-2, got %s", evt.Object.GetName())
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for resumed event")
		}
		watcher.Stop()

		// Create more objects, then resume from w2's revision
		w3 := newTestWidget("gap-3", "green", 3)
		if err := store.Create(ctx, w3); err != nil {
			t.Fatalf("create w3: %v", err)
		}

		watcher2, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(2))
		if err != nil {
			t.Fatalf("watch from 2: %v", err)
		}
		defer watcher2.Stop()

		select {
		case evt := <-watcher2.ResultChan():
			if evt.Object.GetName() != "gap-3" {
				t.Errorf("expected gap-3, got %s", evt.Object.GetName())
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for gap-3")
		}
	})

	t.Run("resourceversion_is_numeric", func(t *testing.T) {
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		w := newTestWidget("rv-numeric", "blue", 1)
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}

		if _, err := strconv.ParseInt(w.GetResourceVersion(), 10, 64); err != nil {
			t.Errorf("ResourceVersion %q is not a numeric string: %v", w.GetResourceVersion(), err)
		}

		list := &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}
		if _, err := strconv.ParseInt(list.GetResourceVersion(), 10, 64); err != nil {
			t.Errorf("List ResourceVersion %q is not a numeric string: %v", list.GetResourceVersion(), err)
		}
	})

	t.Run("concurrent_updates_preserve_monotonicity", func(t *testing.T) {
		if cfg.SkipGlobalRevisionMonotonicity {
			t.Skip("backend uses per-object versioning, not global revision monotonicity")
		}
		store := cfg.NewStore(testScheme())
		ctx := context.Background()

		watcher, err := store.Watch(ctx, &testWidgetList{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		const writers = 5
		var wg sync.WaitGroup
		wg.Add(writers)
		for i := 0; i < writers; i++ {
			go func(id int) {
				defer wg.Done()
				w := newTestWidget(fmt.Sprintf("cmon-%d", id), "blue", id)
				if err := store.Create(ctx, w); err != nil {
					t.Errorf("create %d: %v", id, err)
					return
				}
				w.Spec.Color = "red"
				if err := store.Update(ctx, w); err != nil {
					t.Errorf("update %d: %v", id, err)
				}
			}(i)
		}
		wg.Wait()

		// Drain events and verify monotonicity
		deadline := time.Now().Add(2 * time.Second)
		var prevRV int64
		violations := 0
		for time.Now().Before(deadline) {
			select {
			case evt, ok := <-watcher.ResultChan():
				if !ok {
					goto done
				}
				rv, _ := strconv.ParseInt(evt.Object.GetResourceVersion(), 10, 64)
				if rv <= prevRV {
					violations++
				}
				prevRV = rv
			case <-time.After(200 * time.Millisecond):
				goto done
			}
		}
	done:
		if violations > 0 {
			t.Errorf("watch stream had %d monotonicity violations under concurrent writes", violations)
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
