package storectrl_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/patjlm/storectrl"
	"github.com/patjlm/storectrl/memory"
)

func TestStoreListerWatcher_List(t *testing.T) {
	store := memory.NewStore(testScheme())
	ctx := context.Background()

	w1 := &Widget{Spec: WidgetSpec{Color: "red", Size: 10}}
	w1.SetName("w1")
	w1.SetNamespace("default")
	w1.SetLabels(map[string]string{"color": "red"})
	if err := store.Create(ctx, w1); err != nil {
		t.Fatal(err)
	}

	w2 := &Widget{Spec: WidgetSpec{Color: "blue", Size: 20}}
	w2.SetName("w2")
	w2.SetNamespace("default")
	w2.SetLabels(map[string]string{"color": "blue"})
	if err := store.Create(ctx, w2); err != nil {
		t.Fatal(err)
	}

	lw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	t.Run("list all", func(t *testing.T) {
		obj, err := lw.List(metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		list := obj.(*WidgetList)
		if len(list.Items) != 2 {
			t.Fatalf("expected 2 items, got %d", len(list.Items))
		}
	})

	t.Run("list with label selector", func(t *testing.T) {
		obj, err := lw.List(metav1.ListOptions{LabelSelector: "color=red"})
		if err != nil {
			t.Fatal(err)
		}
		list := obj.(*WidgetList)
		if len(list.Items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(list.Items))
		}
		if list.Items[0].GetName() != "w1" {
			t.Fatalf("expected w1, got %s", list.Items[0].GetName())
		}
	})

	t.Run("list with invalid label selector", func(t *testing.T) {
		_, err := lw.List(metav1.ListOptions{LabelSelector: "!!invalid!!"})
		if err == nil {
			t.Fatal("expected error for invalid label selector")
		}
	})

	t.Run("list returns ResourceVersion", func(t *testing.T) {
		obj, err := lw.List(metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		list := obj.(*WidgetList)
		if list.GetResourceVersion() == "" {
			t.Fatal("expected non-empty ResourceVersion on list")
		}
	})
}

func TestStoreListerWatcher_Watch(t *testing.T) {
	store := memory.NewStore(testScheme())
	lw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	t.Run("event type mapping", func(t *testing.T) {
		wi, err := lw.Watch(metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		ctx := context.Background()

		w := &Widget{Spec: WidgetSpec{Color: "green"}}
		w.SetName("w1")
		w.SetNamespace("default")
		if err := store.Create(ctx, w); err != nil {
			t.Fatal(err)
		}

		evt := waitForEvent(t, wi, 2*time.Second)
		if evt.Type != watch.Added {
			t.Fatalf("expected Added, got %s", evt.Type)
		}

		w.Spec.Color = "yellow"
		if err := store.Update(ctx, w); err != nil {
			t.Fatal(err)
		}

		evt = waitForEvent(t, wi, 2*time.Second)
		if evt.Type != watch.Modified {
			t.Fatalf("expected Modified, got %s", evt.Type)
		}

		if err := store.Delete(ctx, w); err != nil {
			t.Fatal(err)
		}

		evt = waitForEvent(t, wi, 2*time.Second)
		if evt.Type != watch.Deleted {
			t.Fatalf("expected Deleted, got %s", evt.Type)
		}
	})

	t.Run("watch from resource version", func(t *testing.T) {
		ctx := context.Background()

		w := &Widget{Spec: WidgetSpec{Color: "red"}}
		w.SetName("rv-test")
		w.SetNamespace("default")
		if err := store.Create(ctx, w); err != nil {
			t.Fatal(err)
		}
		rv := w.GetResourceVersion()

		w.Spec.Color = "blue"
		if err := store.Update(ctx, w); err != nil {
			t.Fatal(err)
		}

		wi, err := lw.Watch(metav1.ListOptions{ResourceVersion: rv})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		evt := waitForEvent(t, wi, 2*time.Second)
		if evt.Type != watch.Modified {
			t.Fatalf("expected Modified replay, got %s", evt.Type)
		}
	})

	t.Run("stop closes result channel", func(t *testing.T) {
		wi, err := lw.Watch(metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}

		wi.Stop()

		select {
		case _, ok := <-wi.ResultChan():
			if ok {
				// Drain remaining events
				for range wi.ResultChan() {
				}
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ResultChan not closed after Stop")
		}
	})

	t.Run("double stop is safe", func(t *testing.T) {
		wi, err := lw.Watch(metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		wi.Stop()
		wi.Stop()
	})
}

func TestStoreListerWatcher_ListWithContext(t *testing.T) {
	store := memory.NewStore(testScheme())
	ctx := context.Background()

	w := &Widget{Spec: WidgetSpec{Color: "red"}}
	w.SetName("ctx-test")
	w.SetNamespace("default")
	if err := store.Create(ctx, w); err != nil {
		t.Fatal(err)
	}

	lw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	obj, err := lw.ListWithContext(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	list := obj.(*WidgetList)
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(list.Items))
	}
}

func TestStoreListerWatcher_WatchWithContext(t *testing.T) {
	store := memory.NewStore(testScheme())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	wi, err := lw.WatchWithContext(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer wi.Stop()

	w := &Widget{Spec: WidgetSpec{Color: "red"}}
	w.SetName("ctx-watch")
	w.SetNamespace("default")
	if err := store.Create(ctx, w); err != nil {
		t.Fatal(err)
	}

	evt := waitForEvent(t, wi, 2*time.Second)
	if evt.Type != watch.Added {
		t.Fatalf("expected Added, got %s", evt.Type)
	}
}

func TestStoreListerWatcher_WatchWithLabelSelector(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore(testScheme())

	blue := &Widget{Spec: WidgetSpec{Color: "blue"}}
	blue.SetName("w-blue")
	blue.SetNamespace("default")
	blue.SetLabels(map[string]string{"color": "blue"})
	if err := store.Create(ctx, blue); err != nil {
		t.Fatal(err)
	}

	red := &Widget{Spec: WidgetSpec{Color: "red"}}
	red.SetName("w-red")
	red.SetNamespace("default")
	red.SetLabels(map[string]string{"color": "red"})
	if err := store.Create(ctx, red); err != nil {
		t.Fatal(err)
	}

	lw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	t.Run("live events filtered by label selector", func(t *testing.T) {
		wi, err := lw.WatchWithContext(ctx, metav1.ListOptions{
			LabelSelector: "color=blue",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		red2 := &Widget{Spec: WidgetSpec{Color: "red"}}
		red2.SetName("w-red2")
		red2.SetNamespace("default")
		red2.SetLabels(map[string]string{"color": "red"})
		if err := store.Create(ctx, red2); err != nil {
			t.Fatal(err)
		}

		blue2 := &Widget{Spec: WidgetSpec{Color: "blue"}}
		blue2.SetName("w-blue2")
		blue2.SetNamespace("default")
		blue2.SetLabels(map[string]string{"color": "blue"})
		if err := store.Create(ctx, blue2); err != nil {
			t.Fatal(err)
		}

		evt := waitForEvent(t, wi, 2*time.Second)
		if evt.Object.(client.Object).GetName() != "w-blue2" {
			t.Fatalf("expected w-blue2, got %s", evt.Object.(client.Object).GetName())
		}

		select {
		case evt := <-wi.ResultChan():
			t.Fatalf("unexpected event: %s %s", evt.Type, evt.Object.(client.Object).GetName())
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("SendInitialEvents filtered by label selector", func(t *testing.T) {
		sendInitial := true
		wi, err := lw.WatchWithContext(ctx, metav1.ListOptions{
			LabelSelector:     "color=blue",
			SendInitialEvents: &sendInitial,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		var names []string
		for {
			select {
			case evt := <-wi.ResultChan():
				if evt.Type == watch.Bookmark {
					goto done
				}
				names = append(names, evt.Object.(client.Object).GetName())
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for initial events")
			}
		}
	done:
		for _, name := range names {
			if name != "w-blue" && name != "w-blue2" {
				t.Errorf("initial event contained non-blue object: %s", name)
			}
		}
		if len(names) != 2 {
			t.Errorf("expected 2 initial blue events, got %d: %v", len(names), names)
		}

		// Live events after initial should also be filtered
		red3 := &Widget{Spec: WidgetSpec{Color: "red"}}
		red3.SetName("w-red3")
		red3.SetNamespace("default")
		red3.SetLabels(map[string]string{"color": "red"})
		if err := store.Create(ctx, red3); err != nil {
			t.Fatal(err)
		}

		blue3 := &Widget{Spec: WidgetSpec{Color: "blue"}}
		blue3.SetName("w-blue3")
		blue3.SetNamespace("default")
		blue3.SetLabels(map[string]string{"color": "blue"})
		if err := store.Create(ctx, blue3); err != nil {
			t.Fatal(err)
		}

		evt := waitForEvent(t, wi, 2*time.Second)
		if evt.Object.(client.Object).GetName() != "w-blue3" {
			t.Fatalf("expected w-blue3 live event, got %s", evt.Object.(client.Object).GetName())
		}
	})
}

func TestStoreListerWatcher_WatchLabelSelectorEdgeCases(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore(testScheme())
	lw := storectrl.NewStoreListerWatcher(store, &WidgetList{})

	t.Run("update changing labels to match selector appears", func(t *testing.T) {
		red := &Widget{Spec: WidgetSpec{Color: "red"}}
		red.SetName("chameleon")
		red.SetNamespace("default")
		red.SetLabels(map[string]string{"color": "red"})
		if err := store.Create(ctx, red); err != nil {
			t.Fatal(err)
		}

		wi, err := lw.WatchWithContext(ctx, metav1.ListOptions{
			LabelSelector: "color=blue",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		red.SetLabels(map[string]string{"color": "blue"})
		red.Spec.Color = "blue"
		if err := store.Update(ctx, red); err != nil {
			t.Fatal(err)
		}

		evt := waitForEvent(t, wi, 2*time.Second)
		if evt.Type != watch.Modified {
			t.Fatalf("expected Modified, got %s", evt.Type)
		}
		if evt.Object.(client.Object).GetName() != "chameleon" {
			t.Fatalf("expected chameleon, got %s", evt.Object.(client.Object).GetName())
		}
	})

	t.Run("delete of non-matching object is filtered", func(t *testing.T) {
		nonMatch := &Widget{Spec: WidgetSpec{Color: "green"}}
		nonMatch.SetName("ephemeral")
		nonMatch.SetNamespace("default")
		nonMatch.SetLabels(map[string]string{"color": "green"})
		if err := store.Create(ctx, nonMatch); err != nil {
			t.Fatal(err)
		}

		wi, err := lw.WatchWithContext(ctx, metav1.ListOptions{
			LabelSelector: "color=blue",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		if err := store.Delete(ctx, nonMatch); err != nil {
			t.Fatal(err)
		}

		// Should not receive delete for non-matching object
		select {
		case evt := <-wi.ResultChan():
			t.Fatalf("unexpected event for non-matching delete: %s %s",
				evt.Type, evt.Object.(client.Object).GetName())
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("ResourceVersion resume with label selector filters replay", func(t *testing.T) {
		// Record RV before creating mixed objects
		preRV := "0"
		list, err := lw.ListWithContext(ctx, metav1.ListOptions{})
		if err == nil {
			preRV = list.(*WidgetList).GetResourceVersion()
		}

		g1 := &Widget{Spec: WidgetSpec{Color: "green"}}
		g1.SetName("rv-green")
		g1.SetNamespace("default")
		g1.SetLabels(map[string]string{"color": "green"})
		if err := store.Create(ctx, g1); err != nil {
			t.Fatal(err)
		}

		b1 := &Widget{Spec: WidgetSpec{Color: "blue"}}
		b1.SetName("rv-blue")
		b1.SetNamespace("default")
		b1.SetLabels(map[string]string{"color": "blue"})
		if err := store.Create(ctx, b1); err != nil {
			t.Fatal(err)
		}

		wi, err := lw.WatchWithContext(ctx, metav1.ListOptions{
			LabelSelector:   "color=blue",
			ResourceVersion: preRV,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer wi.Stop()

		evt := waitForEvent(t, wi, 2*time.Second)
		if evt.Object.(client.Object).GetName() != "rv-blue" {
			t.Fatalf("expected rv-blue in replay, got %s", evt.Object.(client.Object).GetName())
		}

		// No green event should appear
		select {
		case evt := <-wi.ResultChan():
			t.Fatalf("unexpected replay event: %s %s",
				evt.Type, evt.Object.(client.Object).GetName())
		case <-time.After(100 * time.Millisecond):
		}
	})
}

func waitForEvent(t *testing.T, wi watch.Interface, timeout time.Duration) watch.Event {
	t.Helper()
	select {
	case evt, ok := <-wi.ResultChan():
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return evt
	case <-time.After(timeout):
		t.Fatal("timed out waiting for watch event")
		return watch.Event{}
	}
}
