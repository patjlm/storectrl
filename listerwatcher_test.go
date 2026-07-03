package storectrl_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

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
