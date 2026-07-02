package reconkit

import (
	"context"
	"fmt"

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Kind creates a Source that watches objects of a specific kind using the provided cache.
// It mirrors controller-runtime's source.Kind but works with the reconkit cache.
func Kind(cache cache.Cache, obj client.Object, handler handler.EventHandler, predicates ...predicate.Predicate) source.Source {
	return &kindSource{
		cache:      cache,
		obj:        obj,
		handler:    handler,
		predicates: predicates,
	}
}

type kindSource struct {
	cache      cache.Cache
	obj        client.Object
	handler    handler.EventHandler
	predicates []predicate.Predicate
}

// Start starts the source by getting an informer and adding event handlers.
func (s *kindSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	// Get informer for the object type
	informer, err := s.cache.GetInformer(ctx, s.obj)
	if err != nil {
		return fmt.Errorf("failed to get informer: %w", err)
	}

	// Create event handler that filters and enqueues
	eventHandler := &eventHandlerAdapter{
		handler:    s.handler,
		predicates: s.predicates,
		queue:      queue,
	}

	// Register the event handler with the informer
	if _, err := informer.AddEventHandler(eventHandler); err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}

	return nil
}

// eventHandlerAdapter adapts controller-runtime's handler.EventHandler to
// client-go's ResourceEventHandler interface.
type eventHandlerAdapter struct {
	handler    handler.EventHandler
	predicates []predicate.Predicate
	queue      workqueue.TypedRateLimitingInterface[reconcile.Request]
}

// OnAdd handles object creation events.
func (a *eventHandlerAdapter) OnAdd(obj interface{}, isInInitialList bool) {
	clientObj, ok := obj.(client.Object)
	if !ok {
		return
	}

	evt := event.CreateEvent{
		Object: clientObj,
	}

	// Apply predicates
	for _, p := range a.predicates {
		if !p.Create(evt) {
			return
		}
	}

	// Create context for handler
	ctx := context.Background()

	// Get reconcile requests from handler
	a.handler.Create(ctx, evt, a.queue)
}

// OnUpdate handles object update events.
func (a *eventHandlerAdapter) OnUpdate(oldObj, newObj interface{}) {
	oldClientObj, ok := oldObj.(client.Object)
	if !ok {
		return
	}
	newClientObj, ok := newObj.(client.Object)
	if !ok {
		return
	}

	evt := event.UpdateEvent{
		ObjectOld: oldClientObj,
		ObjectNew: newClientObj,
	}

	// Apply predicates
	for _, p := range a.predicates {
		if !p.Update(evt) {
			return
		}
	}

	// Create context for handler
	ctx := context.Background()

	// Get reconcile requests from handler
	a.handler.Update(ctx, evt, a.queue)
}

// OnDelete handles object deletion events.
func (a *eventHandlerAdapter) OnDelete(obj interface{}) {
	clientObj, ok := obj.(client.Object)
	if !ok {
		return
	}

	evt := event.DeleteEvent{
		Object: clientObj,
	}

	// Apply predicates
	for _, p := range a.predicates {
		if !p.Delete(evt) {
			return
		}
	}

	// Create context for handler
	ctx := context.Background()

	// Get reconcile requests from handler
	a.handler.Delete(ctx, evt, a.queue)
}

var _ source.Source = &kindSource{}
