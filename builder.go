package reconkit

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Builder provides a fluent interface for constructing controllers.
// It wraps controller-runtime's builder with reconkit-specific defaults.
type Builder struct {
	mgr         manager.Manager
	forInput    ForInput
	ownsInputs  []OwnsInput
	watches     []WatchesInput
	ctrlOptions controller.Options
	name        string
}

// ForInput represents the primary object type being reconciled.
type ForInput struct {
	object     client.Object
	predicates []predicate.Predicate
}

// OwnsInput represents an owned object type.
type OwnsInput struct {
	object     client.Object
	predicates []predicate.Predicate
}

// WatchesInput represents a custom watch.
type WatchesInput struct {
	src          source.Source
	eventhandler handler.EventHandler
	watchOpts    []builder.WatchesOption
}

// ForOption configures the For() builder call.
type ForOption = builder.ForOption

// OwnsOption configures the Owns() builder call.
type OwnsOption = builder.OwnsOption

// NewControllerManagedBy returns a new controller builder.
// The builder provides a fluent interface similar to controller-runtime's
// builder, but works with reconkit's store-backed manager.
func NewControllerManagedBy(mgr manager.Manager) *Builder {
	return &Builder{
		mgr:        mgr,
		ownsInputs: make([]OwnsInput, 0),
		watches:    make([]WatchesInput, 0),
	}
}

// For sets the primary object type being reconciled.
// The controller will watch this type and reconcile every instance.
func (b *Builder) For(obj client.Object, opts ...ForOption) *Builder {
	input := ForInput{object: obj}
	for _, opt := range opts {
		opt.ApplyToFor(&builder.ForInput{})
	}
	b.forInput = input
	return b
}

// Owns adds an owned object type to watch.
// When an owned object changes, the controller reconciles its owner
// (determined by OwnerReferences).
func (b *Builder) Owns(obj client.Object, opts ...OwnsOption) *Builder {
	input := OwnsInput{object: obj}
	for _, opt := range opts {
		opt.ApplyToOwns(&builder.OwnsInput{})
	}
	b.ownsInputs = append(b.ownsInputs, input)
	return b
}

// Watches adds a custom watch to the controller.
// This provides full control over the watch source and event mapping.
func (b *Builder) Watches(src source.Source, eventHandler handler.EventHandler, opts ...builder.WatchesOption) *Builder {
	b.watches = append(b.watches, WatchesInput{
		src:          src,
		eventhandler: eventHandler,
		watchOpts:    opts,
	})
	return b
}

// WithOptions configures controller options like max concurrent reconciles.
func (b *Builder) WithOptions(opts controller.Options) *Builder {
	b.ctrlOptions = opts
	return b
}

// Named sets the controller name for logging and metrics.
func (b *Builder) Named(name string) *Builder {
	b.name = name
	return b
}

// Complete builds the controller and registers it with the manager.
// The reconciler will be called for every reconciliation request.
func (b *Builder) Complete(r reconcile.Reconciler) error {
	// Determine controller name
	name := b.name
	if name == "" {
		if b.forInput.object != nil {
			gvk, err := b.mgr.GetClient().GroupVersionKindFor(b.forInput.object)
			if err != nil {
				return fmt.Errorf("failed to get GVK for primary object: %w", err)
			}
			name = fmt.Sprintf("%s-controller", gvk.Kind)
		} else {
			return fmt.Errorf("controller name required when For() is not specified")
		}
	}

	// Create the controller
	c, err := controller.New(name, b.mgr, b.ctrlOptions)
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	// Watch the primary object type
	if b.forInput.object != nil {
		src := source.Kind(
			b.mgr.GetCache(),
			b.forInput.object,
			&handler.TypedEnqueueRequestForObject[client.Object]{},
			b.forInput.predicates...,
		)
		if err := c.Watch(src); err != nil {
			return fmt.Errorf("failed to watch primary object: %w", err)
		}
	}

	// Watch owned object types
	for _, owns := range b.ownsInputs {
		src := source.Kind(
			b.mgr.GetCache(),
			owns.object,
			handler.TypedEnqueueRequestForOwner[client.Object](
				b.mgr.GetScheme(),
				b.mgr.GetRESTMapper(),
				b.forInput.object,
				handler.OnlyControllerOwner(),
			),
			owns.predicates...,
		)
		if err := c.Watch(src); err != nil {
			return fmt.Errorf("failed to watch owned object: %w", err)
		}
	}

	// Add custom watches
	for _, w := range b.watches {
		// For custom watches, we need to create a source that wraps the provided source
		// The controller-runtime controller expects a source.Source interface
		if err := c.Watch(w.src); err != nil {
			return fmt.Errorf("failed to add custom watch: %w", err)
		}
	}

	// Register controller as runnable
	return b.mgr.Add(c)
}

// Build is an alias for Complete for compatibility with some patterns.
func (b *Builder) Build(r reconcile.Reconciler) (controller.Controller, error) {
	// Create a typed controller and return it
	name := b.name
	if name == "" {
		if b.forInput.object != nil {
			gvk, err := b.mgr.GetClient().GroupVersionKindFor(b.forInput.object)
			if err != nil {
				return nil, fmt.Errorf("failed to get GVK for primary object: %w", err)
			}
			name = fmt.Sprintf("%s-controller", gvk.Kind)
		} else {
			return nil, fmt.Errorf("controller name required when For() is not specified")
		}
	}

	c, err := controller.New(name, b.mgr, b.ctrlOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create controller: %w", err)
	}

	// Set up watches similar to Complete
	if b.forInput.object != nil {
		src := source.Kind(
			b.mgr.GetCache(),
			b.forInput.object,
			&handler.TypedEnqueueRequestForObject[client.Object]{},
			b.forInput.predicates...,
		)
		if err := c.Watch(src); err != nil {
			return nil, fmt.Errorf("failed to watch primary object: %w", err)
		}
	}

	for _, owns := range b.ownsInputs {
		src := source.Kind(
			b.mgr.GetCache(),
			owns.object,
			handler.TypedEnqueueRequestForOwner[client.Object](
				b.mgr.GetScheme(),
				b.mgr.GetRESTMapper(),
				b.forInput.object,
				handler.OnlyControllerOwner(),
			),
			owns.predicates...,
		)
		if err := c.Watch(src); err != nil {
			return nil, fmt.Errorf("failed to watch owned object: %w", err)
		}
	}

	for _, w := range b.watches {
		if err := c.Watch(w.src); err != nil {
			return nil, fmt.Errorf("failed to add custom watch: %w", err)
		}
	}

	// Register with manager
	if err := b.mgr.Add(c); err != nil {
		return nil, err
	}

	return c, nil
}
