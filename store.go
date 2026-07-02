package reconkit

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Store is the interface backend implementations must satisfy.
// It abstracts the data persistence layer so controllers can work
// against any datastore (SQL, GCP APIs, in-memory, etc.) instead of
// requiring a Kubernetes API server.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Get retrieves an object by namespace/name key and populates obj.
	// Returns a NotFoundError if the object does not exist.
	Get(ctx context.Context, key client.ObjectKey, obj client.Object) error

	// List populates the given ObjectList with objects matching the options.
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error

	// Create persists a new object. Implementations should set UID and
	// ResourceVersion on the object before returning.
	// Returns an AlreadyExistsError if the object already exists.
	Create(ctx context.Context, obj client.Object) error

	// Update replaces an existing object. Returns a ConflictError if the
	// ResourceVersion does not match (optimistic concurrency).
	Update(ctx context.Context, obj client.Object) error

	// Delete removes an object. Returns a NotFoundError if not found.
	Delete(ctx context.Context, obj client.Object) error

	// Watch returns a Watcher that streams change events for the given type.
	// The list parameter determines the type being watched.
	Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (Watcher, error)

	// Apply performs a server-side-apply-style patch. The apply configuration
	// declares the desired field values; the store merges them using field
	// ownership semantics appropriate for the backend.
	//
	// Backends that do not support apply should return a clear error
	// (e.g. fmt.Errorf("apply not supported")).
	Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error
}
