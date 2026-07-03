package storectrl

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WatchFromRevision is a client.ListOption that tells Store.Watch to replay
// events after the given revision before switching to live events.
// Mirrors the Kubernetes resourceVersion-based watch resumption.
// If the revision is too old (compacted), the store returns RevisionTooOldError.
type WatchFromRevision int64

func (w WatchFromRevision) ApplyToList(_ *client.ListOptions) {}

// EnableWatchBookmarks is a client.ListOption that requests the backend to
// send periodic BOOKMARK events during Watch. Backends that support bookmarks
// honor this; others may ignore it. Bookmarks carry no object data — just a
// resourceVersion — and help the cache track progress to avoid full relists
// on reconnection.
type EnableWatchBookmarks bool

func (e EnableWatchBookmarks) ApplyToList(_ *client.ListOptions) {}

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
	//
	// Backends should check opts for WatchFromRevision. When present, replay
	// events after that revision before switching to live events. If the
	// revision has been compacted, return RevisionTooOldError so callers
	// can relist and restart.
	Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (Watcher, error)

	// Apply performs a server-side-apply-style patch. The apply configuration
	// declares the desired field values; the store merges them using field
	// ownership semantics appropriate for the backend.
	//
	// Backends that do not support apply should return a clear error
	// (e.g. fmt.Errorf("apply not supported")).
	Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error
}
