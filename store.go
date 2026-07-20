package storectrl

import (
	"context"

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
//
// # Backend invariants
//
// ResourceVersion must be a numeric string parseable via strconv.ParseInt.
// It increases monotonically with each mutation — no gaps are required but
// the value must never decrease.
//
// List must return a snapshot-consistent view: all returned objects reflect
// state at a single point in time.
//
// Watch events must be delivered in revision order. When WatchFromRevision
// is supplied, the backend must not skip any events between that revision
// and the current state.
//
// Optimistic concurrency: Update must reject an object whose ResourceVersion
// does not match the stored value with a ConflictError.
//
// No-op suppression is recommended but not required. If the new object is
// byte-identical to the stored one, the backend may skip the revision bump
// and event emission.
//
// Backends supporting sharded or fenced writes may return FencedError from
// Create, Update, or Delete when the caller's lease is lost.
type Store interface {
	// Get retrieves an object by namespace/name key and populates obj.
	// Returns a NotFoundError if the object does not exist.
	Get(ctx context.Context, key client.ObjectKey, obj client.Object) error

	// List populates the given ObjectList with objects matching the options.
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error

	// Create persists a new object. Implementations should set UID and
	// ResourceVersion on the object before returning.
	// Returns an AlreadyExistsError if the object already exists.
	// May return FencedError for backends with lease-based fencing.
	Create(ctx context.Context, obj client.Object) error

	// Update writes spec + metadata from the input object. Status fields in
	// the input are silently ignored — the stored status is preserved.
	// Increments metadata.generation when spec content changes; no-op
	// updates (identical spec) must not increment generation or bump
	// ResourceVersion.
	// Returns a ConflictError if the ResourceVersion does not match.
	// May return FencedError for backends with lease-based fencing.
	Update(ctx context.Context, obj client.Object) error

	// UpdateStatus writes status from the input object. Spec and metadata
	// fields in the input are silently ignored — the stored spec is preserved.
	// Does NOT increment metadata.generation.
	// Returns a ConflictError if the ResourceVersion does not match.
	// Returns a NotFoundError if the object does not exist.
	// May return FencedError for backends with lease-based fencing.
	UpdateStatus(ctx context.Context, obj client.Object) error

	// Delete removes an object. Works with or without ResourceVersion
	// (empty RV = unconditional delete).
	// Returns a NotFoundError if not found.
	// May return FencedError for backends with lease-based fencing.
	Delete(ctx context.Context, obj client.Object) error

	// Watch returns a Watcher that streams change events for the given type.
	// The list parameter determines the type being watched.
	//
	// Backends should check opts for WatchFromRevision. When present, replay
	// events after that revision before switching to live events. If the
	// revision has been compacted, return RevisionTooOldError so callers
	// can relist and restart.
	Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (Watcher, error)
}
