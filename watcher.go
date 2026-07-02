package reconkit

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// EventType describes the type of change that occurred.
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
	EventBookmark EventType = "BOOKMARK"
)

// Event represents a change to an object in the Store.
type Event struct {
	Type   EventType
	Object client.Object
}

// Watcher streams change events from a Store.
// Callers must call Stop when done to release resources.
type Watcher interface {
	// ResultChan returns a channel that receives events.
	// The channel is closed when Stop is called or the watch errors.
	ResultChan() <-chan Event

	// Stop stops the watcher and closes the result channel.
	Stop()
}
