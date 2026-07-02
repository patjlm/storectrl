package storectrl

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Error types implement the APIStatus interface so that
// apierrors.IsNotFound(), apierrors.IsAlreadyExists(), and
// apierrors.IsConflict() work transparently with storectrl errors.
// This lets existing controller code keep using the standard error
// checks without modification.

// NotFoundError indicates the requested object does not exist.
type NotFoundError struct {
	Key string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("object not found: %s", e.Key)
}

func (e *NotFoundError) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonNotFound,
		Message: e.Error(),
		Code:    404,
	}
}

// AlreadyExistsError indicates the object already exists.
type AlreadyExistsError struct {
	Key string
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("object already exists: %s", e.Key)
}

func (e *AlreadyExistsError) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonAlreadyExists,
		Message: e.Error(),
		Code:    409,
	}
}

// ConflictError indicates an optimistic concurrency conflict
// (ResourceVersion mismatch).
type ConflictError struct {
	Key string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict: object was modified: %s", e.Key)
}

func (e *ConflictError) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonConflict,
		Message: e.Error(),
		Code:    409,
	}
}

// RevisionTooOldError indicates the requested watch revision has been
// compacted out of the event log — equivalent to HTTP 410 Gone in the
// Kubernetes API. Callers should relist and restart the watch.
type RevisionTooOldError struct {
	RequestedRevision int64
	OldestRevision    int64
}

func (e *RevisionTooOldError) Error() string {
	return fmt.Sprintf("requested revision %d is too old, oldest available is %d", e.RequestedRevision, e.OldestRevision)
}

func (e *RevisionTooOldError) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Reason:  metav1.StatusReasonGone,
		Message: e.Error(),
		Code:    410,
	}
}
