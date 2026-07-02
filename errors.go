package reconkit

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Error types implement the APIStatus interface so that
// apierrors.IsNotFound(), apierrors.IsAlreadyExists(), and
// apierrors.IsConflict() work transparently with reconkit errors.
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
