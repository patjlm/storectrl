# F9. No finalizer/deletion workflow

**Type:** Feature
**Priority:** Medium

Objects immediately deleted. No `DeletionTimestamp` + finalizer processing.
Controllers using finalizers for cleanup won't work correctly.
