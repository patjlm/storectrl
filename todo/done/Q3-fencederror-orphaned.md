# Q3. FencedError is orphaned in core package

**Type:** Code Quality
**Priority:** Low
**File:** `errors.go:91-111`

`FencedError` exists in the core package but isn't referenced in the `Store` interface
docs. Only used by the postgres backend. Either:

- Document it in Store as a possible error from Create/Update/Delete (if fencing is generic)
- Move it to the postgres package (if it's backend-specific)

For now: add it to Store interface docs since sharding backends will need it.
