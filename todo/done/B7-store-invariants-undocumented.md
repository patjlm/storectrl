# B7. Store interface invariants are undocumented

**Type:** Bug
**Priority:** High
**File:** `store.go`

The `Store` interface doesn't specify critical correctness requirements that backends
must satisfy:

- Revision monotonicity: Watch events must arrive in revision order
- Snapshot-consistent List: must return a point-in-time snapshot
- ResourceVersion format: cache.go:674 does ParseInt, assuming numeric
- Watch ordering: events for the same key must arrive in commit order
- No-op behavior: whether content-identical writes should burn a revision

Without these, each backend can interpret differently. The postgres backend gets this
right (I1-I8 invariants), but a new backend author has no guidance.
