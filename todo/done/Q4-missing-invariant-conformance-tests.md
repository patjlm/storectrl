# Q4. Missing invariant conformance tests

**Type:** Code Quality
**Priority:** Medium

The existing test suite doesn't verify Store invariants learned from the postgres
backend (I1-I8 pattern):

- Revision monotonicity in watch streams
- No-op suppression behavior
- Snapshot consistency of List
- Watch resumption correctness (events since revision, no gaps)
- Optimistic concurrency under concurrent writers

These should be generic conformance tests runnable against any Store backend.
