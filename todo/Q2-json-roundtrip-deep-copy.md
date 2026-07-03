# Q2. JSON round-trip for deep copy

**Type:** Code Quality
**Priority:** High

Used in `MemoryStore.Get` (`memory/store.go:97`), `storeInformer.get`
(`cache.go:896-902`). `DeepCopyObject()` is faster. At high object counts with
frequent Get calls, this is a meaningful perf tax. Cache reads should not pay
marshal/unmarshal cost.
