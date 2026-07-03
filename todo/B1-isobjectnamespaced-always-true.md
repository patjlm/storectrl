# B1. `IsObjectNamespaced` always returns `true`

**Type:** Bug
**Priority:** High
**File:** `client.go:177`

Cluster-scoped resources misclassified. Controllers calling this (some do for
building watches/caches) get wrong answers.

```go
func (c *storeClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
    return true, nil // always true regardless of resource scope
}
```
