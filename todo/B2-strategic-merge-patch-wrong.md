# B2. StrategicMergePatch treated as regular MergePatch

**Type:** Bug
**Priority:** High
**File:** `client.go:82`

Strategic merge has different semantics (list merge keys, directive markers).
Controllers using strategic merge on list-type fields may see unexpected behavior.

```go
case types.MergePatchType, types.StrategicMergePatchType:
    patchedBytes, err = jsonpatch.MergePatch(currentBytes, patchBytes) // wrong for strategic
```
