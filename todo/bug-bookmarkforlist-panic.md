# Bug: bookmarkForList panics on ObjectList without Items field

## Summary

`bookmarkForList` uses `reflect.Value.FieldByName("Items")` without checking the returned value is valid. If the list type lacks an `Items` field, `.Type()` panics with `"reflect: call of reflect.Value.Type on zero Value"`.

## Impact

Low. In practice this is guarded by `apimeta.ExtractList` (called earlier on the same type), which also requires an `Items` field and returns an error. So `bookmarkForList` is only reached after `ExtractList` succeeds, meaning `Items` exists. All standard Kubernetes list types have `Items`.

The risk is theoretical: a custom `client.ObjectList` implementation without `Items` that somehow passes `ExtractList` (unlikely).

## Location

`listerwatcher.go:280-281`:

```go
itemsField := listVal.FieldByName("Items")
obj := reflect.New(itemsField.Type().Elem()).Interface().(client.Object)
```

## Fix

Add a validity check:

```go
itemsField := listVal.FieldByName("Items")
if !itemsField.IsValid() {
    return nil // or return a minimal runtime.Object with just RV set
}
```

Or use `apimeta.ExtractList` on `s.listObj` first and derive the element type from the extracted items instead of reflection.

## Severity

Low. Guarded in practice. Defensive check would be nice but not urgent.
