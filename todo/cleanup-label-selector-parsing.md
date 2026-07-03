# Cleanup: Deduplicate label selector parsing in StoreListerWatcher

## Summary

`ListWithContext` (lines 46-51) and `WatchWithContext` (lines 68-73) both contain identical 6-line blocks for parsing `LabelSelector` from `metav1.ListOptions` into `MatchingLabelsSelector`.

## Current duplication

```go
// In ListWithContext (lines 46-51):
if options.LabelSelector != "" {
    sel, err := labels.Parse(options.LabelSelector)
    if err != nil {
        return nil, err
    }
    opts = append(opts, client.MatchingLabelsSelector{Selector: sel})
}

// In WatchWithContext (lines 68-73): identical block
```

## Fix

Extract to a shared helper:

```go
func parseListOpts(options metav1.ListOptions) ([]client.ListOption, error) {
    var opts []client.ListOption
    if options.LabelSelector != "" {
        sel, err := labels.Parse(options.LabelSelector)
        if err != nil {
            return nil, err
        }
        opts = append(opts, client.MatchingLabelsSelector{Selector: sel})
    }
    return opts, nil
}
```

This also provides a natural place to add field selector or other option parsing later.

## Severity

Low. Maintenance cost only — if parsing logic changes, two call sites must be updated.
