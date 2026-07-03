# Q1. Heavy duplication between backends (~200 lines)

**Type:** Code Quality
**Priority:** High

`labelSet` type, `toLabelSet`, `gvkForObject`, `gvkForList`, `populateListItems`,
`eventLogEntry`, `logEvent`, `eventsSince`, watcher pattern — nearly identical in
`memory/` and `filesystem/`. Plus third `objectLabelSet` in `listerwatcher.go`.

Extract to shared internal package.
