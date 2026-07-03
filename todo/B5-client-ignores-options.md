# B5. Client silently ignores all options

**Type:** Bug
**Priority:** Medium

`CreateOption`, `UpdateOption`, `DeleteOption`, `GetOption` accepted but unused.
`DryRunAll`, `Preconditions` (uid check), `PropagationPolicy` silently dropped.
