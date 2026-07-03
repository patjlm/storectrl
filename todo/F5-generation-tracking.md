# F5. No generation tracking

**Type:** Feature
**Priority:** Medium

`ObjectMeta.Generation` not bumped on spec changes. Controllers relying on
`.status.observedGeneration` pattern won't work.
