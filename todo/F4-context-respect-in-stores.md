# F4. No context.Context respect in stores

**Type:** Feature
**Priority:** High

Memory store ignores context cancellation/deadline. Filesystem uses blocking I/O
(`os.ReadFile`/`os.WriteFile`). Long lists or writes can't be cancelled.
