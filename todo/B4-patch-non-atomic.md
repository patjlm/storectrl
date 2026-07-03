# B4. Patch is non-atomic (Get then Update)

**Type:** Bug
**Priority:** Medium
**File:** `client.go:51-96`

Race window between Get (line 63) and Update (line 95). Two concurrent patches on
different fields will conflict. Inherent to architecture but should be documented.
