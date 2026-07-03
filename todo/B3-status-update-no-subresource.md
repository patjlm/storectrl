# B3. Status update identical to spec update

**Type:** Bug
**Priority:** High
**File:** `client.go:192-193`

No subresource separation. `r.Status().Update()` bumps RV on the whole object,
causing conflicts with concurrent spec updates. Controllers doing status updates
in reconcile loops will fight with themselves.
