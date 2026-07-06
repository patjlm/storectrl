# PostgreSQL Backend — Design Reference

This backend wraps [`github.com/jmelis/postgres-controller-backend`](https://github.com/jmelis/postgres-controller-backend) to implement `storectrl.Store` against PostgreSQL 16+ Multi-AZ. It targets fleet-scale controllers (5K–50K managed clusters) that own their write path and don't need the Kubernetes API server.

## Core Problem

etcd and the API server provide three things controllers silently depend on:
1. **Monotonic, commit-ordered revision numbers** (no gaps, no reordering)
2. **Single-writer fencing** (RBAC + admission prevent concurrent mutations)
3. **Reliable watch stream** (informer cache never misses an update)

A PostgreSQL backend must replicate all three without etcd's built-in MVCC revision log. The upstream module does this through 8 named correctness invariants (I1–I8), each with deterministic race tests.

## Monotonic Index — Never Skip Updates

### Counter Row with Exclusive Lock

Heart of the design. Per `(GVK, bucket)` counter row in `gvk_bucket_counters`, incremented inside the same transaction as the resource upsert:

```sql
INSERT INTO gvk_bucket_counters (gvk, bucket_id, current_seq)
VALUES ($1, $2, 1)
ON CONFLICT (gvk, bucket_id) DO UPDATE
SET current_seq = gvk_bucket_counters.current_seq + 1
RETURNING current_seq
```

This takes an **exclusive row lock** held until COMMIT. Two concurrent writers to the same `(GVK, bucket)` serialize at this lock:

- Writer A increments to seq 5, holds lock
- Writer B blocks until A commits
- B gets seq 6 and commits after A

Therefore `seq(A) < seq(B)` guarantees A committed first — sequence order equals commit order, with no gaps. A plain PostgreSQL `SEQUENCE` cannot do this because sequences are non-transactional.

### Gap Prevention on Abort

Counter increment and resource upsert live in the same transaction (stored procedure `pgctl_write()`). If the upsert fails (e.g., 409 conflict from `object_version` mismatch — `P0002`), the entire call aborts, rolling back the counter. No gap is left.

### No-Op Suppression

Content-equal writes detected before the counter touch. The stored procedure reads the existing row by PK; if content matches (JSONB `=`), it returns immediately — no counter increment, no upsert, no LISTEN/NOTIFY doorbell. Prevents sequence burn from bridge scenarios where status appliers rewrite identical content every ~3 minutes.

### Composite ResourceVersion

The `resourceversion.RV` type is a vector clock: `e{epoch}|b{id}:{seq},...`

Example: `e7|b2:1044,b5:902` means "epoch 7, seen up to seq 1044 in bucket 2, seq 902 in bucket 5." Serialization is canonical (buckets sorted ascending).

The `ShardedStore` in this package extracts the single-bucket HWM for its owned bucket and exposes it as a plain `int64` to storectrl's `WatchFromRevision`.

### Writer Regression Tripwire (I3)

Each writer caches its highest committed seq per `(GVK, bucket)`. On reconnect, reads `current_seq` from the counter table and refuses to write if lower. Defense against failover to a standby with stale counters — should never fire with synchronous Multi-AZ replication.

## Sharding

### Bucket Assignment

Sharding key is `(GVK, bucket_id)`. The `BucketAssigner` function maps `(namespace, name) → bucket_id`. For fixed-scheme resources: `hash(key) % bucketCount`. For DynamoDB-bridged resources: per-Management-Cluster buckets at `MC_BASE(4096) + mc_index`.

Bucket count is fixed at deployment time (recommended: 64). Changing it is an epoch-bump migration — all watchers get `410 Gone` and must relist.

### Lease-Based Fencing

`bucket_leases` table with PK `(bucket_id, domain)`. Each bucket has independent `spec` and `status` lease rows. `ShardedStore` acquires both atomically on startup.

| Operation | Mechanism |
|-----------|-----------|
| Acquire | `INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at < now() OR holder = $me` |
| Renew | `UPDATE ... SET expires_at = now() + ttl WHERE holder = $me` — every 10s for 30s TTL |
| Release | `DELETE ... WHERE holder = $me` — on graceful shutdown |

**Fencing guarantee:** Writers take `FOR SHARE` lock on lease row during writes. Lease grant takes an exclusive lock — conflicts with any in-flight writer. Time-based TTL is advisory; the **lock is the real safety mechanism**.

### Failover

No auto-rebalancer. Dead replica's leases expire in 30s, peer claims them. Graceful shutdown releases immediately for instant handover. `ShardedStore.Start()` attempts re-acquisition if renewal fails.

## Watch/Notify Pipeline

Single-goroutine poll-primary design. LISTEN/NOTIFY is a **latency-only doorbell**, not the correctness path.

### Poll Cycle

One `REPEATABLE READ, READ ONLY` transaction covering:
1. Epoch check — `cluster_epoch.timeline_id` mismatch → `410 Gone`
2. Compaction horizon check — HWM below compacted_seq → `410 Gone`
3. Per-bucket query: `WHERE gvk_bucket_seq > $hwm ORDER BY gvk_bucket_seq ASC`
4. Event classification: `deletion_timestamp != nil` → Deleted; `object_version == 1` → Added; else Modified
5. Advance HWM per bucket

### Doorbell + Baseline Timer

| Trigger | Latency | Purpose |
|---------|---------|---------|
| LISTEN/NOTIFY doorbell | ~25ms p50 | Fast path — nudges poll on commit |
| 5s baseline timer | ≤5s worst case | Liveness backstop — catches total doorbell loss |

Debounce: leading + trailing edge with 100ms floor. 30 rapid doorbells coalesce to ≤2 polls.

Doorbell fires **after** COMMIT in a separate `SELECT pg_notify()` statement. Lost doorbells (crash between COMMIT and notify) are harmless — baseline timer compensates.

### Failure Modes

| Failure | Detection | Recovery |
|---------|-----------|----------|
| LISTEN connection drops | `WaitForNotification` error | Reconnect with exponential backoff (100ms–5s), immediate nudge after |
| Total doorbell loss | Baseline timer fires | All events delivered within 5s |
| Epoch change (failover) | `poll()` checks `timeline_id` | `410 Gone` — caller must relist |
| Compaction past HWM | `compacted_seq` check | `410 Gone` |
| Mid-poll compaction | REPEATABLE READ snapshot | Invisible within poll cycle |

### Watch Bridge in ShardedStore

`watchBridge` translates `crbridge.WatchInterface` events into `storectrl.Event` values, applying label selector filtering and type conversion. Buffered at 256 events.

## Soft Deletes and Compaction

PostgreSQL `DELETE` removes rows — watchers polling `WHERE seq > hwm` would never see the deletion (ghost objects). Solution:

1. **Soft delete** via `deletion_timestamp` column
2. **Compaction** after 24h retention — atomic CTE advances `compaction_horizon` in the same statement as the physical delete
3. **410 Gone** for watchers whose HWM is below the horizon

`ShardedStore.checkCompactionHorizon()` queries the horizon before starting a watch to fail fast instead of silently missing events.

## Stored Procedure Performance

`pgctl_write()` — fence + suppress + counter + upsert in one server-side PL/pgSQL call. Returns per-step timings for Prometheus histograms without extra round-trips.

- Round-trips reduced from 5 to 2 (BEGIN + function call, COMMIT)
- Write throughput: ~685 → 9,622 w/s at 64 buckets
- Per-write latency: p50=6.1ms, p99=13.2ms (COMMIT = ~61% — WAL sync to synchronous standby)
- `fillfactor=50` on counter table guarantees HOT updates (no index maintenance on counter bumps)

## Ambiguous Commit Handling

Connection drop during COMMIT → `AmbiguousCommitError` wrapping the seq number. `ReadBack()` checks if the row exists at expected seq. Found → success. Not found → `ErrConflict` for retry.

## Correctness Invariants

| ID | Invariant |
|----|-----------|
| I1 | Gapless issuance within (GVK, bucket) |
| I2 | Commit order = sequence order |
| I3 | `current_seq` never decreases (including across failover) |
| I4 | Single writer per (bucket, domain) at any instant |
| I5 | Exactly-once delivery per state change (coalescing permitted) |
| I6 | Composite RV never moves backwards |
| I7 | Watcher can never silently skip a compacted event |
| I8 | Stale `object_version` rejected (409) |
