// Integration tests for ShardedStore.
//
// Requires a running PostgreSQL instance. Set TEST_POSTGRES_DSN to enable:
//
//	TEST_POSTGRES_DSN=postgres://user:pass@localhost/test go test ./postgres/...
//
// Tests run against the postgres-controller-backend schema (testdata/schema.sql).
// Each test cleans up its own rows via a unique GVK prefix so parallel runs are safe.
package postgres_test

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/patjlm/storectrl"
	"github.com/patjlm/storectrl/postgres"
	"github.com/patjlm/storectrl/storetest"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed testdata/schema.sql
var schemaSQL string

// TestMain starts a postgres container for the test run. If TEST_POSTGRES_DSN
// is already set (e.g. in CI), it is used as-is. If no container runtime is
// available the container start fails gracefully and all tests skip via
// skipIfNoPostgres.
func TestMain(m *testing.M) {
	if os.Getenv("TEST_POSTGRES_DSN") != "" {
		os.Exit(m.Run())
	}
	// Ryuk reaper container doesn't work reliably with rootless Podman.
	// Container cleanup is handled by defer pgc.Terminate on normal exit.
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	ctx := context.Background()
	pgc, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("postgres container unavailable (%v) — integration tests skipped", err)
		os.Exit(m.Run())
	}
	defer pgc.Terminate(ctx)
	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("postgres DSN: %v", err)
	}
	os.Setenv("TEST_POSTGRES_DSN", dsn)
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// Test types
// ---------------------------------------------------------------------------

var (
	pgTestGV = schema.GroupVersion{Group: "pg.test.dev", Version: "v1"}
)

type testWidget struct {
	storectrl.BaseObject `json:",inline"`
	Spec                 testWidgetSpec `json:"spec,omitempty"`
}

type testWidgetSpec struct {
	Color string `json:"color,omitempty"`
	Size  int    `json:"size,omitempty"`
}

func (w *testWidget) DeepCopyObject() runtime.Object {
	out := &testWidget{}
	w.BaseObject.DeepCopyInto(&out.BaseObject)
	out.Spec = w.Spec
	return out
}

type testWidgetList struct {
	storectrl.BaseList `json:",inline"`
	Items              []testWidget `json:"items"`
}

func (l *testWidgetList) DeepCopyObject() runtime.Object {
	out := &testWidgetList{}
	l.BaseList.DeepCopyInto(&out.BaseList)
	out.Items = make([]testWidget, len(l.Items))
	copy(out.Items, l.Items)
	return out
}

type testGadget struct {
	storectrl.BaseObject `json:",inline"`
}

func (g *testGadget) DeepCopyObject() runtime.Object {
	out := &testGadget{}
	g.BaseObject.DeepCopyInto(&out.BaseObject)
	return out
}

type testGadgetList struct {
	storectrl.BaseList `json:",inline"`
	Items              []testGadget `json:"items"`
}

func (l *testGadgetList) DeepCopyObject() runtime.Object {
	out := &testGadgetList{}
	l.BaseList.DeepCopyInto(&out.BaseList)
	out.Items = make([]testGadget, len(l.Items))
	copy(out.Items, l.Items)
	return out
}

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypes(pgTestGV, &testWidget{}, &testWidgetList{})
	metav1.AddToGroupVersion(s, pgTestGV)
	return s
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

func skipIfNoPostgres(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres integration tests")
	}
	return dsn
}

func applySchema(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, schemaSQL); err != nil {
		conn.Close(ctx)
		t.Fatalf("apply schema: %v", err)
	}
	return conn
}

// cleanGVK deletes all rows associated with a GVK prefix to isolate tests.
func cleanGVK(t *testing.T, conn *pgx.Conn, gvkPrefix string) {
	t.Helper()
	ctx := context.Background()
	conn.Exec(ctx, `DELETE FROM kubernetes_resources WHERE gvk LIKE $1`, gvkPrefix+"%")
	conn.Exec(ctx, `DELETE FROM gvk_bucket_counters WHERE gvk LIKE $1`, gvkPrefix+"%")
	conn.Exec(ctx, `DELETE FROM compaction_horizon WHERE gvk LIKE $1`, gvkPrefix+"%")
}

func newStore(t *testing.T, dsn string, scheme *runtime.Scheme, bucketID int, holderID string) *postgres.ShardedStore {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	store, err := postgres.New(ctx, postgres.Options{
		ConnStr:  dsn,
		BucketID: bucketID,
		Assign:   func(_, _ string) int { return bucketID },
		HolderID: holderID,
		Scheme:   scheme,
	})
	if err != nil {
		cancel()
		t.Fatalf("New store: %v", err)
	}
	store.Start(ctx)
	t.Cleanup(cancel)
	return store
}

func bucketSeq(t *testing.T, conn *pgx.Conn, bucketID int, gvkStr string) int64 {
	t.Helper()
	var seq int64
	err := conn.QueryRow(context.Background(),
		`SELECT COALESCE(current_seq, 0) FROM gvk_bucket_counters WHERE bucket_id = $1 AND gvk = $2`,
		bucketID, gvkStr).Scan(&seq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("read bucket seq: %v", err)
	}
	return seq
}

func expectEvent(t *testing.T, w storectrl.Watcher, want storectrl.EventType, wantName string) {
	t.Helper()
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatalf("watcher closed waiting for %s %s", want, wantName)
		}
		if ev.Type != want {
			t.Errorf("event type: got %s, want %s", ev.Type, want)
		}
		if wantName != "" && ev.Object.GetName() != wantName {
			t.Errorf("object name: got %s, want %s", ev.Object.GetName(), wantName)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for %s %s", want, wantName)
	}
}

// ---------------------------------------------------------------------------
// Conformance suite
// ---------------------------------------------------------------------------

// TestShardedStore_Conformance runs the standard Store conformance suite.
// Skips tests that rely on event-history replay or watch-channel overflow,
// which do not apply to a poll-based postgres backend.
func TestShardedStore_Conformance(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	const holderID = "conformance-test"
	gvkPrefix := "widget.storetest.dev"

	// Shared counter so each cfg.NewStore call gets a fresh table namespace.
	var storeSeq int

	cfg := storetest.Config{
		NewStore: func(scheme *runtime.Scheme) storectrl.Store {
			storeSeq++
			cleanGVK(t, conn, gvkPrefix)
			// Re-release existing lease so the new store can acquire it.
			conn.Exec(context.Background(), `
				DELETE FROM bucket_leases WHERE bucket_id = 0 AND holder = $1`, holderID)
			return newStore(t, dsn, scheme, 0, holderID)
		},
		SkipApply:                 true,
		SkipWatchOverflow:         true,
		SkipWatchEventHistory:     true,
		SkipConcurrencyWatchCount: true,
	}

	storetest.TestStore(t, cfg)
}

// ---------------------------------------------------------------------------
// Postgres-specific tests
// ---------------------------------------------------------------------------

// TestShardedStore_MonotonicBucketSeq verifies the core I3/I6 invariant:
// gvk_bucket_seq strictly increases with every write to the same GVK+bucket.
func TestShardedStore_MonotonicBucketSeq(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	gvkStr := fmt.Sprintf("%s/%s/%s", pgTestGV.Group, pgTestGV.Version, "testWidget")
	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 10`)

	store := newStore(t, dsn, newScheme(), 10, "mono-test")
	ctx := context.Background()

	var prev int64
	for i := 0; i < 5; i++ {
		w := &testWidget{Spec: testWidgetSpec{Color: "blue"}}
		w.Name = fmt.Sprintf("mono-%d", i)
		w.Namespace = "default"
		w.APIVersion = pgTestGV.String()
		w.Kind = "Widget"
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		seq := bucketSeq(t, conn, 10, gvkStr)
		if seq <= prev {
			t.Errorf("seq not monotonic: after create %d got %d, prev %d", i, seq, prev)
		}
		prev = seq
	}

	// Update one object → seq must advance.
	w := &testWidget{Spec: testWidgetSpec{Color: "blue"}}
	w.Name = "mono-0"
	w.Namespace = "default"
	w.APIVersion = pgTestGV.String()
	w.Kind = "Widget"
	if err := store.Get(ctx, client.ObjectKey{Namespace: "default", Name: "mono-0"}, w); err != nil {
		t.Fatalf("get: %v", err)
	}
	w.Spec.Color = "red"
	if err := store.Update(ctx, w); err != nil {
		t.Fatalf("update: %v", err)
	}
	seq := bucketSeq(t, conn, 10, gvkStr)
	if seq <= prev {
		t.Errorf("seq did not advance after update: got %d, prev %d", seq, prev)
	}
}

// TestShardedStore_Sharding verifies that two stores owning different buckets
// only see their own objects when listing.
func TestShardedStore_Sharding(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id IN (20, 21)`)

	scheme := newScheme()
	storeA := newStore(t, dsn, scheme, 20, "shard-a")
	storeB := newStore(t, dsn, scheme, 21, "shard-b")
	ctx := context.Background()

	makeWidget := func(name string, gv schema.GroupVersion) *testWidget {
		w := &testWidget{Spec: testWidgetSpec{Color: "blue"}}
		w.Name = name
		w.Namespace = "default"
		w.APIVersion = gv.String()
		w.Kind = "Widget"
		return w
	}

	// Objects created through each store land in their respective bucket.
	for i := 0; i < 3; i++ {
		if err := storeA.Create(ctx, makeWidget(fmt.Sprintf("a-%d", i), pgTestGV)); err != nil {
			t.Fatalf("storeA create: %v", err)
		}
		if err := storeB.Create(ctx, makeWidget(fmt.Sprintf("b-%d", i), pgTestGV)); err != nil {
			t.Fatalf("storeB create: %v", err)
		}
	}

	listA := &testWidgetList{}
	if err := storeA.List(ctx, listA); err != nil {
		t.Fatalf("storeA list: %v", err)
	}
	if len(listA.Items) != 3 {
		t.Errorf("storeA: want 3 objects, got %d", len(listA.Items))
	}
	for _, it := range listA.Items {
		if it.Name[0] != 'a' {
			t.Errorf("storeA sees object from wrong bucket: %s", it.Name)
		}
	}

	listB := &testWidgetList{}
	if err := storeB.List(ctx, listB); err != nil {
		t.Fatalf("storeB list: %v", err)
	}
	if len(listB.Items) != 3 {
		t.Errorf("storeB: want 3 objects, got %d", len(listB.Items))
	}
	for _, it := range listB.Items {
		if it.Name[0] != 'b' {
			t.Errorf("storeB sees object from wrong bucket: %s", it.Name)
		}
	}
}

// TestShardedStore_LeaseFencing verifies that writes fail with ErrFenced when
// the bucket lease is lost (deleted from under the store).
func TestShardedStore_LeaseFencing(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 30`)

	store := newStore(t, dsn, newScheme(), 30, "fencing-test")
	ctx := context.Background()

	// Verify write succeeds while lease is held.
	w := &testWidget{Spec: testWidgetSpec{Color: "blue"}}
	w.Name = "fence-obj"
	w.Namespace = "default"
	w.APIVersion = pgTestGV.String()
	w.Kind = "Widget"
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("initial create: %v", err)
	}

	// Force-delete the lease, simulating a fencing event.
	conn.Exec(ctx, `DELETE FROM bucket_leases WHERE bucket_id = 30`)

	// Attempt a write without a valid lease — must fail with ErrFenced.
	w2 := &testWidget{Spec: testWidgetSpec{Color: "red"}}
	w2.Name = "fence-obj2"
	w2.Namespace = "default"
	w2.APIVersion = pgTestGV.String()
	w2.Kind = "Widget"
	err := store.Create(ctx, w2)
	var fenced *storectrl.FencedError
	if !errors.As(err, &fenced) {
		t.Errorf("expected FencedError after lease deleted, got %v", err)
	}
}

// TestShardedStore_OptimisticLocking verifies that two concurrent updates with
// the same object_version produce exactly one ConflictError.
func TestShardedStore_OptimisticLocking(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 40`)

	store := newStore(t, dsn, newScheme(), 40, "locking-test")
	ctx := context.Background()

	w := &testWidget{Spec: testWidgetSpec{Color: "blue"}}
	w.Name = "lock-obj"
	w.Namespace = "default"
	w.APIVersion = pgTestGV.String()
	w.Kind = "Widget"
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Fetch the same object twice — both at the same object_version.
	r1, r2 := &testWidget{}, &testWidget{}
	r1.APIVersion = pgTestGV.String()
	r1.Kind = "Widget"
	r2.APIVersion = pgTestGV.String()
	r2.Kind = "Widget"
	key := client.ObjectKey{Namespace: "default", Name: "lock-obj"}
	if err := store.Get(ctx, key, r1); err != nil {
		t.Fatalf("get r1: %v", err)
	}
	if err := store.Get(ctx, key, r2); err != nil {
		t.Fatalf("get r2: %v", err)
	}

	r1.Spec.Color = "red"
	if err := store.Update(ctx, r1); err != nil {
		t.Fatalf("r1 update: %v", err)
	}

	r2.Spec.Color = "green"
	err := store.Update(ctx, r2)

	var conflict *storectrl.ConflictError
	if !errors.As(err, &conflict) {
		t.Errorf("expected ConflictError from stale update, got %v", err)
	}

	// Winning value must be r1's.
	got := &testWidget{}
	got.APIVersion = pgTestGV.String()
	got.Kind = "Widget"
	if err := store.Get(ctx, key, got); err != nil {
		t.Fatalf("get after conflict: %v", err)
	}
	if got.Spec.Color != "red" {
		t.Errorf("after conflict: want color=red, got %s", got.Spec.Color)
	}
}

// TestShardedStore_WatchRevisionTooOld verifies that Watch returns
// RevisionTooOldError when hwm is at or below the compaction horizon,
// so the cache relists instead of spinning.
func TestShardedStore_WatchRevisionTooOld(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 50`)

	store := newStore(t, dsn, newScheme(), 50, "compaction-test")
	ctx := context.Background()
	gvkStr := fmt.Sprintf("%s/%s/%s", pgTestGV.Group, pgTestGV.Version, "testWidget")

	// Create a few objects to advance the bucket sequence.
	for i := 0; i < 5; i++ {
		w := &testWidget{Spec: testWidgetSpec{Color: "blue"}}
		w.Name = fmt.Sprintf("compact-%d", i)
		w.Namespace = "default"
		w.APIVersion = pgTestGV.String()
		w.Kind = "Widget"
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	// Set compaction horizon to seq=3 (objects 0-3 are "compacted away").
	conn.Exec(ctx, `
		INSERT INTO compaction_horizon (bucket_id, gvk, compacted_seq)
		VALUES (50, $1, 3)
		ON CONFLICT (bucket_id, gvk) DO UPDATE SET compacted_seq = 3`,
		gvkStr)

	// Watch from hwm=1 must return RevisionTooOldError immediately.
	_, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(1))
	var rvErr *storectrl.RevisionTooOldError
	if !errors.As(err, &rvErr) {
		t.Fatalf("expected RevisionTooOldError for hwm=1, got %v", err)
	}
	if rvErr.RequestedRevision != 1 {
		t.Errorf("RequestedRevision: got %d, want 1", rvErr.RequestedRevision)
	}
	if rvErr.OldestRevision != 4 {
		t.Errorf("OldestRevision: got %d, want 4", rvErr.OldestRevision)
	}

	// Watch from hwm=3 must also fail (at or below horizon).
	_, err = store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(3))
	if !errors.As(err, &rvErr) {
		t.Fatalf("expected RevisionTooOldError for hwm=3, got %v", err)
	}

	// Watch from hwm=4 must succeed (above horizon).
	watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(4))
	if err != nil {
		t.Fatalf("Watch from hwm=4 should succeed: %v", err)
	}
	watcher.Stop()

	// Watch from hwm=0 must always succeed (no compaction check for 0).
	watcher, err = store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(0))
	if err != nil {
		t.Fatalf("Watch from hwm=0 should always succeed: %v", err)
	}
	watcher.Stop()
}

// TestShardedStore_NoOpSuppression verifies that a content-equal write does not
// increment gvk_bucket_seq (the stored-proc suppression invariant).
func TestShardedStore_NoOpSuppression(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 60`)

	store := newStore(t, dsn, newScheme(), 60, "suppress-test")
	ctx := context.Background()
	gvkStr := fmt.Sprintf("%s/%s/%s", pgTestGV.Group, pgTestGV.Version, "testWidget")

	w := &testWidget{Spec: testWidgetSpec{Color: "blue", Size: 10}}
	w.Name = "supp-obj"
	w.Namespace = "default"
	w.APIVersion = pgTestGV.String()
	w.Kind = "Widget"
	if err := store.Create(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	seqAfterCreate := bucketSeq(t, conn, 60, gvkStr)

	// Read back to get correct object_version annotation.
	got := &testWidget{}
	got.APIVersion = pgTestGV.String()
	got.Kind = "Widget"
	if err := store.Get(ctx, client.ObjectKey{Namespace: "default", Name: "supp-obj"}, got); err != nil {
		t.Fatalf("get: %v", err)
	}

	// Update with identical content — should be suppressed (no seq increment).
	if err := store.Update(ctx, got); err != nil {
		t.Fatalf("no-op update: %v", err)
	}

	seqAfterNoOp := bucketSeq(t, conn, 60, gvkStr)
	if seqAfterNoOp != seqAfterCreate {
		t.Errorf("no-op write incremented seq: before=%d after=%d", seqAfterCreate, seqAfterNoOp)
	}

	// Update with different content — must increment seq.
	got2 := &testWidget{}
	got2.APIVersion = pgTestGV.String()
	got2.Kind = "Widget"
	if err := store.Get(ctx, client.ObjectKey{Namespace: "default", Name: "supp-obj"}, got2); err != nil {
		t.Fatalf("get2: %v", err)
	}
	got2.Spec.Color = "red"
	if err := store.Update(ctx, got2); err != nil {
		t.Fatalf("real update: %v", err)
	}

	seqAfterReal := bucketSeq(t, conn, 60, gvkStr)
	if seqAfterReal <= seqAfterCreate {
		t.Errorf("real write did not increment seq: before=%d after=%d", seqAfterCreate, seqAfterReal)
	}
}

// TestShardedStore_WatchEvents verifies end-to-end event streaming and
// watch resumption from a bucket HWM.
func TestShardedStore_WatchEvents(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 70`)

	store := newStore(t, dsn, newScheme(), 70, "watch-test")
	ctx := context.Background()

	t.Run("streams_CRUD_events", func(t *testing.T) {
		watcher, err := store.Watch(ctx, &testWidgetList{})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer watcher.Stop()

		w := newPGWidget("watch-crud", "blue")
		if err := store.Create(ctx, w); err != nil {
			t.Fatalf("create: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventAdded, "watch-crud")

		w.Spec.Color = "red"
		if err := store.Update(ctx, w); err != nil {
			t.Fatalf("update: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventModified, "watch-crud")

		if err := store.Delete(ctx, w); err != nil {
			t.Fatalf("delete: %v", err)
		}
		expectEvent(t, watcher, storectrl.EventDeleted, "watch-crud")
	})

	t.Run("resume_from_hwm", func(t *testing.T) {
		// Create two objects, note list HWM, then create a third.
		w1 := newPGWidget("hwm-a", "blue")
		w2 := newPGWidget("hwm-b", "blue")
		if err := store.Create(ctx, w1); err != nil {
			t.Fatalf("create w1: %v", err)
		}
		if err := store.Create(ctx, w2); err != nil {
			t.Fatalf("create w2: %v", err)
		}

		list := &testWidgetList{}
		if err := store.List(ctx, list); err != nil {
			t.Fatalf("list: %v", err)
		}
		hwm, err := strconv.ParseInt(list.GetResourceVersion(), 10, 64)
		if err != nil {
			t.Fatalf("parse RV: %v", err)
		}

		w3 := newPGWidget("hwm-c", "green")
		if err := store.Create(ctx, w3); err != nil {
			t.Fatalf("create w3: %v", err)
		}

		// Watch from captured HWM should only deliver w3.
		watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(hwm))
		if err != nil {
			t.Fatalf("watch from hwm: %v", err)
		}
		defer watcher.Stop()

		expectEvent(t, watcher, storectrl.EventAdded, "hwm-c")

		// Verify no extra events within a brief window.
		select {
		case ev := <-watcher.ResultChan():
			t.Errorf("unexpected extra event: %s %s", ev.Type, ev.Object.GetName())
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("label_selector_filter", func(t *testing.T) {
		blue := newPGWidget("sel-blue", "blue")
		blue.Labels = map[string]string{"color": "blue"}
		red := newPGWidget("sel-red", "red")
		red.Labels = map[string]string{"color": "red"}
		if err := store.Create(ctx, blue); err != nil {
			t.Fatalf("create blue: %v", err)
		}
		if err := store.Create(ctx, red); err != nil {
			t.Fatalf("create red: %v", err)
		}

		watcher, err := store.Watch(ctx, &testWidgetList{},
			client.MatchingLabels{"color": "blue"},
		)
		if err != nil {
			t.Fatalf("watch with selector: %v", err)
		}
		defer watcher.Stop()

		// Only blue should arrive; red must not show up.
		expectEvent(t, watcher, storectrl.EventAdded, "sel-blue")
		select {
		case ev := <-watcher.ResultChan():
			t.Errorf("unexpected event for non-matching label: %s %s", ev.Type, ev.Object.GetName())
		case <-time.After(200 * time.Millisecond):
		}
	})
}

// TestShardedStore_ListResourceVersion verifies that the list metadata
// ResourceVersion is the bucket HWM and is usable as WatchFromRevision.
func TestShardedStore_ListResourceVersion(t *testing.T) {
	dsn := skipIfNoPostgres(t)
	conn := applySchema(t, dsn)
	defer conn.Close(context.Background())

	cleanGVK(t, conn, pgTestGV.Group)
	conn.Exec(context.Background(), `DELETE FROM bucket_leases WHERE bucket_id = 80`)

	store := newStore(t, dsn, newScheme(), 80, "rv-test")
	ctx := context.Background()

	list := &testWidgetList{}
	if err := store.List(ctx, list); err != nil {
		t.Fatalf("empty list: %v", err)
	}
	if list.GetResourceVersion() != "0" {
		t.Errorf("empty list RV: got %s, want 0", list.GetResourceVersion())
	}

	if err := store.Create(ctx, newPGWidget("rv-a", "blue")); err != nil {
		t.Fatalf("create: %v", err)
	}

	list = &testWidgetList{}
	if err := store.List(ctx, list); err != nil {
		t.Fatalf("list after create: %v", err)
	}
	rv, err := strconv.ParseInt(list.GetResourceVersion(), 10, 64)
	if err != nil || rv < 1 {
		t.Errorf("list RV after create: got %q, want >= 1", list.GetResourceVersion())
	}

	// Confirm the RV is usable as WatchFromRevision without error.
	watcher, err := store.Watch(ctx, &testWidgetList{}, storectrl.WatchFromRevision(rv))
	if err != nil {
		t.Fatalf("Watch from list RV: %v", err)
	}
	watcher.Stop()
}

func newPGWidget(name, color string) *testWidget {
	w := &testWidget{Spec: testWidgetSpec{Color: color}}
	w.Name = name
	w.Namespace = "default"
	w.APIVersion = pgTestGV.String()
	w.Kind = "Widget"
	return w
}
