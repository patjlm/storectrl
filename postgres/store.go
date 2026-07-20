// Package postgres provides a PostgreSQL-backed storectrl.Store backed by
// postgres-controller-backend's sharding and lease mechanism.
//
// ShardedStore owns one bucket (shard) and supports all GVKs registered in
// the scheme. Each operation derives the GVK from the scheme so objects of
// any registered type can share the same bucket. List and Watch are scoped
// to the GVK of the provided list type.
//
// ResourceVersion semantics:
//   - Per-object: object_version from postgres — consistent between Get/Create/
//     Update and Watch events. Used for optimistic locking via ExpectedVersion.
//   - List metadata: bucket HWM (gvk_bucket_seq) for the watched GVK — passed
//     back as WatchFromRevision so the cache resumes without missing events.
//   - object_version is also stored in annotation "postgres.storectrl.io/object-version"
//     so Update/Delete can pass it as ExpectedVersion for conflict detection.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/pkg/crbridge"
	"github.com/patjlm/storectrl"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	annotationObjectVersion = "postgres.storectrl.io/object-version"
	leaseTTL                = 30 * time.Second
	leaseRenewInterval      = 10 * time.Second
)

type leaseEpochs struct {
	spec   int64
	status int64
}

// ShardedStore implements storectrl.Store against PostgreSQL, owning one bucket
// (shard). Multiple instances across processes can own different buckets of the
// same GVK for horizontal scaling.
//
// All GVKs registered in the scheme are supported. List and Watch are scoped to
// the GVK of the provided list type.
type ShardedStore struct {
	connStr  string
	bucketID int
	assign   crbridge.BucketAssigner
	holderID string
	scheme   *runtime.Scheme

	mu           sync.RWMutex
	epochs       leaseEpochs
	clusterEpoch int64
}

// Options configures a ShardedStore.
type Options struct {
	// ConnStr is the PostgreSQL DSN.
	ConnStr string
	// BucketID is the shard this instance owns.
	BucketID int
	// Assign maps (namespace, name) → bucket ID. Must be consistent across instances.
	Assign crbridge.BucketAssigner
	// HolderID uniquely identifies this controller instance (e.g. pod name).
	HolderID string
	// Scheme is used to resolve GVKs for objects and to instantiate typed results.
	Scheme *runtime.Scheme
}

// New creates a ShardedStore and acquires the spec+status lease for the bucket.
// Call Start to begin background lease renewal.
func New(ctx context.Context, opts Options) (*ShardedStore, error) {
	conn, err := pgx.Connect(ctx, opts.ConnStr)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	epochs, err := acquireBothLeases(ctx, conn, opts.BucketID, opts.HolderID, leaseTTL)
	conn.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire lease bucket %d: %w", opts.BucketID, err)
	}

	return &ShardedStore{
		connStr:  opts.ConnStr,
		bucketID: opts.BucketID,
		assign:   opts.Assign,
		holderID: opts.HolderID,
		scheme:   opts.Scheme,
		epochs:   epochs,
	}, nil
}

// Start begins background lease renewal. Runs until ctx is cancelled, at which
// point it releases the lease.
func (s *ShardedStore) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(leaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				conn, err := pgx.Connect(releaseCtx, s.connStr)
				if err == nil {
					_ = releaseBothLeases(releaseCtx, conn, s.bucketID, s.holderID)
					conn.Close(releaseCtx)
				}
				cancel()
				return
			case <-ticker.C:
				conn, err := pgx.Connect(ctx, s.connStr)
				if err != nil {
					continue
				}
				if err := renewBothLeases(ctx, conn, s.bucketID, s.holderID, leaseTTL); err != nil {
					if epochs, err2 := acquireBothLeases(ctx, conn, s.bucketID, s.holderID, leaseTTL); err2 == nil {
						s.mu.Lock()
						s.epochs = epochs
						s.mu.Unlock()
					}
				}
				conn.Close(ctx)
			}
		}
	}()
}

func (s *ShardedStore) currentEpochs() leaseEpochs {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochs
}

func (s *ShardedStore) clientFor(ctx context.Context, gvk schema.GroupVersionKind) *crbridge.Client {
	e := s.currentEpochs()
	return crbridge.NewClient(makeConnFactory(ctx, s.connStr), gvkToString(gvk), s.assign, s.holderID, e.spec)
}

func (s *ShardedStore) gvkForObject(obj client.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("GVK for %T: %w", obj, err)
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK registered for %T", obj)
	}
	return gvks[0], nil
}

// Get retrieves an object by key. The GVK is inferred from obj's registered type.
func (s *ShardedStore) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}
	crbObj, err := s.clientFor(ctx, gvk).Get(ctx, key.Namespace, key.Name)
	if err != nil {
		if errors.Is(err, crbridge.ErrNotFound) {
			return &storectrl.NotFoundError{Key: key.String()}
		}
		return fmt.Errorf("get %s: %w", key, err)
	}
	return crbToClient(crbObj, obj, gvk)
}

// List retrieves all objects of the list's element type in this shard.
// The list's ResourceVersion is set to the bucket HWM for use as WatchFromRevision.
func (s *ShardedStore) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := &client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(listOpts)
	}

	elemGVK, err := s.elemGVKForList(list)
	if err != nil {
		return err
	}

	lw := crbridge.NewListerWatcher(makeConnFactory(ctx, s.connStr), gvkToString(elemGVK), []int{s.bucketID})
	result, err := lw.List(ctx)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}

	epoch, hwm, parseErr := parseBucketHWM(result.ResourceVersion, s.bucketID)
	if parseErr == nil && epoch > 0 {
		s.mu.Lock()
		if epoch > s.clusterEpoch {
			s.clusterEpoch = epoch
		}
		s.mu.Unlock()
	}

	var items []client.Object
	for _, crbObj := range result.Objects {
		if listOpts.Namespace != "" && crbObj.Namespace != listOpts.Namespace {
			continue
		}
		if listOpts.LabelSelector != nil && !listOpts.LabelSelector.Empty() {
			if !listOpts.LabelSelector.Matches(labelSet(extractLabels(crbObj.Metadata))) {
				continue
			}
		}
		elem, err := s.scheme.New(elemGVK)
		if err != nil {
			return fmt.Errorf("scheme.New(%v): %w", elemGVK, err)
		}
		clientObj := elem.(client.Object)
		if err := crbToClient(crbObj, clientObj, elemGVK); err != nil {
			return err
		}
		items = append(items, clientObj)
	}

	if err := populateList(list, items); err != nil {
		return err
	}
	list.SetResourceVersion(strconv.FormatInt(hwm, 10))
	return nil
}

// Create stores a new object. The GVK is inferred from obj's registered type.
func (s *ShardedStore) Create(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}
	spec, status, metadata, err := clientToColumns(obj)
	if err != nil {
		return err
	}
	result, err := s.clientFor(ctx, gvk).Create(ctx, obj.GetNamespace(), obj.GetName(), spec, status, metadata)
	if err != nil {
		return mapCRBError(err, client.ObjectKeyFromObject(obj))
	}
	obj.SetUID(types.UID(result.UID.String()))
	obj.SetResourceVersion(result.ResourceVersion)
	setOVAnnotation(obj, result.ResourceVersion)
	return nil
}

// Update replaces an existing object. obj must carry the object_version annotation
// set by a prior Get, Create, or Update — used for optimistic locking.
func (s *ShardedStore) Update(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}
	spec, status, metadata, err := clientToColumns(obj)
	if err != nil {
		return err
	}
	ov := getOVAnnotation(obj)
	if ov == "" {
		ov = obj.GetResourceVersion()
	}
	if ov == "" {
		return &storectrl.NotFoundError{Key: client.ObjectKeyFromObject(obj).String()}
	}
	crbObj := &crbridge.Object{
		GVK:             gvkToString(gvk),
		Namespace:       obj.GetNamespace(),
		Name:            obj.GetName(),
		ResourceVersion: ov,
		Spec:            spec,
		Status:          status,
		Metadata:        metadata,
		BucketID:        s.bucketID,
	}
	result, err := s.clientFor(ctx, gvk).Update(ctx, crbObj)
	if err != nil {
		return s.mapConflictToNotFound(ctx, gvk, mapCRBError(err, client.ObjectKeyFromObject(obj)), obj)
	}
	obj.SetResourceVersion(result.ResourceVersion)
	setOVAnnotation(obj, result.ResourceVersion)
	return nil
}

func (s *ShardedStore) statusClientFor(ctx context.Context, gvk schema.GroupVersionKind) *crbridge.Client {
	e := s.currentEpochs()
	return crbridge.NewClient(makeConnFactory(ctx, s.connStr), gvkToString(gvk), s.assign, s.holderID, e.status)
}

// UpdateStatus writes only the status subresource. Spec and metadata in obj are
// ignored — the stored spec is preserved. Uses the status-domain lease epoch
// for fencing.
func (s *ShardedStore) UpdateStatus(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}
	_, status, _, err := clientToColumns(obj)
	if err != nil {
		return err
	}
	ov := getOVAnnotation(obj)
	if ov == "" {
		ov = obj.GetResourceVersion()
	}
	if ov == "" {
		return &storectrl.NotFoundError{Key: client.ObjectKeyFromObject(obj).String()}
	}
	crbObj := &crbridge.Object{
		GVK:             gvkToString(gvk),
		Namespace:       obj.GetNamespace(),
		Name:            obj.GetName(),
		ResourceVersion: ov,
		BucketID:        s.bucketID,
	}
	result, err := s.statusClientFor(ctx, gvk).Status().Update(crbObj, status)
	if err != nil {
		return s.mapConflictToNotFound(ctx, gvk, mapCRBError(err, client.ObjectKeyFromObject(obj)), obj)
	}
	obj.SetResourceVersion(result.ResourceVersion)
	setOVAnnotation(obj, result.ResourceVersion)
	return nil
}

// Delete removes an object. Works with or without ResourceVersion — empty RV
// triggers an unconditional delete (fetches current version first).
func (s *ShardedStore) Delete(ctx context.Context, obj client.Object) error {
	gvk, err := s.gvkForObject(obj)
	if err != nil {
		return err
	}

	ov := getOVAnnotation(obj)
	if ov == "" {
		ov = obj.GetResourceVersion()
	}

	// Unconditional delete: fetch current version first
	if ov == "" {
		key := client.ObjectKeyFromObject(obj)
		crbObj, err := s.clientFor(ctx, gvk).Get(ctx, key.Namespace, key.Name)
		if err != nil {
			if errors.Is(err, crbridge.ErrNotFound) {
				return &storectrl.NotFoundError{Key: key.String()}
			}
			return fmt.Errorf("get for unconditional delete %s: %w", key, err)
		}
		ov = crbObj.ResourceVersion
	}

	spec, status, metadata, err := clientToColumns(obj)
	if err != nil {
		return err
	}
	crbObj := &crbridge.Object{
		GVK:             gvkToString(gvk),
		Namespace:       obj.GetNamespace(),
		Name:            obj.GetName(),
		ResourceVersion: ov,
		Spec:            spec,
		Status:          status,
		Metadata:        metadata,
		BucketID:        s.bucketID,
	}
	err = s.clientFor(ctx, gvk).Delete(ctx, crbObj)
	return s.mapConflictToNotFound(ctx, gvk, mapCRBError(err, client.ObjectKeyFromObject(obj)), obj)
}

// Watch streams change events for the list's element type starting from the
// given WatchFromRevision (bucket HWM returned by a prior List).
// Returns RevisionTooOldError immediately if hwm is below the compaction horizon.
func (s *ShardedStore) Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (storectrl.Watcher, error) {
	listOpts := &client.ListOptions{}
	var hwm int64
	for _, o := range opts {
		o.ApplyToList(listOpts)
		if rv, ok := o.(storectrl.WatchFromRevision); ok {
			hwm = int64(rv)
		}
	}

	elemGVK, err := s.elemGVKForList(list)
	if err != nil {
		return nil, err
	}

	if err := s.checkCompactionHorizon(ctx, elemGVK, hwm); err != nil {
		return nil, err
	}

	s.mu.RLock()
	clusterEpoch := s.clusterEpoch
	s.mu.RUnlock()
	rvStr := fmt.Sprintf("e%d|b%d:%d", clusterEpoch, s.bucketID, hwm)

	lw := crbridge.NewListerWatcher(makeConnFactory(ctx, s.connStr), gvkToString(elemGVK), []int{s.bucketID})
	wi, err := lw.Watch(ctx, rvStr)
	if err != nil {
		return nil, fmt.Errorf("watch: %w", err)
	}

	gvk := elemGVK // capture for closure
	convertFn := func(crbObj *crbridge.Object) (client.Object, error) {
		elem, err := s.scheme.New(gvk)
		if err != nil {
			return nil, err
		}
		clientObj := elem.(client.Object)
		return clientObj, crbToClient(crbObj, clientObj, gvk)
	}
	return newWatchBridge(wi, convertFn, listOpts.LabelSelector), nil
}

// --- GVK helpers ---

func (s *ShardedStore) elemGVKForList(list client.ObjectList) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(list)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("GVK for list %T: %w", list, err)
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK registered for list type %T", list)
	}
	g := gvks[0]
	kind := g.Kind
	if len(kind) > 4 && kind[len(kind)-4:] == "List" {
		kind = kind[:len(kind)-4]
	}
	return schema.GroupVersionKind{
		Group:   g.Group,
		Version: g.Version,
		Kind:    kind,
	}, nil
}

// checkCompactionHorizon returns RevisionTooOldError if hwm is at or below the
// compaction horizon for this bucket+GVK, preventing infinite Watch retry loops.
func (s *ShardedStore) checkCompactionHorizon(ctx context.Context, gvk schema.GroupVersionKind, hwm int64) error {
	if hwm == 0 {
		return nil
	}
	conn, err := pgx.Connect(ctx, s.connStr)
	if err != nil {
		return nil // conservative: let watch fail naturally on connection error
	}
	defer conn.Close(ctx)

	var horizon int64
	err = conn.QueryRow(ctx,
		`SELECT compacted_seq FROM compaction_horizon WHERE bucket_id = $1 AND gvk = $2`,
		s.bucketID, gvkToString(gvk),
	).Scan(&horizon)
	if err != nil {
		return nil // no compaction record → no restriction
	}
	if hwm <= horizon {
		return &storectrl.RevisionTooOldError{
			RequestedRevision: hwm,
			OldestRevision:    horizon + 1,
		}
	}
	return nil
}

// --- object conversion ---

// crbToClient converts a crbridge.Object to a typed client.Object.
// Metadata JSON is unmarshalled first; then name/namespace/uid/rv are overridden
// from the crbridge fields. object_version is stored in a hidden annotation so
// subsequent Update/Delete calls can pass it as ExpectedVersion.
func crbToClient(src *crbridge.Object, dst client.Object, gvk schema.GroupVersionKind) error {
	type shell struct {
		APIVersion string          `json:"apiVersion"`
		Kind       string          `json:"kind"`
		Metadata   json.RawMessage `json:"metadata,omitempty"`
		Spec       json.RawMessage `json:"spec,omitempty"`
		Status     json.RawMessage `json:"status,omitempty"`
	}
	data, err := json.Marshal(shell{
		APIVersion: apiVersionStr(gvk),
		Kind:       gvk.Kind,
		Metadata:   src.Metadata,
		Spec:       src.Spec,
		Status:     src.Status,
	})
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return err
	}
	dst.SetName(src.Name)
	dst.SetNamespace(src.Namespace)
	dst.SetUID(types.UID(src.UID.String()))
	dst.SetResourceVersion(src.ResourceVersion)
	setOVAnnotation(dst, src.ResourceVersion)
	return nil
}

// clientToColumns extracts spec, status, and metadata JSON for storage.
// Strips the hidden object-version annotation from metadata, along with uid
// and resourceVersion which are stored in dedicated DB columns.
func clientToColumns(obj client.Object) (spec, status, metadata json.RawMessage, err error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, nil, nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, nil, nil, err
	}
	if raw, ok := m["metadata"]; ok {
		var metaMap map[string]json.RawMessage
		if json.Unmarshal(raw, &metaMap) == nil {
			// Strip server-managed fields stored in dedicated DB columns.
			delete(metaMap, "uid")
			delete(metaMap, "resourceVersion")
			if annRaw, ok := metaMap["annotations"]; ok {
				var ann map[string]string
				if json.Unmarshal(annRaw, &ann) == nil {
					delete(ann, annotationObjectVersion)
					if len(ann) == 0 {
						delete(metaMap, "annotations")
					} else {
						metaMap["annotations"], _ = json.Marshal(ann)
					}
				}
			}
			m["metadata"], _ = json.Marshal(metaMap)
		}
	}
	spec = m["spec"]
	if spec == nil {
		spec = json.RawMessage("{}")
	}
	status = m["status"]
	if status == nil {
		status = json.RawMessage("{}")
	}
	return spec, status, m["metadata"], nil
}

// populateList fills the Items field of a concrete list type via reflection,
// matching the pattern used by the memory backend.
func populateList(list client.ObjectList, items []client.Object) error {
	listVal := reflect.ValueOf(list)
	if listVal.Kind() == reflect.Ptr {
		listVal = listVal.Elem()
	}
	itemsField := listVal.FieldByName("Items")
	if !itemsField.IsValid() || !itemsField.CanSet() {
		return fmt.Errorf("list type %T has no settable Items field", list)
	}
	slice := reflect.MakeSlice(itemsField.Type(), 0, len(items))
	for _, item := range items {
		v := reflect.ValueOf(item)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		slice = reflect.Append(slice, v)
	}
	itemsField.Set(slice)
	return nil
}

// --- lease management (raw SQL; avoids internal/lease import restriction) ---

func acquireBothLeases(ctx context.Context, conn *pgx.Conn, bucketID int, holderID string, ttl time.Duration) (leaseEpochs, error) {
	ttlStr := fmt.Sprintf("%d seconds", int(ttl.Seconds()))
	rows, err := conn.Query(ctx, `
		INSERT INTO bucket_leases (bucket_id, domain, holder, epoch, expires_at)
		VALUES ($1, 'spec',   $2, 1, now() + $3::interval),
		       ($1, 'status', $2, 1, now() + $3::interval)
		ON CONFLICT (bucket_id, domain) DO UPDATE
		SET holder = EXCLUDED.holder,
		    epoch = bucket_leases.epoch + 1,
		    expires_at = EXCLUDED.expires_at
		WHERE bucket_leases.expires_at < now()
		   OR bucket_leases.holder = EXCLUDED.holder
		RETURNING domain, epoch`,
		bucketID, holderID, ttlStr)
	if err != nil {
		return leaseEpochs{}, err
	}
	defer rows.Close()

	var e leaseEpochs
	count := 0
	for rows.Next() {
		var domain string
		var epoch int64
		if err := rows.Scan(&domain, &epoch); err != nil {
			return leaseEpochs{}, err
		}
		switch domain {
		case "spec":
			e.spec = epoch
		case "status":
			e.status = epoch
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return leaseEpochs{}, err
	}
	if count != 2 {
		_, _ = conn.Exec(ctx, `
			DELETE FROM bucket_leases
			WHERE bucket_id = $1 AND holder = $2`,
			bucketID, holderID)
		return leaseEpochs{}, fmt.Errorf("bucket %d held by another instance (%d/2 domains acquired)", bucketID, count)
	}
	return e, nil
}

func renewBothLeases(ctx context.Context, conn *pgx.Conn, bucketID int, holderID string, ttl time.Duration) error {
	ttlStr := fmt.Sprintf("%d seconds", int(ttl.Seconds()))
	tag, err := conn.Exec(ctx, `
		UPDATE bucket_leases
		SET expires_at = now() + $1::interval
		WHERE bucket_id = $2 AND domain IN ('spec', 'status') AND holder = $3`,
		ttlStr, bucketID, holderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 2 {
		return fmt.Errorf("lease renewal: not holder of bucket %d", bucketID)
	}
	return nil
}

func releaseBothLeases(ctx context.Context, conn *pgx.Conn, bucketID int, holderID string) error {
	_, err := conn.Exec(ctx, `
		DELETE FROM bucket_leases
		WHERE bucket_id = $1 AND domain IN ('spec', 'status') AND holder = $2`,
		bucketID, holderID)
	return err
}

// --- watch bridge ---

type watchBridge struct {
	inner         crbridge.WatchInterface
	out           chan storectrl.Event
	convertFn     func(*crbridge.Object) (client.Object, error)
	labelSelector labels.Selector
	stop          chan struct{}
	stopOnce      sync.Once
}

func newWatchBridge(wi crbridge.WatchInterface, convertFn func(*crbridge.Object) (client.Object, error), sel labels.Selector) *watchBridge {
	b := &watchBridge{
		inner:         wi,
		out:           make(chan storectrl.Event, 256),
		convertFn:     convertFn,
		labelSelector: sel,
		stop:          make(chan struct{}),
	}
	go b.translate()
	return b
}

func (b *watchBridge) ResultChan() <-chan storectrl.Event { return b.out }

func (b *watchBridge) Stop() {
	b.stopOnce.Do(func() { close(b.stop) })
	b.inner.Stop()
}

func (b *watchBridge) translate() {
	defer close(b.out)
	for ev := range b.inner.ResultChan() {
		var evType storectrl.EventType
		switch ev.Type {
		case crbridge.EventAdded:
			evType = storectrl.EventAdded
		case crbridge.EventModified:
			evType = storectrl.EventModified
		case crbridge.EventDeleted:
			evType = storectrl.EventDeleted
		default:
			continue
		}
		obj, err := b.convertFn(ev.Object)
		if err != nil {
			b.stopOnce.Do(func() { close(b.stop) })
			b.inner.Stop()
			return
		}
		if b.labelSelector != nil && !b.labelSelector.Empty() {
			if !b.labelSelector.Matches(labelSet(obj.GetLabels())) {
				continue
			}
		}
		select {
		case b.out <- storectrl.Event{Type: evType, Object: obj}:
		case <-b.stop:
			return
		}
	}
}

// --- utility ---

func makeConnFactory(ctx context.Context, connStr string) func() (*pgx.Conn, error) {
	return func() (*pgx.Conn, error) {
		return pgx.Connect(ctx, connStr)
	}
}

func gvkToString(gvk schema.GroupVersionKind) string {
	return fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
}

func apiVersionStr(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return gvk.Version
	}
	return gvk.Group + "/" + gvk.Version
}

// parseBucketHWM extracts epoch and bucket HWM from composite RV "e7|b2:1044,b5:902".
func parseBucketHWM(rvStr string, bucketID int) (epoch, hwm int64, err error) {
	parts := strings.SplitN(rvStr, "|", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid composite RV: %q", rvStr)
	}
	epoch, err = strconv.ParseInt(strings.TrimPrefix(parts[0], "e"), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse epoch from RV %q: %w", rvStr, err)
	}
	prefix := fmt.Sprintf("b%d:", bucketID)
	for _, part := range strings.Split(parts[1], ",") {
		if strings.HasPrefix(part, prefix) {
			hwm, err = strconv.ParseInt(strings.TrimPrefix(part, prefix), 10, 64)
			return epoch, hwm, err
		}
	}
	return epoch, 0, nil
}

func setOVAnnotation(obj client.Object, objectVersion string) {
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[annotationObjectVersion] = objectVersion
	obj.SetAnnotations(ann)
}

func getOVAnnotation(obj client.Object) string {
	return obj.GetAnnotations()[annotationObjectVersion]
}

func extractLabels(metadata json.RawMessage) map[string]string {
	if metadata == nil {
		return nil
	}
	var m struct {
		Labels map[string]string `json:"labels"`
	}
	json.Unmarshal(metadata, &m)
	return m.Labels
}

// labelSet implements labels.Labels for selector matching.
type labelSet map[string]string

func (l labelSet) Has(label string) bool              { _, ok := l[label]; return ok }
func (l labelSet) Get(label string) string            { return l[label] }
func (l labelSet) Lookup(label string) (string, bool) { v, ok := l[label]; return v, ok }

func mapCRBError(err error, key client.ObjectKey) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, crbridge.ErrNotFound):
		return &storectrl.NotFoundError{Key: key.String()}
	case errors.Is(err, crbridge.ErrAlreadyExists):
		return &storectrl.AlreadyExistsError{Key: key.String()}
	case errors.Is(err, crbridge.ErrConflict):
		return &storectrl.ConflictError{Key: key.String()}
	case errors.Is(err, crbridge.ErrGone):
		return &storectrl.RevisionTooOldError{}
	case errors.Is(err, crbridge.ErrFenced):
		return &storectrl.FencedError{}
	}
	return err
}

// mapConflictToNotFound disambiguates a ConflictError from Update/Delete: the
// stored proc returns P0002 for both "version mismatch" and "object not found"
// (both cause the WHERE clause to match zero rows). A Get after the conflict
// determines which case applies.
func (s *ShardedStore) mapConflictToNotFound(ctx context.Context, gvk schema.GroupVersionKind, err error, obj client.Object) error {
	if err == nil {
		return nil
	}
	var conflict *storectrl.ConflictError
	if !errors.As(err, &conflict) {
		return err
	}
	key := client.ObjectKeyFromObject(obj)
	_, getErr := s.clientFor(ctx, gvk).Get(ctx, key.Namespace, key.Name)
	if errors.Is(getErr, crbridge.ErrNotFound) {
		return &storectrl.NotFoundError{Key: key.String()}
	}
	if getErr != nil {
		return getErr
	}
	return err
}
