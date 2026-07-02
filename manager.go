package ctrlforge

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"
)

// ManagerOptions configure the ctrlforge manager.
type ManagerOptions struct {
	// Scheme is required - defines the API types known to the manager.
	Scheme *runtime.Scheme

	// Logger is optional - defaults to a no-op logger if not set.
	Logger logr.Logger

	// HealthProbeBindAddress is the TCP address for health checks.
	// Set to "" to disable the health probe server.
	// Example: ":8081"
	HealthProbeBindAddress string
}

// NewManager creates a manager.Manager backed by a ctrlforge Store.
// The manager provides the same interface as controller-runtime's manager
// so existing controller patterns work with non-Kubernetes backends.
func NewManager(store Store, opts ManagerOptions) (manager.Manager, error) {
	if opts.Scheme == nil {
		return nil, fmt.Errorf("scheme is required")
	}

	// Default logger to no-op
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	// Create client and cache backed by the store
	cl := NewClient(store, opts.Scheme)
	ca := NewCache(store, opts.Scheme)

	mgr := &storeManager{
		store:                  store,
		scheme:                 opts.Scheme,
		client:                 cl,
		cache:                  ca,
		logger:                 opts.Logger,
		healthProbeBindAddress: opts.HealthProbeBindAddress,
		runnables:              make([]manager.Runnable, 0),
		elected:                make(chan struct{}),
		healthChecks:           make(map[string]healthz.Checker),
		readyChecks:            make(map[string]healthz.Checker),
		restMapper:             meta.NewDefaultRESTMapper(nil),
	}

	// Always elected (no leader election for non-kube backends)
	close(mgr.elected)

	return mgr, nil
}

type storeManager struct {
	store  Store
	scheme *runtime.Scheme
	client client.Client
	cache  cache.Cache
	logger logr.Logger

	healthProbeBindAddress string
	healthServer           *http.Server

	runnables    []manager.Runnable
	runnablesMu  sync.Mutex
	elected      chan struct{}
	healthChecks map[string]healthz.Checker
	readyChecks  map[string]healthz.Checker
	restMapper   meta.RESTMapper

	// Track running state
	startOnce sync.Once
	started   chan struct{}
}

// Cluster interface methods

func (m *storeManager) GetHTTPClient() *http.Client {
	return nil // Not applicable for non-kube backends
}

func (m *storeManager) GetConfig() *rest.Config {
	return nil // Not applicable for non-kube backends
}

func (m *storeManager) GetCache() cache.Cache {
	return m.cache
}

func (m *storeManager) GetScheme() *runtime.Scheme {
	return m.scheme
}

func (m *storeManager) GetClient() client.Client {
	return m.client
}

func (m *storeManager) GetFieldIndexer() client.FieldIndexer {
	return m.cache
}

func (m *storeManager) GetEventRecorderFor(name string) record.EventRecorder {
	return &noopEventRecorder{name: name, logger: m.logger}
}

func (m *storeManager) GetEventRecorder(name string) events.EventRecorder {
	return &noopEventsRecorder{name: name, logger: m.logger}
}

func (m *storeManager) GetRESTMapper() meta.RESTMapper {
	return m.restMapper
}

func (m *storeManager) GetAPIReader() client.Reader {
	// For non-kube backends, no distinction between cached and direct reads
	return m.client
}

func (m *storeManager) GetConverterRegistry() conversion.Registry {
	// Not applicable for non-kube backends
	return nil
}

// Manager-specific methods

func (m *storeManager) Add(r manager.Runnable) error {
	m.runnablesMu.Lock()
	defer m.runnablesMu.Unlock()
	m.runnables = append(m.runnables, r)
	return nil
}

func (m *storeManager) Elected() <-chan struct{} {
	return m.elected
}

func (m *storeManager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	// Not supported - metrics could be added later if needed
	return nil
}

func (m *storeManager) AddHealthzCheck(name string, check healthz.Checker) error {
	m.runnablesMu.Lock()
	defer m.runnablesMu.Unlock()
	m.healthChecks[name] = check
	return nil
}

func (m *storeManager) AddReadyzCheck(name string, check healthz.Checker) error {
	m.runnablesMu.Lock()
	defer m.runnablesMu.Unlock()
	m.readyChecks[name] = check
	return nil
}

func (m *storeManager) GetWebhookServer() webhook.Server {
	panic("webhooks not supported in ctrlforge - use manager backed by real Kubernetes API server")
}

func (m *storeManager) GetLogger() logr.Logger {
	return m.logger
}

func (m *storeManager) GetControllerOptions() config.Controller {
	return config.Controller{}
}

func (m *storeManager) Start(ctx context.Context) error {
	var startErr error
	m.startOnce.Do(func() {
		m.started = make(chan struct{})
		startErr = m.start(ctx)
	})
	return startErr
}

func (m *storeManager) start(ctx context.Context) error {
	m.logger.Info("starting ctrlforge manager")

	// Start health probe server if configured
	if m.healthProbeBindAddress != "" {
		if err := m.startHealthProbeServer(); err != nil {
			return fmt.Errorf("failed to start health probe server: %w", err)
		}
	}

	// Start the cache first and wait for sync
	cacheCtx, cacheCancel := context.WithCancel(ctx)
	defer cacheCancel()

	go func() {
		if err := m.cache.Start(cacheCtx); err != nil {
			m.logger.Error(err, "cache failed to start")
		}
	}()

	// Wait for cache to sync with timeout
	syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
	defer syncCancel()

	if !m.cache.WaitForCacheSync(syncCtx) {
		return fmt.Errorf("cache sync timeout")
	}

	m.logger.Info("cache synced")

	// Start all runnables
	m.runnablesMu.Lock()
	runnables := make([]manager.Runnable, len(m.runnables))
	copy(runnables, m.runnables)
	m.runnablesMu.Unlock()

	var wg sync.WaitGroup
	for i := range runnables {
		r := runnables[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Start(ctx); err != nil {
				m.logger.Error(err, "runnable failed")
			}
		}()
	}

	m.logger.Info("all runnables started")
	close(m.started)

	// Block until context is cancelled
	<-ctx.Done()
	m.logger.Info("manager shutting down")

	// Shutdown health server gracefully
	if m.healthServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.healthServer.Shutdown(shutdownCtx); err != nil {
			m.logger.Error(err, "health server shutdown failed")
		}
	}

	// Wait for runnables to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.logger.Info("all runnables stopped")
	case <-time.After(30 * time.Second):
		m.logger.Info("runnable shutdown timeout")
	}

	return nil
}

func (m *storeManager) startHealthProbeServer() error {
	mux := http.NewServeMux()

	// /healthz endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		for name, check := range m.healthChecks {
			if err := check(r); err != nil {
				m.logger.Error(err, "health check failed", "check", name)
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "health check %s failed: %v\n", name, err)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok\n")
	})

	// /readyz endpoint
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		for name, check := range m.readyChecks {
			if err := check(r); err != nil {
				m.logger.Error(err, "ready check failed", "check", name)
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "ready check %s failed: %v\n", name, err)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok\n")
	})

	m.healthServer = &http.Server{
		Addr:              m.healthProbeBindAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		m.logger.Info("starting health probe server", "address", m.healthProbeBindAddress)
		if err := m.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error(err, "health probe server failed")
		}
	}()

	return nil
}

// noopEventRecorder implements record.EventRecorder with no-op methods
type noopEventRecorder struct {
	name   string
	logger logr.Logger
}

func (r *noopEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	r.logger.V(1).Info("event",
		"recorder", r.name,
		"type", eventtype,
		"reason", reason,
		"message", message,
	)
}

func (r *noopEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	message := fmt.Sprintf(messageFmt, args...)
	r.Event(object, eventtype, reason, message)
}

func (r *noopEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	message := fmt.Sprintf(messageFmt, args...)
	r.logger.V(1).Info("annotated event",
		"recorder", r.name,
		"type", eventtype,
		"reason", reason,
		"message", message,
		"annotations", annotations,
	)
}

type noopEventsRecorder struct {
	name   string
	logger logr.Logger
}

func (r *noopEventsRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	r.logger.V(1).Info("event",
		"recorder", r.name,
		"type", eventtype,
		"reason", reason,
		"action", action,
		"note", fmt.Sprintf(note, args...),
	)
}
