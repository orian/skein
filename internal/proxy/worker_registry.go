package proxy

import (
	"context"
	"errors"
	"log/slog"
	"skein/internal/api"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	workerSendTimeout  = 500 * time.Millisecond
	staleWorkerTimeout = 60 * time.Second
	cleanupInterval    = 5 * time.Minute
)

// ErrNoWorkersAvailable is returned when a job cannot be dispatched because no workers are ready.
var ErrNoWorkersAvailable = errors.New("no workers available to dispatch job")

// WorkerHandler represents the proxy's state for a single worker.
type WorkerHandler struct {
	ID            string
	JobChannel    chan *api.Job
	mu            sync.RWMutex
	ready         bool
	lastHeartbeat time.Time
}

// NewWorkerHandler creates a new handler for a worker.
func NewWorkerHandler() *WorkerHandler {
	return &WorkerHandler{
		ID: uuid.NewString(),
		// A buffered channel of 1 allows the registry to send a job without blocking
		// while the worker's long poll request might be in flight.
		JobChannel:    make(chan *api.Job, 0),
		ready:         false,
		lastHeartbeat: time.Now().UTC(),
	}
}

// IsReady checks if the worker is marked as ready.
func (wh *WorkerHandler) IsReady() bool {
	wh.mu.RLock()
	defer wh.mu.RUnlock()
	return wh.ready
}

// SetReady marks the worker's ready status.
func (wh *WorkerHandler) SetReady(ready bool) {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	wh.ready = ready
}

// UpdateHeartbeat sets the last heartbeat time to now.
func (wh *WorkerHandler) UpdateHeartbeat() {
	wh.mu.Lock()
	defer wh.mu.Unlock()
	wh.lastHeartbeat = time.Now().UTC()
}

// IsStale checks if the worker has not sent a heartbeat in a while.
func (wh *WorkerHandler) IsStale() bool {
	wh.mu.RLock()
	defer wh.mu.RUnlock()
	return time.Since(wh.lastHeartbeat) > staleWorkerTimeout
}

// WorkerRegistry manages the pool of active workers.
type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*WorkerHandler
}

// NewWorkerRegistry creates a new worker registry and starts its cleanup process.
func NewWorkerRegistry() *WorkerRegistry {
	r := &WorkerRegistry{
		workers: make(map[string]*WorkerHandler),
	}
	go r.cleanupLoop()
	return r
}

// Register creates a new WorkerHandler, adds it to the pool, and returns it.
func (r *WorkerRegistry) Register() *WorkerHandler {
	handler := NewWorkerHandler()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[handler.ID] = handler
	slog.Info("worker registered", "worker_id", handler.ID)
	return handler
}

// Deregister removes a worker from the pool.
func (r *WorkerRegistry) Deregister(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.workers, workerID)
	slog.Info("worker deregistered", "worker_id", workerID)
}

// Heartbeat updates the heartbeat timestamp for a given worker.
func (r *WorkerRegistry) Heartbeat(workerID string) bool {
	r.mu.RLock()
	handler, ok := r.workers[workerID]
	r.mu.RUnlock()
	if ok {
		handler.UpdateHeartbeat()
		return true
	}
	return false
}

// Get returns a worker handler by its ID.
func (r *WorkerRegistry) Get(workerID string) (*WorkerHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.workers[workerID]
	return handler, ok
}

// Dispatch finds a ready worker and attempts to send it a job.
func (r *WorkerRegistry) Dispatch(ctx context.Context, job *api.Job) error {
	r.mu.RLock()
	for _, handler := range r.workers {
		r.mu.RUnlock()

		if !handler.IsReady() {
			r.mu.RLock()
			continue
		}

		// Try to send the job with a timeout.
		select {
		case handler.JobChannel <- job:
			slog.Info("job dispatched to worker", "job_id", job.ID, "worker_id", handler.ID)
			return nil // Job dispatched successfully
		case <-time.After(workerSendTimeout):
			// Worker was not ready to receive, try the next one.
			slog.Warn("timed out sending job to worker, trying next", "worker_id", handler.ID)
			r.mu.RLock()
			continue
		case <-ctx.Done():
			// The original request context was cancelled.
			return ctx.Err()
		}
	}
	r.mu.RUnlock()

	return ErrNoWorkersAvailable
}

// cleanupLoop periodically removes stale workers from the registry.
func (r *WorkerRegistry) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		slog.Info("running worker cleanup")
		for id, handler := range r.workers {
			if handler.IsStale() {
				delete(r.workers, id)
				slog.Info("removed stale worker", "worker_id", id)
			}
		}
		r.mu.Unlock()
	}
}
