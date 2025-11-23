package proxy

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"skein/internal/api"
	"skein/internal/settings"
	"time"

	"github.com/google/uuid"
)

const (
	// Default timeout for a synchronous query.
	requestTimeout = 30 * time.Second
)

// Proxy holds the dependencies for the proxy server.
type Proxy struct {
	registry    *WorkerRegistry
	jobQueue    *JobQueue
	resultStore *ResultStore
}

// NewProxy creates a new Proxy instance.
func NewProxy(registry *WorkerRegistry, jobQueue *JobQueue, resultStore *ResultStore) *Proxy {
	return &Proxy{
		registry:    registry,
		jobQueue:    jobQueue,
		resultStore: resultStore,
	}
}

// RegisterWorkerHandler handles the registration of a new worker.
func (p *Proxy) RegisterWorkerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	handler := p.registry.Register()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"worker_id": handler.ID})
}

// DeregisterWorkerHandler handles the graceful shutdown of a worker.
func (p *Proxy) DeregisterWorkerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	workerID := r.URL.Query().Get("worker_id")
	if workerID == "" {
		http.Error(w, "worker_id query parameter is required", http.StatusBadRequest)
		return
	}
	p.registry.Deregister(workerID)
	w.WriteHeader(http.StatusOK)
}

// HeartbeatHandler handles heartbeat pings from workers.
func (p *Proxy) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if payload.WorkerID == "" {
		http.Error(w, "worker_id is required", http.StatusBadRequest)
		return
	}
	if !p.registry.Heartbeat(payload.WorkerID) {
		http.Error(w, "Worker not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// JobDispatcherHandler provides the next available job to a requesting worker.
func (p *Proxy) JobDispatcherHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	workerID := r.URL.Query().Get("worker_id")
	if workerID == "" {
		http.Error(w, "worker_id query parameter is required", http.StatusBadRequest)
		return
	}

	handler, ok := p.registry.Get(workerID)
	if !ok {
		http.Error(w, "Worker not registered or has been deregistered", http.StatusForbidden)
		return
	}
	handler.UpdateHeartbeat()

	var (
		job     *api.Job
		timeout bool
	)

	job = p.jobQueue.Get()
	if job == nil {
		// Mark worker as ready and defer setting it to not ready.
		handler.SetReady(true)
		defer handler.SetReady(false)
		slog.Debug("worker is ready and waiting for a job", "worker_id", workerID)

		select {
		case job = <-handler.JobChannel:
			slog.Info("dispatching job to worker", "event", "query.assigned", "job_id", job.ID, "worker_id", workerID)
		case <-time.After(settings.LongPollTimeout):
			slog.Debug("long poll timeout", "worker_id", workerID, "error", r.Context().Err())
			timeout = true
		case <-r.Context().Done():
			slog.Debug("worker request context done", "worker_id", workerID, "error", r.Context().Err())
			timeout = true
		}
		if timeout {
			select {
			case job, ok = <-handler.JobChannel:
				slog.Info("last minute catch, worker channel not empty, dispatching job anyway", "worker_id", workerID, "job_id", job.ID)
			default:
			}
		}
	}
	if job != nil {
		job.Status = api.StatusRunning
		job.DispatchedAt = time.Now().UTC()
		job.UpdatedAt = time.Now().UTC()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(job); err != nil {
			slog.Error("failed to encode job for worker", "job_id", job.ID, "error", err)
			// If we fail to send, try to re-dispatch the job.
			// This is best-effort and might fail if no other workers are available.
			// TODO do this in some smart way
			p.registry.Dispatch(context.Background(), job)
		}
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// QueryHandler handles the synchronous submission of new queries.
func (p *Proxy) QueryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var req api.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("failed to decode request body", "error", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	job := &api.Job{
		ID:               uuid.NewString(),
		UserID:           req.UserID,
		Query:            req.Query,
		Params:           req.Params,
		Priority:         req.Priority,
		Status:           api.StatusPending,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
		DisableProfiling: req.DisableProfiling,
	}

	slog.Info("query received", "event", "query.received", "job_id", job.ID, "user_id", job.UserID)
	resultChan := p.resultStore.Register(job.ID)
	defer p.resultStore.Deregister(job.ID)

	// Create a context for dispatching.
	dispatchCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second) // Short timeout for dispatch attempt
	defer cancel()

	if err := p.registry.Dispatch(dispatchCtx, job); err != nil {
		// If dispatch fails (e.g., no workers), add to the fallback queue.
		slog.Warn("direct dispatch failed, adding to fallback queue", "job_id", job.ID, "error", err)
		p.jobQueue.Add(job)
	}

	// Wait for the result or a timeout.
	select {
	case result := <-resultChan:
		w.Header().Set("Content-Type", "application/json")
		if result.Error != "" {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(api.QueryResults{Error: result.Error})
			return
		}

		var duckdbProfile api.DuckDBProfile
		if len(result.Profile) > 0 {
			if err := json.Unmarshal(result.Profile, &duckdbProfile); err != nil {
				slog.Error("failed to unmarshal DuckDB profile", "job_id", job.ID, "error", err)
				http.Error(w, "Internal server error: failed to process profiling data", http.StatusInternalServerError)
				return
			}
		}

		queryResults := api.QueryResults{
			ColumnNames: result.ColumnNames,
			ColumnTypes: result.ColumnTypes,
			ColumnData:  result.ColumnData,
			Profile: api.ProfilingStats{
				TotalBytesWritten: duckdbProfile.TotalBytesWritten,
				TotalBytesRead:    duckdbProfile.TotalBytesRead,
				RowsReturned:      duckdbProfile.RowsReturned,
				Latency:           duckdbProfile.Latency,
				CPUTime:           duckdbProfile.CPUTime,
			},
			GoProfile: api.GoProfileStats{
				ExecuteTime:       result.GoProfile.ExecuteTime,
				QueryTime:         result.GoProfile.QueryTime,
				DispatchLatencyMs: job.DispatchedAt.Sub(job.CreatedAt).Milliseconds(),
			},
		}

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(queryResults); err != nil {
			slog.Error("failed to encode query results", "job_id", job.ID, "error", err)
		}
	case <-r.Context().Done():
		slog.Warn("client cancelled request", "job_id", job.ID)
		http.Error(w, "Request cancelled", 499) // 499 Client Closed Request
	case <-time.After(requestTimeout):
		slog.Error("request timed out waiting for result", "job_id", job.ID)
		http.Error(w, "Request timed out", http.StatusGatewayTimeout)
	}
}

// ResultHandler is an internal endpoint for workers to post query results.
func (p *Proxy) ResultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	type resultPayload struct {
		JobID  string         `json:"job_id"`
		Result *api.JobResult `json:"result"`
	}

	var payload resultPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid result payload", http.StatusBadRequest)
		return
	}

	if !p.resultStore.Notify(payload.JobID, payload.Result) {
		slog.Warn("result received for timed-out or unknown job", "job_id", payload.JobID)
	}

	w.WriteHeader(http.StatusOK)
}

// HealthCheckHandler provides a simple endpoint for health checks.
func (p *Proxy) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
