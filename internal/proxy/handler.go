package proxy

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"skein/internal/api"
	"time"

	"github.com/google/uuid"
)

const (
	// Default timeout for a synchronous query.
	requestTimeout = 30 * time.Second
)

// Proxy holds the dependencies for the proxy server.
type Proxy struct {
	jobQueue    *JobQueue
	resultStore *ResultStore
}

// NewProxy creates a new Proxy instance.
func NewProxy(queue *JobQueue, store *ResultStore) *Proxy {
	return &Proxy{
		jobQueue:    queue,
		resultStore: store,
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

	if req.UserID == "" || req.Query == "" {
		slog.Warn("validation failed: UserID and Query are required", "user_id", req.UserID)
		http.Error(w, "UserID and Query fields are required", http.StatusBadRequest)
		return
	}

	job := &api.Job{
		ID:        uuid.NewString(),
		UserID:    req.UserID,
		Query:     req.Query,
		Params:    req.Params,
		Priority:  req.Priority,
		Status:    api.StatusPending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	slog.Info("query received", "event", "query.received", "job_id", job.ID, "user_id", job.UserID)

	// Register the job to get a result channel and defer deregistration.
	resultChan := p.resultStore.Register(job.ID)
	defer p.resultStore.Deregister(job.ID)

	// Add the job to the queue for a worker to pick up.
	p.jobQueue.Add(job)

	// Wait for the result or a timeout.
	select {
	case result := <-resultChan:
		w.Header().Set("Content-Type", "application/json")
		if result.Error != "" {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(result)

	case <-r.Context().Done():
		// Client cancelled the request. Attempt to remove the job from the queue.
		if p.jobQueue.Remove(job.ID) {
			slog.Warn("request cancelled by client, job removed from queue", "job_id", job.ID)
		} else {
			slog.Warn("request cancelled by client, but job was already processed or is running", "job_id", job.ID)
		}
		http.Error(w, "Request cancelled", 499) // 499 Client Closed Request

	case <-time.After(requestTimeout):
		// The server-side timeout was hit. Attempt to remove the job.
		if p.jobQueue.Remove(job.ID) {
			slog.Error("request timed out, job removed from queue", "job_id", job.ID)
		} else {
			slog.Error("request timed out, but job was already processed or is running", "job_id", job.ID)
		}
		http.Error(w, "Request timed out", http.StatusGatewayTimeout)
	}
}

// ResultHandler is an internal endpoint for workers to post query results.
func (p *Proxy) ResultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// The worker will post a result including the Job ID
	type resultPayload struct {
		JobID  string         `json:"job_id"`
		Result *api.JobResult `json:"result"`
	}

	var payload resultPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		slog.Error("failed to decode result payload", "error", err)
		http.Error(w, "Invalid result payload", http.StatusBadRequest)
		return
	}

	if payload.JobID == "" || payload.Result == nil {
		http.Error(w, "JobID and Result are required", http.StatusBadRequest)
		return
	}

	// Notify the waiting handler via the result store.
	if !p.resultStore.Notify(payload.JobID, payload.Result) {
		// This means the original request already timed out and was deregistered.
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

// JobDispatcherHandler provides the next available job to a requesting worker.
func (p *Proxy) JobDispatcherHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	job := p.jobQueue.Get()
	if job == nil {
		// No job available in the queue
		w.WriteHeader(http.StatusNoContent)
		return
	}

	slog.Info("dispatching job to worker", "event", "query.assigned", "job_id", job.ID)
	job.Status = api.StatusRunning
	job.UpdatedAt = time.Now().UTC()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(job); err != nil {
		slog.Error("failed to encode job for worker", "job_id", job.ID, "error", err)
		// We can't write to the header here as it's already been written.
		// The worker will likely time out and retry.
		// It would be good to try and put the job back in the queue.
		p.jobQueue.Add(job) // Re-queue the job
	}
}
