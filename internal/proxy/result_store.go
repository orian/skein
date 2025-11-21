package proxy

import (
	"skein/internal/api"
	"sync"
)

// ResultStore is a thread-safe map to hold channels for waiting query results.
// This is the mechanism that allows a synchronous API call to wait for an
// asynchronous job to be completed by a worker.
type ResultStore struct {
	mu      sync.Mutex
	results map[string]chan *api.JobResult
}

// NewResultStore creates a new ResultStore.
func NewResultStore() *ResultStore {
	return &ResultStore{
		results: make(map[string]chan *api.JobResult),
	}
}

// Register creates a channel for a given job ID and stores it.
// The caller can then wait on this channel for the result.
func (rs *ResultStore) Register(jobID string) chan *api.JobResult {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// The channel should be buffered to prevent the worker from blocking
	// if the handler has already timed out.
	resultChan := make(chan *api.JobResult, 1)
	rs.results[jobID] = resultChan
	return resultChan
}

// Notify sends a result to the waiting channel for a given job ID.
// It's called by the worker-facing endpoint when a result is received.
func (rs *ResultStore) Notify(jobID string, result *api.JobResult) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if ch, ok := rs.results[jobID]; ok {
		ch <- result
		return true
	}
	// This can happen if the original request timed out and was deregistered.
	return false
}

// Deregister removes a job's result channel.
// This should be called once the query handler is done (either it received
// the result or it timed out).
func (rs *ResultStore) Deregister(jobID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if ch, ok := rs.results[jobID]; ok {
		close(ch)
		delete(rs.results, jobID)
	}
}
