package proxy

import (
	"log/slog"
	"skein/internal/api"
	"sync"
)

// JobQueue represents a simple FIFO queue for jobs.
type JobQueue struct {
	mu   sync.Mutex
	jobs []*api.Job
}

// NewJobQueue creates and returns a new FIFO JobQueue.
func NewJobQueue() *JobQueue {
	return &JobQueue{
		jobs: make([]*api.Job, 0),
	}
}

// Add adds a job to the end of the queue.
func (q *JobQueue) Add(job *api.Job) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, job)
	slog.Info("query queued", "event", "query.queued", "job_id", job.ID, "user_id", job.UserID)
}

// Get retrieves and removes the first job from the queue.
// Returns nil if the queue is empty.
func (q *JobQueue) Get() *api.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.jobs) == 0 {
		return nil
	}

	job := q.jobs[0]
	q.jobs = q.jobs[1:] // Remove the job from the queue
	return job
}

// IsEmpty checks if the queue is empty.
func (q *JobQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs) == 0
}

// Remove removes a job from the queue by its ID.
// It returns true if the job was found and removed, false otherwise.
func (q *JobQueue) Remove(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, job := range q.jobs {
		if job.ID == jobID {
			// Remove the element at index i
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			return true
		}
	}
	return false
}
