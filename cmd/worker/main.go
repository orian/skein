package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"skein/internal/api"
	"skein/internal/settings"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	dbPath = "" // Use in-memory DuckDB
)

var httpClient = &http.Client{
	Timeout: settings.LongPollTimeout + (5 * time.Second), // Must be longer than the proxy's long poll timeout.
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	proxyURL := os.Getenv("PROXY_BASE_URL")
	if proxyURL == "" {
		proxyURL = "http://localhost:8080"
	}
	slog.Info("Worker starting...", "proxy_url", proxyURL)

	// 1. Register with the proxy to get a worker ID.
	workerID, err := register(proxyURL)
	if err != nil {
		slog.Error("failed to register with proxy", "error", err)
		os.Exit(1)
	}
	slog.Info("Worker registered successfully", "worker_id", workerID)

	// 2. Handle graceful shutdown.
	// This will call deregister when the process is terminated.
	setupGracefulShutdown(proxyURL, workerID)

	// 3. Start the heartbeat goroutine.
	go runHeartbeat(proxyURL, workerID)

	workerDelay, _ := time.ParseDuration(os.Getenv("WORKER_DELAY"))

	// 4. Main job-fetching loop.
	for {
		job := fetchJob(proxyURL, workerID)
		if job == nil {
			// This means the long poll timed out or an error occurred.
			// The fetchJob function handles logging and backoff, so we just continue.
			continue
		}

		slog.Info("Executing job", "event", "query.execution.started", "job_id", job.ID, "worker_id", workerID)
		startTime := time.Now()
		result, err := ExecuteJob(context.Background(), job, dbPath)
		duration := time.Since(startTime)

		if result == nil {
			result = &api.JobResult{}
		}
		result.GoProfile.ExecuteTime = duration

		if err != nil {
			slog.Error("Job execution failed", "event", "query.execution.failed", "job_id", job.ID, "worker_id", workerID, "error", err)
			result.Error = err.Error()
		} else {
			slog.Info("Job execution completed", "event", "query.execution.completed", "job_id", job.ID, "worker_id", workerID, "duration_ms", duration.Milliseconds())
		}

		if workerDelay > 0 {
			slog.Info("Delaying result submission", "job_id", job.ID, "delay", workerDelay)
			time.Sleep(workerDelay)
		}

		submitResult(proxyURL, job.ID, result)
	}
}

// register contacts the proxy to get a unique worker ID.
func register(proxyURL string) (string, error) {
	resp, err := httpClient.Post(proxyURL+"/internal/worker/register", "application/json", nil)
	if err != nil {
		return "", fmt.Errorf("failed to send registration request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registration failed with status: %s", resp.Status)
	}

	var payload struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("failed to decode registration response: %w", err)
	}
	if payload.WorkerID == "" {
		return "", errors.New("proxy did not return a worker_id")
	}
	return payload.WorkerID, nil
}

// runHeartbeat sends periodic heartbeats to the proxy.
func runHeartbeat(proxyURL, workerID string) {
	ticker := time.NewTicker(settings.HeartbeatInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		payload, _ := json.Marshal(map[string]string{"worker_id": workerID})
		resp, err := httpClient.Post(proxyURL+"/internal/worker/heartbeat", "application/json", bytes.NewBuffer(payload))
		if err != nil {
			slog.Warn("failed to send heartbeat", "worker_id", workerID, "error", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			slog.Warn("heartbeat request failed", "worker_id", workerID, "status", resp.Status)
		}
	}
}

// setupGracefulShutdown listens for OS signals and deregisters the worker.
func setupGracefulShutdown(proxyURL, workerID string) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Shutdown signal received, deregistering worker...", "worker_id", workerID)
		req, _ := http.NewRequest("POST", proxyURL+"/internal/worker/goodbye?worker_id="+workerID, nil)
		resp, err := httpClient.Do(req)
		if err != nil {
			slog.Error("failed to send deregister request", "worker_id", workerID, "error", err)
		} else {
			resp.Body.Close()
		}
		os.Exit(0)
	}()
}

// fetchJob long-polls the proxy for the next available job.
func fetchJob(proxyURL, workerID string) *api.Job {
	slog.Debug("Polling for next job...", "worker_id", workerID)
	reqURL := fmt.Sprintf("%s/internal/job/next?worker_id=%s", proxyURL, workerID)
	resp, err := httpClient.Get(reqURL)
	if err != nil {
		slog.Error("Failed to fetch job from proxy", "error", err)
		time.Sleep(2 * time.Second) // Wait before retrying if there's a connection error.
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		slog.Debug("No job available, polling again.", "worker_id", workerID)
		return nil // Expected outcome of a long poll timeout.
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("Proxy returned non-OK status for job fetch", "status", resp.Status)
		time.Sleep(5 * time.Second) // Wait a bit longer if the proxy is misbehaving.
		return nil
	}

	var job api.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		slog.Error("Failed to decode job from proxy response", "error", err)
		return nil
	}
	return &job
}

func runQuery(ctx context.Context, db *sql.DB, job *api.Job) (*api.JobResult, error) {
	args := make([]any, 0, len(job.Params))
	for k, v := range job.Params {
		args = append(args, sql.Named(k, v))
	}

	start := time.Now()
	rows, err := db.QueryContext(ctx, job.Query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	sqlColumnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	apiColumnTypes := make([]api.ColumnType, len(sqlColumnTypes))
	for i, ct := range sqlColumnTypes {
		_, nullable := ct.Nullable()
		apiColumnTypes[i] = api.ColumnType{
			Type:     ct.DatabaseTypeName(),
			Nullable: nullable,
		}
	}

	// Initialize columnData as a slice of empty slices, one for each column
	columnData := make([]interface{}, len(columnNames))
	for i := range columnData {
		columnData[i] = make([]interface{}, 0)
	}

	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range columnNames {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append scanned values to their respective column slices
		for i, val := range values {
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			columnData[i] = append(columnData[i].([]interface{}), val)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}
	duration := time.Since(start)
	slog.Info("query execution completed", "duration_ms", duration.Milliseconds())

	return &api.JobResult{
		ColumnNames: columnNames,
		ColumnTypes: apiColumnTypes,
		ColumnData:  columnData,
		GoProfile: api.GoProfileStats{
			QueryTime: duration,
		},
	}, nil
}

func enableProfiling(ctx context.Context, db *sql.DB, id string) (string, error) {
	profileFileName := path.Join(os.TempDir(), id+".json")
	slog.Info("Generated profile file name", "file", profileFileName)

	_, err := db.ExecContext(ctx, "PRAGMA enable_profiling = 'json'")
	if err != nil {
		return "", fmt.Errorf("failed to enable profiling: %w", err)
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output = '%s'", profileFileName))
	if err != nil {
		return "", fmt.Errorf("failed to set profiling output: %w", err)
	}
	return profileFileName, nil
}

func disableProfiling(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec("PRAGMA disable_profiling;"); err != nil {
		slog.Warn("failed to disable profiling", "error", err)
	}
	return nil
}

func collectProfileStats(profileFileName string) []byte {
	profileBytes, err := os.ReadFile(profileFileName)
	if err != nil {
		slog.Warn("failed to read profiling output file", "file", profileFileName, "error", err)
	}
	if err := os.Remove(profileFileName); err != nil {
		slog.Warn("failed to remove profiling output file", "file", profileFileName, "error", err)
	}
	return profileBytes
}

func ExecuteJob(ctx context.Context, job *api.Job, dbPath string) (*api.JobResult, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	var profileFileName string
	if !job.DisableProfiling {
		if profileFileName, err = enableProfiling(ctx, db, job.ID); err != nil {
			return nil, fmt.Errorf("enable profiling: %w", err)
		}
	}

	result, runSqlErr := runQuery(ctx, db, job)
	if !job.DisableProfiling {
		if result == nil {
			result = &api.JobResult{}
		}
		result.Profile = collectProfileStats(profileFileName)

		if err = disableProfiling(ctx, db); err != nil {
			slog.Warn("failed to disable profiling", "error", err)
		}
	}

	return result, runSqlErr
}

func submitResult(proxyURL, jobID string, result *api.JobResult) {
	payload := map[string]interface{}{
		"job_id": jobID,
		"result": result,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal result payload", "job_id", jobID, "error", err)
		return
	}

	resp, err := httpClient.Post(proxyURL+"/internal/job/result", "application/json", bytes.NewBuffer(body))
	if err != nil {
		slog.Error("failed to submit result to proxy", "job_id", jobID, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Error("proxy returned non-OK status for result submission", "job_id", jobID, "status", resp.Status)
	}
}
