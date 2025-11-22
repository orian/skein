package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path"
	"skein/internal/api"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	pollInterval = 100 * time.Millisecond
	dbPath       = "" // Use in-memory DuckDB
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	proxyURL := os.Getenv("PROXY_BASE_URL")
	if proxyURL == "" {
		proxyURL = "http://localhost:8080"
	}
	slog.Info("Using proxy URL", "url", proxyURL)

	slog.Info("Worker starting...")
	workerID := "worker-" + time.Now().Format("20060102-150405")
	slog.Info("Worker ID", "id", workerID)

	workerDelay, _ := time.ParseDuration(os.Getenv("WORKER_DELAY"))

	// Main worker loop
	for {
		job := fetchJob(proxyURL)
		if job == nil {
			time.Sleep(pollInterval)
			continue
		}

		slog.Info("executing job", "event", "query.execution.started", "job_id", job.ID, "worker_id", workerID)
		startTime := time.Now()
		result, err := ExecuteJob(context.Background(), job, dbPath)
		duration := time.Since(startTime)
		result.GoProfile.ExecuteTime = duration

		if err != nil {
			slog.Error("job execution failed", "event", "query.execution.failed", "job_id", job.ID, "worker_id", workerID, "error", err)
			result = &api.JobResult{Error: err.Error()}
		} else {
			slog.Info("job execution completed", "event", "query.execution.completed", "job_id", job.ID, "worker_id", workerID, "duration_ms", duration.Milliseconds())
		}

		if workerDelay > 0 {
			slog.Info("delaying result submission", "job_id", job.ID, "delay", workerDelay)
			time.Sleep(workerDelay)
		}

		submitResult(proxyURL, job.ID, result)
	}
}

// fetchJob polls the proxy for the next available job.
func fetchJob(proxyURL string) *api.Job {
	resp, err := http.Get(proxyURL + "/internal/job/next")
	if err != nil {
		slog.Error("failed to fetch job from proxy", "error", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		// No job available, this is normal.
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("proxy returned non-OK status for job fetch", "status", resp.Status)
		return nil
	}

	var job api.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		slog.Error("failed to decode job from proxy response", "error", err)
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

	// Get column names
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get column types
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
			// Handle any type conversions if necessary, e.g., byte slices to strings
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

	// ,"LATENCY":"true","EXTRA_INFO": "false", "OPERATOR_CARDINALITY": "false", "OPERATOR_TIMING": "false", "RESULT_SET_SIZE":"true", "ROWS_RETURNED":"true"
	// customProfiling := `PRAGMA custom_profiling_settings = '{"CPU_TIME": "true","ROWS_RETURNED": "true"}';`
	//_, err = db.ExecContext(ctx, customProfiling)
	//if err != nil {
	//	return "", fmt.Errorf("failed to set custom profiling: %w", err)
	//}

	return profileFileName, nil
}

func disableProfiling(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec("PRAGMA disable_profiling;"); err != nil {
		slog.Warn("failed to disable profiling", "error", err)
	}
	return nil
}

func collectProfileStats(profileFileName string) []byte {
	if _, err := os.Stat(profileFileName); os.IsNotExist(err) {
		slog.Warn("Profiling file does not exist", "file", profileFileName)
	} else if err != nil {
		slog.Warn("Error stating profiling file", "file", profileFileName, "error", err)
	}

	// Read profiling output
	profileBytes, err := os.ReadFile(profileFileName)
	if err != nil {
		slog.Warn("failed to read profiling output file", "file", profileFileName, "error", err)
	}

	// Clean up profiling file
	if err := os.Remove(profileFileName); err != nil {
		slog.Warn("failed to remove profiling output file", "file", profileFileName, "error", err)
	}
	return profileBytes
}

// ExecuteJob runs the query using DuckDB.
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

// submitResult sends the result of a job execution back to the proxy.
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

	resp, err := http.Post(proxyURL+"/internal/result", "application/json", bytes.NewBuffer(body))
	if err != nil {
		slog.Error("failed to submit result to proxy", "job_id", jobID, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Error("proxy returned non-OK status for result submission", "job_id", jobID, "status", resp.Status)
	}
}
