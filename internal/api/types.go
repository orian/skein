package api

import (
	"encoding/json"
	"fmt"
	"time"
)

// Priority defines the priority level of a query.
type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 10
	PriorityHigh   Priority = 20
)

// QueryRequest is the structure of a query submission from a client.
type QueryRequest struct {
	UserID           string                 `json:"user_id"`
	Query            string                 `json:"query"`
	Params           map[string]interface{} `json:"params,omitempty"`
	Priority         Priority               `json:"priority"`
	DisableProfiling bool                   `json:"disable_profiling,omitempty"`
}

// QueryResponse is the initial response sent to the client after a query is submitted.
type QueryResponse struct {
	JobID string `json:"job_id"`
}

// JobStatus defines the status of a job in its lifecycle.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
)

// Job represents a query to be executed by a worker.
type Job struct {
	ID               string                 `json:"id"`
	UserID           string                 `json:"user_id"`
	Query            string                 `json:"query"`
	Params           map[string]interface{} `json:"params,omitempty"`
	Priority         Priority               `json:"priority"`
	Status           JobStatus              `json:"status"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	Result           *JobResult             `json:"result,omitempty"`
	DisableProfiling bool                   `json:"disable_profiling,omitempty"`
}

// JobResult holds the outcome of a query's execution.
// For simplicity, we'll represent results as a JSON raw message.
type JobResult struct {
	ColumnNames []string        `json:"column_names,omitempty"`
	ColumnTypes []ColumnType    `json:"column_types,omitempty"`
	ColumnData  []interface{}   `json:"column_data,omitempty"`
	Error       string          `json:"error,omitempty"`
	Profile     json.RawMessage `json:"profile,omitempty"`
	GoProfile   GoProfileStats  `json:"go_profile,omitempty"`
}

func (r *JobResult) UnmarshalJSON(data []byte) error {
	aux := &internalJobResult{}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	r.ColumnNames = aux.ColumnNames
	r.ColumnTypes = aux.ColumnTypes
	r.ColumnData = make([]interface{}, len(aux.ColumnTypes))
	r.Profile = aux.Profile
	r.GoProfile = aux.GoProfile

	for i, colType := range r.ColumnTypes {
		switch colType.Type {
		case "INTEGER":
			var col []int32
			if err := json.Unmarshal(aux.ColumnData[i], &col); err != nil {
				return err
			}
			r.ColumnData[i] = col
		case "BIGINT":
			var col []int64
			if err := json.Unmarshal(aux.ColumnData[i], &col); err != nil {
				return err
			}
			r.ColumnData[i] = col
		case "REAL", "FLOAT":
			var col []float32
			if err := json.Unmarshal(aux.ColumnData[i], &col); err != nil {
				return err
			}
			r.ColumnData[i] = col
		case "DOUBLE", "FLOAT8":
			var col []float64
			if err := json.Unmarshal(aux.ColumnData[i], &col); err != nil {
				return err
			}
			r.ColumnData[i] = col
		case "TEXT", "VARCHAR":
			var col []string
			if err := json.Unmarshal(aux.ColumnData[i], &col); err != nil {
				return err
			}
			r.ColumnData[i] = col
		case "BOOLEAN":
			var col []bool
			if err := json.Unmarshal(aux.ColumnData[i], &col); err != nil {
				return err
			}
			r.ColumnData[i] = col
		default:
			return fmt.Errorf("unsupported column type: %s", colType.Type)
		}
	}

	return nil
}

type internalJobResult struct {
	ColumnNames []string          `json:"column_names,omitempty"`
	ColumnTypes []ColumnType      `json:"column_types,omitempty"`
	ColumnData  []json.RawMessage `json:"column_data,omitempty"`
	Error       string            `json:"error,omitempty"`
	Profile     json.RawMessage   `json:"profile,omitempty"`
	GoProfile   GoProfileStats    `json:"go_profile,omitempty"`
}

// ColumnType holds the type and nullability information for a result column.
type ColumnType struct {
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
