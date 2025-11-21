package api

import "time"

// ProfilingStats holds the extracted profiling information for a query.
type ProfilingStats struct {
	TotalBytesWritten int     `json:"total_bytes_written"`
	TotalBytesRead    int     `json:"total_bytes_read"`
	RowsReturned      int     `json:"rows_returned"`
	Latency           float64 `json:"latency"`
	CPUTime           float64 `json:"cpu_time"`
}

type GoProfileStats struct {
	ExecuteTime time.Duration
	QueryTime   time.Duration
}

// QueryResults is the structure of the data returned to the API user.
// It contains both the query result data and profiling information.
type QueryResults struct {
	ColumnNames []string       `json:"column_names,omitempty"`
	ColumnTypes []ColumnType   `json:"column_types,omitempty"`
	ColumnData  []interface{}  `json:"column_data,omitempty"`
	Error       string         `json:"error,omitempty"`
	Profile     ProfilingStats `json:"profile,omitempty"`
	GoProfile   GoProfileStats `json:"go_profile,omitempty"`
}

// This struct is used to extract profiling data from the DuckDB JSON output.
type DuckDBProfile struct {
	TotalBytesWritten int     `json:"total_bytes_written"`
	TotalBytesRead    int     `json:"total_bytes_read"`
	RowsReturned      int     `json:"rows_returned"`
	Latency           float64 `json:"latency"`
	CPUTime           float64 `json:"cpu_time"`
}
