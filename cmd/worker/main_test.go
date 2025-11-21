package main

import (
	"skein/internal/api"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecuteJob(t *testing.T) {
	// The parquet file is in the 'datasets' directory at the project root.
	// The test runs from 'cmd/worker', so the relative path is '../../datasets/'.
	const query = `
		SELECT count(*) as total_count
		FROM '../../datasets/taxi_2019_04.parquet'
		WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20';
	`

	job := &api.Job{
		ID:    "test-job-1",
		Query: query,
	}

	// Use an in-memory database by passing an empty dbPath.
	result, err := executeJob(job, "")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Error)

	assert.NotEmpty(t, result.ColumnNames)
	assert.NotEmpty(t, result.ColumnTypes)
	assert.NotEmpty(t, result.ColumnData)

	// Find the index of the "total_count" column
	totalCountIndex := -1
	for i, name := range result.ColumnNames {
		if name == "total_count" {
			totalCountIndex = i
			break
		}
	}
	assert.NotEqual(t, -1, totalCountIndex, "Expected column 'total_count' not found")

	assert.Len(t, result.ColumnData[totalCountIndex].([]interface{}), 1, "Expected 1 row of data for total_count column")

	// The value will be of the native Go type (int64 for count), but after JSON
	// marshalling/unmarshalling, it might become float64.
	count := result.ColumnData[totalCountIndex].([]interface{})[0]
	assert.Equal(t, int64(1276565), count.(int64), "expected exact count for TestExecuteJob")

	t.Logf("Query executed successfully, count: %v", count)
}

// TestExecuteJobWithParams tests a query with named parameters.
func TestExecuteJobWithParams(t *testing.T) {
	const query = `
		SELECT count(*) as total_count
		FROM '../../datasets/taxi_2019_04.parquet'
		WHERE passenger_count = $pax_count;
	`
	job := &api.Job{
		ID:    "test-job-params",
		Query: query,
		Params: map[string]interface{}{
			"pax_count": 2,
		},
	}

	result, err := executeJob(job, "")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Error)

	assert.NotEmpty(t, result.ColumnNames)
	assert.NotEmpty(t, result.ColumnTypes)
	assert.NotEmpty(t, result.ColumnData)

	// Find the index of the "total_count" column
	totalCountIndex := -1
	for i, name := range result.ColumnNames {
		if name == "total_count" {
			totalCountIndex = i
			break
		}
	}
	assert.NotEqual(t, -1, totalCountIndex, "Expected column 'total_count' not found")

	assert.Len(t, result.ColumnData[totalCountIndex].([]interface{}), 1, "Expected 1 row of data for total_count column")

	// The value will be of the native Go type (int64 for count), but after JSON
	// marshalling/unmarshalling, it might become float64.
	count := result.ColumnData[totalCountIndex].([]interface{})[0]
	assert.Equal(t, int64(1113704), count.(int64), "expected exact count for TestExecuteJobWithParams")

	t.Logf("Parameterized query executed successfully, count: %v", count)
}
