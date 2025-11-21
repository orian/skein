package skein

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"skein/internal/api"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	proxyURL = "http://localhost:8080"
)

func getParquetPath() string {
	path := os.Getenv("E2E_DATA_PATH")
	if path == "" {
		return "/data/taxi_2019_04.parquet" // Default path inside the worker container
	}
	return path
}

func TestEndToEndQueryExecution(t *testing.T) {
	// Wait for the proxy to be healthy
	assert.Eventually(t, func() bool {
		resp, err := http.Get(proxyURL + "/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 1*time.Second, "proxy did not become healthy")

	parquetPath := getParquetPath()

	t.Run("Simple Query", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT count(*) as total_count
			FROM '%s'
			WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20';
		`, parquetPath)

		req := api.QueryRequest{
			UserID:   "e2e-test-user-1",
			Query:    query,
			Priority: api.PriorityNormal,
		}

		body, err := json.Marshal(req)
		assert.NoError(t, err)

		resp, err := http.Post(proxyURL+"/query", "application/json", bytes.NewBuffer(body))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected OK status for simple query")

		// Read the response body
		respBody, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)

		// Define the expected JSON response
		expectedJSON := `{
			"column_names": ["total_count"],
			"column_types": [{"type": "BIGINT", "nullable": false}],
			"column_data": [[1276565]]
		}`

		assert.JSONEq(t, expectedJSON, string(respBody), "The JSON response should match the expected output.")
	})

	t.Run("Parameterized Query", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT count(*) as total_count
			FROM '%s'
			WHERE passenger_count = $pax_count;
		`, parquetPath)

		req := api.QueryRequest{
			UserID:   "e2e-test-user-2",
			Query:    query,
			Params:   map[string]interface{}{"pax_count": 2},
			Priority: api.PriorityNormal,
		}

		body, err := json.Marshal(req)
		assert.NoError(t, err)

		resp, err := http.Post(proxyURL+"/query", "application/json", bytes.NewBuffer(body))
		assert.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected OK status for parameterized query")

		// Read the response body
		respBody, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)

		// Define the expected JSON response
		expectedJSON := `{
			"column_names": ["total_count"],
			"column_types": [{"type": "BIGINT", "nullable": false}],
			"column_data": [[1113704]]
		}`

		assert.JSONEq(t, expectedJSON, string(respBody), "The JSON response should match the expected output.")
	})
}
