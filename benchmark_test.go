package skein

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"skein/internal/api"
	"sort"
	"testing"
	"time"
)

type CombinedStats struct {
	ProfilingStats api.ProfilingStats
	GoProfileStats api.GoProfileStats
}

var profilingEnabledStats []CombinedStats
var profilingDisabledStats []CombinedStats

func BenchmarkQueryExecution(b *testing.B) {
	// Wait for the proxy to be healthy
	if !isProxyHealthy() {
		b.Fatal("proxy did not become healthy")
	}

	//parquetPath := getParquetPath()
	parquetPath := "./datasets/taxi/taxi_2019_*.parquet"
	query := fmt.Sprintf(`
		SELECT count(*) as total_count, avg(passenger_count)
		FROM '%s'
		WHERE (pickup_at BETWEEN '2019-04-15' AND '2019-04-20')
			or (pickup_at BETWEEN '2019-05-18' AND '2019-04-25')
			or (pickup_at BETWEEN '2019-06-01' AND '2019-06-06');
	`, parquetPath)

	b.Run("CollectProfilingEnabled", func(b *testing.B) {
		for b.Loop() {
			stats, err := runQuery(query, false)
			if err != nil {
				b.Fatal(err)
			}
			profilingEnabledStats = append(profilingEnabledStats, stats)
		}
	})

	b.Run("CollectProfilingDisabled", func(b *testing.B) {
		for b.Loop() {
			stats, err := runQuery(query, true)
			if err != nil {
				b.Fatal(err)
			}
			profilingDisabledStats = append(profilingDisabledStats, stats)
		}
	})

	printStats("Profiling Enabled", profilingEnabledStats)
	printStats("Profiling Disabled", profilingDisabledStats)
}

func isProxyHealthy() bool {
	healthy := false
	for i := 0; i < 30; i++ {
		resp, err := http.Get(proxyURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			healthy = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	return healthy
}

func runQuery(query string, disableProfiling bool) (CombinedStats, error) {
	req := api.QueryRequest{
		UserID:           "benchmark-user",
		Query:            query,
		Priority:         api.PriorityNormal,
		DisableProfiling: disableProfiling,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return CombinedStats{}, err
	}

	resp, err := http.Post(proxyURL+"/query", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return CombinedStats{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return CombinedStats{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return CombinedStats{}, err
	}

	var result api.QueryResults
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		return CombinedStats{}, err
	}

	var jobResult api.JobResult
	err = json.Unmarshal(respBody, &jobResult)
	if err != nil {
		return CombinedStats{}, err
	}

	return CombinedStats{
		ProfilingStats: result.Profile,
		GoProfileStats: result.GoProfile,
	}, nil
}

func printStats(name string, stats []CombinedStats) {
	if len(stats) == 0 {
		return
	}

	fmt.Printf("--- %s ---\n", name)

	totalBytesWritten := make([]float64, len(stats))
	totalBytesRead := make([]float64, len(stats))
	rowsReturned := make([]float64, len(stats))
	latency := make([]float64, len(stats))
	cpuTime := make([]float64, len(stats))
	executeTime := make([]float64, len(stats))
	queryTime := make([]float64, len(stats))

	for i, s := range stats {
		totalBytesWritten[i] = float64(s.ProfilingStats.TotalBytesWritten)
		totalBytesRead[i] = float64(s.ProfilingStats.TotalBytesRead)
		rowsReturned[i] = float64(s.ProfilingStats.RowsReturned)
		latency[i] = s.ProfilingStats.Latency
		cpuTime[i] = s.ProfilingStats.CPUTime
		executeTime[i] = float64(s.GoProfileStats.ExecuteTime.Microseconds())
		queryTime[i] = float64(s.GoProfileStats.QueryTime.Microseconds())
	}

	fmt.Printf("TotalBytesWritten: p50=%.2f, p99=%.2f\n", percentile(totalBytesWritten, 50), percentile(totalBytesWritten, 99))
	fmt.Printf("TotalBytesRead:    p50=%.2f, p99=%.2f\n", percentile(totalBytesRead, 50), percentile(totalBytesRead, 99))
	fmt.Printf("RowsReturned:      p50=%.2f, p99=%.2f\n", percentile(rowsReturned, 50), percentile(rowsReturned, 99))
	fmt.Printf("Latency:           p50=%.2f, p99=%.2f\n", percentile(latency, 50), percentile(latency, 99))
	fmt.Printf("CPUTime:           p50=%.2f, p99=%.2f\n", percentile(cpuTime, 50), percentile(cpuTime, 99))
	fmt.Printf("ExecuteTime:       p50=%.2fµs, p99=%.2fµs\n", percentile(executeTime, 50), percentile(executeTime, 99))
	fmt.Printf("QueryTime:         p50=%.2fµs, p99=%.2fµs\n", percentile(queryTime, 50), percentile(queryTime, 99))
}

func percentile(data []float64, perc float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sort.Float64s(data)
	k := (perc / 100) * float64(len(data)-1)
	f := int(k)
	c := k - float64(f)
	if f+1 < len(data) {
		return data[f] + c*(data[f+1]-data[f])
	}
	return data[f]
}
