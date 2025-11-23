package main

import (
	"log/slog"
	"net/http"
	"os"
	"skein/internal/settings"
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
	slog.SetLogLoggerLevel(slog.LevelDebug)

	proxyURL := os.Getenv("PROXY_BASE_URL")
	if proxyURL == "" {
		proxyURL = "http://localhost:8080"
	}
	slog.Info("Worker starting...", "proxy_url", proxyURL)

	w := &Worker{proxyURL: proxyURL}
	w.runWorker()
}
