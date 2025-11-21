package main

import (
	"log/slog"
	"net/http"
	"os"
	"skein/internal/proxy"
)

func main() {
	// Set up structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	jobQueue := proxy.NewJobQueue()
	resultStore := proxy.NewResultStore()
	p := proxy.NewProxy(jobQueue, resultStore)

	http.HandleFunc("/query", p.QueryHandler)
	http.HandleFunc("/internal/result", p.ResultHandler)
	http.HandleFunc("/internal/job/next", p.JobDispatcherHandler)
	http.HandleFunc("/healthz", p.HealthCheckHandler)

	slog.Info("Proxy server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("failed to start server", "error", err)
		os.Exit(1)
	}
}
