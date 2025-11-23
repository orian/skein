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

	// Instantiate the new worker registry and the old queue systems.
	registry := proxy.NewWorkerRegistry()
	jobQueue := proxy.NewJobQueue()
	resultStore := proxy.NewResultStore()

	// The Proxy now holds all dispatching and result systems.
	p := proxy.NewProxy(registry, jobQueue, resultStore)

	// User-facing and health-check endpoints.
	http.HandleFunc("/query", p.QueryHandler)
	http.HandleFunc("/healthz", p.HealthCheckHandler)

	// Internal endpoints for worker communication.
	http.HandleFunc("/internal/job/result", p.ResultHandler)
	http.HandleFunc("/internal/job/next", p.JobDispatcherHandler)
	http.HandleFunc("/internal/worker/register", p.RegisterWorkerHandler)
	http.HandleFunc("/internal/worker/heartbeat", p.HeartbeatHandler)
	http.HandleFunc("/internal/worker/goodbye", p.DeregisterWorkerHandler)

	slog.Info("Proxy server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("failed to start server", "error", err)
		os.Exit(1)
	}
}
