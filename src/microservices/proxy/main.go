package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	port             string
	monolithURL      *url.URL
	moviesURL        *url.URL
	gradualMigration bool
	moviesPercent    int
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           newProxy(cfg, rand.IntN),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("Proxy listening on :%s; gradual migration: %t; movies migration: %d%%", cfg.port, cfg.gradualMigration, cfg.moviesPercent)
	log.Fatal(server.ListenAndServe())
}

func newProxy(cfg config, draw func(int) int) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second

	return &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(r *httputil.ProxyRequest) {
			target := cfg.monolithURL
			path := r.In.URL.Path
			isMovies := path == "/api/movies" || strings.HasPrefix(path, "/api/movies/")
			if isMovies && cfg.gradualMigration && draw(100) < cfg.moviesPercent {
				target = cfg.moviesURL
			}
			r.SetURL(target)
			r.SetXForwarded()
		},
	}
}

func loadConfig() (config, error) {
	cfg := config{port: envOrDefault("PORT", "8000")}
	port, err := strconv.Atoi(cfg.port)
	if err != nil || port < 1 || port > 65535 {
		return cfg, fmt.Errorf("PORT must be an integer between 1 and 65535")
	}
	cfg.gradualMigration, err = strconv.ParseBool(envOrDefault("GRADUAL_MIGRATION", "false"))
	if err != nil {
		return cfg, fmt.Errorf("GRADUAL_MIGRATION must be a boolean (true or false)")
	}
	cfg.moviesPercent, err = strconv.Atoi(envOrDefault("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || cfg.moviesPercent < 0 || cfg.moviesPercent > 100 {
		return cfg, fmt.Errorf("MOVIES_MIGRATION_PERCENT must be an integer between 0 and 100")
	}
	cfg.monolithURL, err = backendURL("MONOLITH_URL", "http://localhost:8080")
	if err != nil {
		return cfg, err
	}
	cfg.moviesURL, err = backendURL("MOVIES_SERVICE_URL", "http://localhost:8081")
	return cfg, err
}

func backendURL(key, fallback string) (*url.URL, error) {
	u, err := url.Parse(envOrDefault(key, fallback))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("%s must be an HTTP(S) URL without credentials, query or fragment", key)
	}
	return u, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
