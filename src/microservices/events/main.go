package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

var topics = []string{"user-events", "payment-events", "movie-events"}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	port := envOrDefault("PORT", "8082")
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("PORT must be an integer between 1 and 65535")
	}
	brokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		if _, _, err := net.SplitHostPort(brokers[i]); err != nil {
			return fmt.Errorf("invalid KAFKA_BROKERS address: %w", err)
		}
	}
	writer := &kafka.Writer{
		Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, BatchSize: 1, MaxAttempts: 3,
		ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, GroupID: envOrDefault("KAFKA_GROUP_ID", "cinemaabyss-events"),
		GroupTopics: topics, MinBytes: 1, MaxBytes: 1 << 20,
		StartOffset: kafka.FirstOffset, MaxWait: time.Second,
	})
	defer reader.Close()
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	defer stopConsumer()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consume(consumerCtx, reader, logger)
	}()

	server := &http.Server{
		Addr: ":" + port, Handler: newHandler(writer, func(ctx context.Context) error {
			return checkKafka(ctx, brokers)
		}, logger),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()
	logger.Info("events service started", "port", port, "topics", topics)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serverDone:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown failed", "error", err)
		server.Close()
	}
	stopConsumer()
	<-consumerDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	return serveErr
}

func checkKafka(ctx context.Context, brokers []string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var lastErr error
	for _, broker := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}
		deadline, _ := ctx.Deadline()
		conn.SetDeadline(deadline)
		partitions, err := conn.ReadPartitions(topics...)
		conn.Close()
		if err == nil {
			ready := make(map[string]bool)
			for _, p := range partitions {
				if p.Leader.ID >= 0 {
					ready[p.Topic] = true
				}
			}
			if len(ready) == len(topics) {
				return nil
			}
			err = errors.New("event topics are not ready")
		}
		lastErr = err
	}
	return lastErr
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
