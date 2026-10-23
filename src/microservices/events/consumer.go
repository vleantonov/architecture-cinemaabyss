package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

func consume(ctx context.Context, reader consumer, logger *slog.Logger) {
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("event fetch failed", "error", err)
			if !retryDelay(ctx) {
				return
			}
			continue
		}
		var e event
		if err := json.Unmarshal(message.Value, &e); err != nil || e.ID == "" || e.Type == "" {
			// Malformed messages are logged and skipped so they do not block the partition.
			logger.Error("invalid event skipped", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)
		} else {
			logger.Info("event processed", "event_id", e.ID, "event_type", e.Type,
				"payload", e.Payload, "topic", message.Topic, "partition", message.Partition, "offset", message.Offset)
		}
		// Commit only after processing; retry before fetching the next message.
		for ctx.Err() == nil {
			if err := reader.CommitMessages(ctx, message); err == nil {
				break
			} else if ctx.Err() == nil {
				logger.Error("event commit failed", "topic", message.Topic, "offset", message.Offset, "error", err)
			}
			if !retryDelay(ctx) {
				return
			}
		}
	}
}

func retryDelay(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
