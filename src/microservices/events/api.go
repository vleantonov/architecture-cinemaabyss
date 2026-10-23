package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type event struct {
	ID        string          `json:"event_id"`
	Type      string          `json:"event_type"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

type publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type api struct {
	writer      publisher
	checkHealth func(context.Context) error
	logger      *slog.Logger
}

func newHandler(writer publisher, health func(context.Context) error, logger *slog.Logger) http.Handler {
	a := &api{writer: writer, checkHealth: health, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/events/health", a.handleHealth)
	mux.HandleFunc("POST /api/events/user", a.createUserEvent)
	mux.HandleFunc("POST /api/events/payment", a.createPaymentEvent)
	mux.HandleFunc("POST /api/events/movie", a.createMovieEvent)
	return mux
}

func (a *api) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := a.checkHealth(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]bool{"status": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (a *api) createUserEvent(w http.ResponseWriter, r *http.Request) {
	a.createEvent(w, r, "user")
}

func (a *api) createPaymentEvent(w http.ResponseWriter, r *http.Request) {
	a.createEvent(w, r, "payment")
}

func (a *api) createMovieEvent(w http.ResponseWriter, r *http.Request) {
	a.createEvent(w, r, "movie")
}

func (a *api) createEvent(w http.ResponseWriter, r *http.Request, kind string) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must not exceed 64 KiB"})
		return
	}
	key, err := validatePayload(kind, body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not create event ID"})
		return
	}
	e := event{ID: hex.EncodeToString(id[:]), Type: strings.ToUpper(kind[:1]) + kind[1:], CreatedAt: time.Now().UTC(), Payload: body}
	value, err := json.Marshal(e)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not encode event"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	if err := a.writer.WriteMessages(ctx, kafka.Message{Topic: kind + "-events", Key: []byte(key), Value: value}); err != nil {
		a.logger.Error("event publish failed", "event_id", e.ID, "event_type", e.Type, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "could not publish event"})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "success", "event_id": e.ID})
}

func validatePayload(kind string, body []byte) (string, error) {
	var payload struct {
		UserID    int64  `json:"user_id"`
		MovieID   int64  `json:"movie_id"`
		PaymentID int64  `json:"payment_id"`
		Action    string `json:"action"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("body must be a JSON object with integer IDs")
	}
	if payload.UserID <= 0 {
		return "", fmt.Errorf("user_id must be positive")
	}
	id := payload.UserID
	if kind == "movie" {
		id = payload.MovieID
	}
	if kind == "payment" {
		id = payload.PaymentID
	}
	if id <= 0 {
		return "", fmt.Errorf("%s_id must be positive", kind)
	}
	if kind == "payment" {
		if strings.TrimSpace(payload.Status) == "" {
			return "", fmt.Errorf("status is required")
		}
	} else if strings.TrimSpace(payload.Action) == "" {
		return "", fmt.Errorf("action is required")
	}
	return strconv.FormatInt(id, 10), nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
