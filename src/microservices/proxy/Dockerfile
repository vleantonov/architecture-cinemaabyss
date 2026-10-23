FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /proxy-service .

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /proxy-service /proxy-service
USER 65532:65532
EXPOSE 8000
ENTRYPOINT ["/proxy-service"]
