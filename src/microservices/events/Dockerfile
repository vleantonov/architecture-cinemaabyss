FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /events-service .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /events-service /events-service
USER 65532:65532
EXPOSE 8082
ENTRYPOINT ["/events-service"]
