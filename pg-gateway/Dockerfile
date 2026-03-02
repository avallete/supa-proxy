FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY pg-gateway/go.mod pg-gateway/go.sum* ./pg-gateway/
COPY multigres/ ./multigres/
WORKDIR /src/pg-gateway
RUN go mod download 2>/dev/null || true
COPY pg-gateway/ .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /pg-gateway .

FROM scratch
COPY --from=builder /pg-gateway /pg-gateway
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 15432 2112
ENTRYPOINT ["/pg-gateway"]
