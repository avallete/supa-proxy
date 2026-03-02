.PHONY: build test test-integration test-e2e test-stress bench lint fmt vet docker-up docker-down clean

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o pg-gateway .

test:
	go test -v -race ./...

test-integration:
	go test -tags integration -v -race -timeout 5m ./...

test-e2e:
	cd e2e && bun install && bun test --timeout 120000

test-stress:
	cd e2e && bun test stress.test.ts --timeout 600000

bench:
	go test -tags integration -bench=. -benchmem -timeout 5m ./...

lint:
	golangci-lint run

fmt:
	go fmt ./...

vet:
	go vet ./...

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

clean:
	rm -f pg-gateway
