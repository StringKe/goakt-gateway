# Contributing

## Prerequisites

Install `mise` and the Go `1.26.5` toolchain.

## Local verification

Run these commands before opening a pull request:

```sh
gofmt -l .
mise exec go@1.26.5 -- go mod verify
mise exec go@1.26.5 -- go mod tidy -diff
mise exec go@1.26.5 -- go build ./...
mise exec go@1.26.5 -- go vet ./...
mise exec go@1.26.5 -- golangci-lint run ./...
mise exec go@1.26.5 -- go test -race -count=1 -covermode=atomic -coverprofile=coverage.out ./...
mise exec go@1.26.5 -- go tool cover -func=coverage.out
mise exec go@1.26.5 -- go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 .github/workflows
docker compose up -d
TEST_REDIS_ADDR=127.0.0.1:6399 mise exec go@1.26.5 -- go test -race -count=1 ./...
TEST_REDIS_ADDR=127.0.0.1:6400 mise exec go@1.26.5 -- go test -race -count=1 ./...
docker compose down
git diff --check
```

Run `docker compose down` after each integration run, including failed runs.
