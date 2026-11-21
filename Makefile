.PHONY: run demo test test-short race fmt vet build

build:
	go build -o bin/kitewheel ./cmd/kitewheel

run:
	go run ./cmd/kitewheel -data ./data

demo:
	go run ./cmd/kitewheel -demo -addr 127.0.0.1:8080

test:
	go test ./... -timeout 1800s

test-short:
	go test -short ./... -timeout 600s

race:
	go test -race ./internal/engine ./internal/server ./internal/store ./internal/calendar -timeout 900s

fmt:
	gofmt -w $(shell find . -name '*.go' -not -path './.git/*')

vet:
	go vet ./...
