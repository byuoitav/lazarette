NAME := lazarette
OWNER := byuoitav
PKG := github.com/${OWNER}/${NAME}
PKG_LIST := $(shell go list ${PKG}/...)

.PHONY: all deps build test test-cov clean

all: build

# must have protoc installed
deps:
	@go get -u github.com/golang/protobuf/protoc-gen-go
	@go generate ./...
	@go mod download

build: deps
	@go build -i -o dist/${NAME} ${PKG}

test:
	@go test -v ${PKG_LIST}

test-cov:
	@go test -coverprofile=coverage.txt -covermode=atomic ${PKG_LIST}

clean:
	@rm -f dist/

lint:
	@golangci-lint run --tests=false
