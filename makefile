NAME := lazarette
OWNER := byuoitav
PKG := github.com/${OWNER}/${NAME}
PKG_LIST := $(shell go list ${PKG}/...)

.PHONY: all deps build test test-cov clean

all: build

deps:
	@go generate ./...
	@go mod download

build: deps
	@go build -i -o dist/${NAME} ${PKG}

test:
	@go test -v ${PKG_LIST}

test-cov:
	@go test -coverprofile coverage.txt ${PKG_LIST}

clean:
	@rm -f dist/

lint:
	@golangci-lint run --tests=false
