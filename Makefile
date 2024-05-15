# SPDX-FileCopyrightText: 2024 Christoph Mewes
# SPDX-License-Identifier: MIT

GIT_VERSION = $(shell git describe --tags --always)
GIT_HEAD ?= $(shell git log -1 --format=%H)
NOW_GO_RFC339 = $(shell date --utc +'%Y-%m-%dT%H:%M:%SZ')

export CGO_ENABLED ?= 0
export GOFLAGS ?= -mod=readonly -trimpath
OUTPUT_DIR ?= _build
GO_DEFINES ?= -X main.Tag=$(GIT_VERSION) -X main.Commit=$(GIT_HEAD)
GO_LDFLAGS += -w -extldflags '-static' $(GO_DEFINES)
GO_BUILD_FLAGS ?= -v -ldflags '$(GO_LDFLAGS)'
GO_TEST_FLAGS ?= -v -race

default: build

.PHONY: build
build:
	go build $(GO_BUILD_FLAGS) -o $(OUTPUT_DIR)/ .

.PHONY: test
test:
	CGO_ENABLED=1 go test $(GO_TEST_FLAGS) ./...

.PHONY: clean
clean:
	rm -rf $(OUTPUT_DIR)

.PHONY: lint
lint:
	golangci-lint run ./...
