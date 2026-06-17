NAME=p2p-chat
VERSION=$(shell git rev-parse --short HEAD)
BIN_DIR=$(CURDIR)/build

CLI_DIR=$(CURDIR)/cmd/cli

CLI_NAME=$(NAME)-cli

PLATFORMS=linux-amd64 darwin-amd64 windows-amd64 darwin-arm64

.PHONY: all cli clean

all: cli

cli: $(PLATFORMS:%=cli-%)

cli-%:
	@mkdir -p $(BIN_DIR)
	GOOS=$(word 1,$(subst -, ,$*)) \
	GOARCH=$(word 2,$(subst -, ,$*)) \
	CGO_ENABLED=0 \
	go build -o $(BIN_DIR)/$(CLI_NAME)-$*-$(VERSION) $(CLI_DIR)
