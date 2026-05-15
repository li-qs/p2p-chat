NAME=p2p-chat
VERSION=$(shell git rev-parse --short HEAD)
BIN_DIR=$(CURDIR)/build

APP_DIR=$(CURDIR)/cmd/app
CLI_DIR=$(CURDIR)/cmd/cli

APP_NAME=$(NAME)
CLI_NAME=$(NAME)-cli

PLATFORMS=linux-amd64 darwin-amd64 windows-amd64 darwin-arm64

.PHONY: all app cli clean

all: app cli

app: $(PLATFORMS:%=app-%)
cli: $(PLATFORMS:%=cli-%)

app-%:
	@mkdir -p $(BIN_DIR)
	GOOS=$(word 1,$(subst -, ,$*)) \
	GOARCH=$(word 2,$(subst -, ,$*)) \
	CGO_ENABLED=0 \
	go build -o $(BIN_DIR)/$(APP_NAME)-$*-$(VERSION) $(APP_DIR)

cli-%:
	@mkdir -p $(BIN_DIR)
	GOOS=$(word 1,$(subst -, ,$*)) \
	GOARCH=$(word 2,$(subst -, ,$*)) \
	CGO_ENABLED=0 \
	go build -o $(BIN_DIR)/$(CLI_NAME)-$*-$(VERSION) $(CLI_DIR)
