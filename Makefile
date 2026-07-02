NAME=p2p-chat
BUILD_DIR=$(CURDIR)/build
APP_DIR=$(CURDIR)/cmd/app

PLATFORMS=linux-amd64 darwin-amd64 windows-amd64 darwin-arm64

.PHONY: all clean app

all: clean app

app: $(PLATFORMS:%=app-%)

app-%:
	@mkdir -p $(BUILD_DIR)
	GOOS=$(word 1,$(subst -, ,$*)) \
	GOARCH=$(word 2,$(subst -, ,$*)) \
	CGO_ENABLED=0 \
	go build -o $(BUILD_DIR)/$(NAME)-$* $(APP_DIR)

clean:
	rm -rf $(BUILD_DIR)
