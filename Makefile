export GOPROXY=https://goproxy.io
GO := GO111MODULE=on go
BINARY_NAME = consumerserver
CONFIG_NAME = ./config.yml
default: dev

clean:
	@rm -rf ./$(CONFIG_NAME)
	@rm -rf ./$(BINARY_NAME)

dev: clean main.go go.sum go.mod
	@cp ./config/config.dev.yml $(CONFIG_NAME)
	@$(GO) build -o $(BINARY_NAME)
	@echo "[dev] $(BINARY_NAME) build success"

test: clean main.go go.sum go.mod
	@cp ./config/config.test.yml $(CONFIG_NAME)
	@$(GO) build -o $(BINARY_NAME)
	@echo "[test] $(BINARY_NAME) build success"

online: clean main.go go.sum go.mod
	@cp ./config/config.online.yml $(CONFIG_NAME)
	@$(GO) build -o $(BINARY_NAME)
	@echo "[online] $(BINARY_NAME) build success"

start:
	@nohup ./$(BINARY_NAME) -c ./$(CONFIG_NAME) >> /tmp/$(BINARY_NAME).log 2>&1 &

.PHONY: clean dev online test start