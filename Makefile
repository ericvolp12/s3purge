BIN := s3purge
MAIN := main.go

.PHONY: build
build:
	@echo "Building..."
	@go build -o $(BIN) $(MAIN)
