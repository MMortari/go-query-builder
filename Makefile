test:
	go test . -v -coverprofile=bin/c.out

coverage:
	go tool cover -html="bin/c.out"

lint:
	golangci-lint run