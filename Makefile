test:
	go test ./...

test-e2e:
	go test -tags=e2e ./...

build:
	go build -ldflags="-s -w" ./

clean:
	rm -f edgar
