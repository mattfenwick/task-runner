test:
	go test ./pkg/...

install:
	go get -u golang.org/x/lint/golint

lint:
	fmt
	vet
	# ./hack/verify-golint.sh

fmt:
	go fmt ./cmd/... ./pkg/...

vet:
	go vet ./cmd/... ./pkg/...
