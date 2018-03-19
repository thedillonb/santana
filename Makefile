BINARY=dist/santana
PACKAGE=./cmd/main.go
BUILD=0.0.0-Development
LDFLAGS=-ldflags "-X main.build=${BUILD}"

.PHONY: build clean

gen:
	protoc -I protocol/ protocol/protocol.proto --go_out=plugins=grpc:protocol

build: clean
	go build ${LDFLAGS} -o ${BINARY} ${PACKAGE}

build_lineserver:
	if [ -f dist/line-server ] ; then rm dist/line-server ; fi
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o dist/line-server ./cmd/line-server/main.go

build_reader:
	if [ -f dist/reader ] ; then rm dist/reader ; fi
	GOOS=linux GOARCH=amd64  go build ${LDFLAGS} -o dist/reader ./cmd/reader/main.go

clean:
	if [ -f ${} ] ; then rm ${BINARY} ; fi