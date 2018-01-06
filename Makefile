BINARY=dist/santana
PACKAGE=./cmd/main.go
BUILD=0.0.0-Development
LDFLAGS=-ldflags "-X main.build=${BUILD}"

.PHONY: build clean

gen:
	protoc -I protocol/ protocol/protocol.proto --go_out=plugins=grpc:protocol

build: clean
	go build ${LDFLAGS} -o ${BINARY} ${PACKAGE}

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi