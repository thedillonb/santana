BINARY=dist/santana
PACKAGE=./cmd/main.go
BUILD=0.0.0-Development
LDFLAGS=-ldflags "-X main.build=${BUILD}"

.PHONY: build clean

build: clean
	go build ${LDFLAGS} -o ${BINARY} ${PACKAGE}

build_linux:
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o ${BINARY} ${PACKAGE}

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi