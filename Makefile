BINARY_DIR=./bin
BINARY_NAME=event_managear
BINARY_PATH=${BINARY_DIR}/${BINARY_NAME}

build:
	go build -o ${BINARY_PATH} event_manager

clean:
	rm -rf bin
	go clean

run: build
	${BINARY_PATH}
