#!/bin/bash

mkdir -p bin

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/tpch-rf ./main.go
echo "linux build success..."


CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./bin/tpch-rf.exe ./main.go
echo "windows build success..."


CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./bin/tpch-rf_mac ./main.go
echo "macos build success..."