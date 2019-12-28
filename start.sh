#!/bin/bash

cd /go/src/BCDns_client

if [[ $# == 1 ]]; then
    echo 1
	go run main.go --mode $1
else
    echo 2
	go run main.go --mode $1 --frq $2
fi