#!/bin/bash

cd /go/src/BCDns_0.1/bcDns/cmd

rm -rf ../data/blockchain_*

if [[ $# -eq 1 ]]; then
    sed "s/\(false\|true\)/$1/g" ../conf/$HOST/BCDNS.json -i
elif [[ $# -eq 2 ]]; then
    sed "s/\(false\|true\)/$1/g" ../conf/$HOST/BCDNS.json -i
    sed "s/\(PBFT\|MYBFT\)/$2/g" ../conf/$HOST/BCDNS.json -i
elif [[ $# -eq 3 ]]; then
    sed "s/\(false\|true\)/$1/g" ../conf/$HOST/BCDNS.json -i
    sed "s/\(PBFT\|MYBFT\)/$2/g" ../conf/$HOST/BCDNS.json -i
    sed "s/\(yes\|no\)/$3/g" ../conf/$HOST/BCDNS.json -i
elif [[ $# -eq 4 ]]; then
    sed "s/\(false\|true\)/$1/g" ../conf/$HOST/BCDNS.json -i
    sed "s/\(PBFT\|MYBFT\)/$2/g" ../conf/$HOST/BCDNS.json -i
    sed "s/\(yes\|no\)/$3/g" ../conf/$HOST/BCDNS.json -i
    sed "s/\"DELAY\": [0-9]/\"DELAY\": $4/g" ../conf/$HOST/BCDNS.json -i
fi
#go run main.go > ../data/run.log
go run main.go
