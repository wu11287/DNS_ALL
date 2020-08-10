FROM golang:1.13.0

WORKDIR $GOPATH/src

COPY ./BCDns_0.1 ./BCDns_0.1

COPY ./BCDns_client ./BCDns_client

COPY ./BCDns_daemon ./BCDns_daemon

ENV GO111MODULE="on" GOPROXY="https://goproxy.cn"

RUN apt update && apt install -y net-tools && apt install -y expect && apt install -y vim && apt install -y bc

RUN cd BCDns_0.1 && go mod tidy && cd ../BCDns_daemon && go mod tidy