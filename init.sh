#!/bin/bash

# export HOST=$(ifconfig |grep ens3 -C 1|grep "inet"|awk  '{print $2}'|awk -F "." '{printf "s%d", $4}')
# export BCDNSConfFile="/go/src/BCDns_0.1/bcDns/conf/$HOST/BCDNS"
# export CertificatesPath="/go/src/BCDns_0.1/certificateAuthority/conf/$HOST/"
# bash /go/src/BCDns_0.1/init.sh
# bash /go/src/BCDns_daemon/startDaemon.sh

cd /go/src/BCDns_0.1/certificateAuthority/conf/
if [ ! -d "./$HOST" ]; then
    mkdir ./$HOST
fi

#expect -c "
#    spawn ./generateCert.sh ens3 CH BJ BJ BUPT 222 $HOST
#    expect {
#        \"*pass*\" {set timeout 300; send \"0401\r\"; exp_continue;}
#    }"

cd /go/src/BCDns_0.1/bcDns/conf/
if [ ! -d "./$HOST" ]; then
    mkdir ./$HOST
fi

cp BCDNS.json ./$HOST

sed "s/hostname/$HOST/g" ./$HOST/BCDNS.json -i

cd /go/src/BCDns_0.1/bcDns

if [ ! -d "./data" ]; then
    mkdir ./data
fi

mount -t tmpfs -o size=1g tmpfs ./data/
