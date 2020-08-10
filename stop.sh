#!/bin/bash
#
#export BCDNSConfFile="/var/opt/go/src/BCDns_0.1/bcDns/conf/s1/BCDNS"
#export CertificatesPath="/var/opt/go/src/BCDns_0.1/certificateAuthority/conf/s1/"
#
#cd bcDns/cmd
#
#rm -rf blockchain_*
#
#go run main.go

pid=$(netstat -tualp|grep 8001| awk '{print $7}'| awk -F "/" '{print $1}')

kill -9 $pid