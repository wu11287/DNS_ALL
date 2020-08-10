#!/bin/bash

mode="docker"

cd /go/src/BCDns_0.1

rm -f ./certificateAuthority/conf/$HOST/s*.cer

if [[ $1 == ${mode} ]]; then
    cd /var/run/bcdns
    for i in $(seq 1 $2)
    do
        for j in $(seq 1 $2)
        do
            if [[ ${i} -ne ${j} ]]; then
                cp "s$i/LocalCertificate.cer" "s$j/s$i.cer"
            fi
        done
    done
else
    cat /tmp/hosts | while read line
    do
        ip=$(echo $line | awk '{print $1}')
        hostname=$(echo $line | awk '{print $2}')
        if [[ $HOST == $hostname ]]; then
            continue
        fi
	expect -c "
	spawn scp /tmp/hosts root@$ip:/tmp
	expect {
	    \"*assword\" {set timeout 300; send \"NSL2020\r\"; exp_continue;}
	    \"yes/no\" {send \"yes\r\"; exp_continue;}
	 }"

        expect -c "
        spawn scp root@$ip:/go/src/BCDns_0.1/certificateAuthority/conf/$hostname/LocalCertificate.cer ./certificateAuthority/conf/$HOST/$hostname.cer
	    expect {
            \"*yes/no*\" {send \"yes\r\";exp_continue;}
            \"*assword\" {set timeout 300; send \"NSL2020\r\"; exp_continue;}
        }"
    done
fi
