#!/bin/bash
cd /go/src/BCDns_0.1/
python3 getHostList.py

echo ----------------------------------------------------------------------------
echo ----------------------------------------------------------------------------
echo ----------------------------------------------------------------------------

cd /go/src/BCDns_0.1/certificateAuthority/conf

rm -f ./tmp/s*

cat /tmp/hosts | while read line
do
    ip=$(echo $line | awk '{print $1}')
    hostname=$(echo $line | awk '{print $2}')
    expect -c "
    spawn ./generateCertByIp.sh $ip CH BJ BJ BUPT 222 $hostname
    expect {
        \"*pass*\" {set timeout 300; send \"0401\r\"; exp_continue;}
    }"
    expect -c "
        spawn scp ./tmp/$hostname.cer root@$ip:/go/src/BCDns_0.1/certificateAuthority/conf/$hostname/LocalCertificate.cer
            expect {
            \"*yes/no*\" {send \"yes\r\";exp_continue;}
            \"*assword\" {set timeout 300; send \"NSL2020\r\"; exp_continue;}
        }"
    expect -c "
        spawn scp ./tmp/$hostname.pem root@$ip:/go/src/BCDns_0.1/certificateAuthority/conf/$hostname/LocalPrivate.pem
            expect {
            \"*yes/no*\" {send \"yes\r\";exp_continue;}
            \"*assword\" {set timeout 300; send \"NSL2020\r\"; exp_continue;}
        }"
    expect -c "
        spawn scp ./tmp/RootCertificate.cer root@$ip:/go/src/BCDns_0.1/certificateAuthority/conf/$hostname/RootCertificate.cer
            expect {
            \"*yes/no*\" {send \"yes\r\";exp_continue;}
            \"*assword\" {set timeout 300; send \"NSL2020\r\"; exp_continue;}
        }"
done

echo ----------------------------------------------------------------------------
echo ----------------------------------------------------------------------------
echo ----------------------------------------------------------------------------


for fn in `ls ./tmp/s*.cer`
do
    cat /tmp/hosts | while read line
    do
        ip=$(echo $line | awk '{print $1}')
        hostname=$(echo $line | awk '{print $2}')
        cername="./tmp/"$hostname".cer"
        if [ $fn = $cername ];then
            continue
        fi
        expect -c "
        spawn scp $fn root@$ip:/go/src/BCDns_0.1/certificateAuthority/conf/$hostname/
	    expect {
            \"*yes/no*\" {send \"yes\r\";exp_continue;}
            \"*assword\" {set timeout 300; send \"NSL2020\r\"; exp_continue;}
        }"
    done
done
