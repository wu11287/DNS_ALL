#!/usr/bin/env bash

rm -f Local*

ip=$(ifconfig |grep $1 -C 1|grep "inet"|awk  '{print $2}')

sed "s/0.0.0.0/$ip/g" v3.txt -i

ssh-keygen -t rsa -b 1024 -f LocalPrivate.pem -N "" -m PEM

openssl req -new -key LocalPrivate.pem -out LocalCertificate.csr -subj "/C=$2/ST=$3/L=$4/O=$5/OU=$6/CN=$7"

openssl x509 -req -days 365 -sha1 -extfile v3.txt -CA RootCertificate.cer -CAkey RootPrivate.pem -CAcreateserial -in LocalCertificate.csr -out LocalCertificate.cer

sed "s/$ip/0.0.0.0/g" v3.txt -i

cp Local* Root* $7
