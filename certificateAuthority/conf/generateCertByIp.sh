#!/usr/bin/env bash

cd ./tmp

rm -f $7.cer

sed "s/0.0.0.0/$1/g" v3.txt -i

ssh-keygen -t rsa -b 1024 -f $7.pem -N "" -m PEM

openssl req -new -key $7.pem -out $7.csr -subj "/C=$2/ST=$3/L=$4/O=$5/OU=$6/CN=$7"

openssl x509 -req -days 365 -sha1 -extfile v3.txt -CA RootCertificate.cer -CAkey RootPrivate.pem -CAcreateserial -in $7.csr -out $7.cer

sed "s/$1/0.0.0.0/g" v3.txt -i