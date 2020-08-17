#!/usr/bin/env bash

cd /go/src/BCDns_0.1/bcDns/data

#NF表示最后一列输出的时间，总时间/读取发送成功的条数
latency=$(grep "execute successfully" run.log | awk '{sum+=$NF} END {print sum/NR}')

l=0
timeStart=0
timeEnd=0
fLatency=0

grep "execute successfully" run.log > tmp.log
while read line
do
    if [[ l -eq 0 ]]; then
        l=1
    elif [[ l -eq 1 ]]; then
        timeStart=$(echo $line| awk '{print $1}')
    	timeEnd=$(echo $line| awk '{print $1}')
	    fLatency=$(echo $line| awk '{print $NF}'| awk -F "." '{print $1}')
        l=2
    else
	timeEnd=$(echo $line| awk '{print $1}')
    fi
done<tmp.log

//最后一条提案执行成功的时间-第一条提案执行开始的时间 + flatency（对最后一条执行成功的提案时间取整）
gap=$(($timeEnd-$timeStart+$fLatency))

amount=$(grep "execute successfully" run.log | wc -l)

sendAmount=$(grep "execute successfully" run.log | tail -1| awk '{print $3}')

throughout=$(printf "%.5f" `echo "scale=5;$amount/$gap"|bc`) --gap

sendRate=$(printf "%.5f" `echo "scale=5;$sendAmount/$gap"|bc`)

echo "$latency $throughout $sendRate"