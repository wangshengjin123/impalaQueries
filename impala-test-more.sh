#!/bin/sh
time1=`date`
echo -e "\e[033m $time1 \033[0m" >>time.log
#Concurrency test
let i=1
while [ $i -le $1 ];
do
 a=1
 cat $2 | while read line
 do
 echo $line > /root/wang/yqg.sql;
 impala-shell  -i 172.31.0.40:25003 -B  -f /root/wang/yqg.sql -o log/${i}_${a}.out &
 let a=a+1
 done
 let i=i+1
done
wait
