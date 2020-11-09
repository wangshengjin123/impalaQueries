#!/bin/sh
time1=`date`
echo -e "\e[033m $time1 \033[0m" >>time.log
#Concurrency test
let i=1
while [ $i -le $1 ];
do
 impala-shell -B  -f $2 -o log/${i}.out &
 let i=i+1
done
wait
