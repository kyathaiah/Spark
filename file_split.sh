#!/bin/sh

start_val=10000
end_val=10000

echo "Start value : "$start_val
echo "End value : "$end_val
while true
do
echo "Hanuman"
date
head -${start_val} apache-access-log.txt | tail -${end_val} > output.txt
hadoop fs -put -f output.txt /dev/solenis/sol_d_test/str_test
start_val=`expr $start_val + ${end_val}`
echo "Start value : "$start_val
sleep 30
done
