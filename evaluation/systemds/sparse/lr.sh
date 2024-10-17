#!/bin/bash

nrow=$1
iter=$2
inputX=$3
inputY=$4
inputW=$5
output=$6
num_thread=$7
dmem=$8
emem=$9

if [ $# -ne 9 ]; then
	echo "Usage: $0 [rows] [iteration] [input X] [input Y] [input W] [output] [parallelism] [driver_memory] [executor_memory]"
	exit
fi

echo "Client Mode Configuration"
echo "Driver Memory: ${dmem}GB | Executor Memory: ${emem}GB"
echo "==============================================="

source bashenv
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# create dump file (we don't need files for PageRank)
touch dump

# Client Mode
$SPARK_ROOT/sbin/start-master.sh -h 127.0.0.1; sleep 3;
$SPARK_ROOT/sbin/start-worker.sh -c $num_thread spark://127.0.0.1:7077; sleep 3;

spark-submit --master spark://127.0.0.1:7077 \
        --conf spark.driver.memory="${dmem}g" \
        --conf spark.executor.memory="${emem}g" \
	--conf spark.driver.maxResultSize=0 \
	--class SystemDSMLAlgorithms target/scala-2.12/SystemDSAlgs-assembly-0.1.jar \
	opType=lr nrow=$nrow iter=$iter \
	inputX=$inputX inputY=$inputY inputW=$inputW output=$output \
	inputM=dump

$SPARK_ROOT/sbin/stop-worker.sh
$SPARK_ROOT/sbin/stop-master.sh

rm dump
