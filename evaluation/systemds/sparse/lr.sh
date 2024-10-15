#!/bin/bash

nrow=$1
iter=$2
inputX=$3
inputY=$4
inputW=$5
output=$6
num_thread=$7
driver_mem=$8
executor_mem=$9

if [ $# -ne 9 ]; then
	echo "Usage: $0 [rows] [iteration] [input X] [input Y] [input W] [output] [parallelism] [driver mem] [executor mem]"
	exit
fi

source bashenv
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# create dump file (we don't need files for PageRank)
touch dump

spark-submit --master local \
	--conf spark.driver.memory="$driver_mem"g --conf spark.executor.memory="$executor_mem"g \
	--conf spark.executor.cores=$num_thread \
	--conf spark.driver.maxResultSize=0 \
	--class SystemDSMLAlgorithms target/scala-2.12/SystemDSAlgs-assembly-0.1.jar \
	opType=lr nrow=$nrow iter=$iter \
	inputX=$inputX inputY=$inputY inputW=$inputW output=$output \
	inputM=dump

rm dump