#!/bin/bash

nrow=$1
iter=$2
inputX=$3
inputY=$4
inputW=$5
outputB=$6
num_thread=$7
dmem=${8:-}
emem=${9:-}

if [ $# -lt 7 ]; then
	echo "Usage: $0 [rows] [iteration] [input X] [input Y] [input W] [output] [parallelism] [driver_memory (optional)] [executor_memory (optional)]"
	exit
fi

if [ -n "$dmem" ] && [ -z "$emem" ] || [ -z "$dmem" ] && [ -n "$emem" ]; then
	echo "Error: You must set both driver and executor memory, or neither."
	exit
fi

set_memory() {
	if [ -z "$dmem" ] && [ -z "$emem" ]; then
		if [ "$nrow" -eq 10000000 ] || [ "$nrow" -eq 20000000 ]; then
			dmem=26
			emem=1
		elif [ "$nrow" -eq 40000000 ]; then
			dmem=7
			emem=20
		elif [ "$nrow" -eq 80000000 ]; then
			dmem=14
			emem=13
		else
			echo -e "Error: No default memory configuration for the dataset size provided.\nPlease set both driver and executor memory manually."
			exit
		fi
	fi
}

set_memory
echo "Client Mode Configuration"
echo "Driver Memory: ${dmem}GB | Executor Memory: ${emem}GB"
echo "==============================================="

source bashenv
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# create dump file (we don't need files for NMF)
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
	inputX_LR=$inputX inputY=$inputY inputW_LR=$inputW outputB=$outputB \
	inputX_NMF=dump inputW_NMF=dump inputH=dump outputW=dump outputH=dump

$SPARK_ROOT/sbin/stop-worker.sh
$SPARK_ROOT/sbin/stop-master.sh

rm dump
