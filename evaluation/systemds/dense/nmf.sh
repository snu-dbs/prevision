#!/bin/bash

nrow=$1
iter=$2
inputX=$3
inputW=$4
inputH=$5
outputW=$6
outputH=$7
num_thread=$8
dmem=${9:-}
emem=${10:-}

if [ $# -lt 8 ]; then
	echo "Usage: $0 [rows] [iteration] [input X] [input W] [input H] [output W] [output H] [parallelism] [driver_memory (optional)] [executor_memory (optional)]"
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
                elif [ "$nrow" -eq 40000000 ] || [ "$nrow" -eq 80000000 ]; then
                        dmem=20
                        emem=7
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

# create dump file (we don't need files for LR)
touch dump

# Client Mode
$SPARK_ROOT/sbin/start-master.sh -h 127.0.0.1; sleep 3;
$SPARK_ROOT/sbin/start-worker.sh -c $num_thread spark://127.0.0.1:7077; sleep 3;

spark-submit --master spark://127.0.0.1:7077 \
        --conf spark.driver.memory="${dmem}g" \
        --conf spark.executor.memory="${emem}g" \
	--conf spark.driver.maxResultSize=0 \
	--class SystemDSMLAlgorithms target/scala-2.12/SystemDSAlgs-assembly-0.1.jar \
	opType=nmf nrow=$nrow iter=$iter \
	inputX_NMF=$inputX inputW_NMF=$inputW inputH=$inputH outputW=$outputW outputH=$outputH \
	inputX_LR=dump inputY=dump inputW_LR=dump outputB=dump

$SPARK_ROOT/sbin/stop-worker.sh
$SPARK_ROOT/sbin/stop-master.sh

rm dump
