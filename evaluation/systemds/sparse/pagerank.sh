#!/bin/bash

nrow=$1
iter=$2
inputM=$3
output=$4
num_thread=$5
dmem=${6:-}
emem=${7:-}

if [ $# -lt 5 ]; then
	echo "Usage: $0 [rows] [iteration] [input] [output] [parallelism] [driver_memory (optional)] [executor_memory (optional)]"
	exit
fi

if [ -n "$dmem" ] && [ -z "$emem" ] || [ -z "$dmem" ] && [ -n "$emem" ]; then
        echo "Error: You must set both driver and executor memory, or neither."
        exit
fi

set_memory() {
        if [ -z "$dmem" ] && [ -z "$emem" ]; then
                if [ "$nrow" -eq 36692 ]; then #enron
                        dmem=7
                        emem=20
                elif [ "$nrow" -eq 75888 ]; then #epinions
                        dmem=20
                        emem=7
                elif [ "$nrow" -eq 4847571 ]; then #livejournal
                        dmem=14
                        emem=13
		elif [ "$nrow" -eq 61578415 ]; then #twitter
			dmem=14
			emem=13
			echo "Warning: Default memory configuration might not be optimal for the 'Twitter' dataset since previous experiments encountered out-of-memory errors. Please set it manually if necessary."
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
	opType=pr nrow=$nrow iter=$iter \
	inputM=$inputM output=$output \
	inputX=dump inputY=dump inputW=dump \

$SPARK_ROOT/sbin/stop-worker.sh
$SPARK_ROOT/sbin/stop-master.sh

rm dump
