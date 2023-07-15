#!/bin/bash

nrow=$1
iter=$2
inputM=$3
output=$4

if [ $# -ne 4 ]; then
	echo "Usage: $0 [rows] [iteration] [input] [output]"
	exit
fi

source bashenv
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# create dump file (we don't need files for LR)
touch dump

spark-submit --master local \
	--conf spark.driver.memory=30g --conf spark.executor.memory=30g \
	--conf spark.driver.maxResultSize=0 \
	--class SystemDSMLAlgorithms target/scala-2.12/SystemDSAlgs-assembly-0.1.jar \
	opType=pr nrow=$nrow iter=$iter \
	inputM=$inputM output=$output \
	inputX=dump inputY=dump inputW=dump \

rm dump