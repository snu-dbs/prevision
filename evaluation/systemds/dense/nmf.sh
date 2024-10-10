#!/bin/bash

nrow=$1
iter=$2
inputX=$3
inputW=$4
inputH=$5
outputW=$6
outputH=$7

if [ $# -ne 7 ]; then
	echo "Usage: $0 [rows] [iteration] [input X] [input W] [input H] [output W] [output H]"
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
	opType=nmf nrow=$nrow iter=$iter \
	inputX_NMF=$inputX inputW_NMF=$inputW inputH=$inputH outputW=$outputW outputH=$outputH \
	inputX_LR=dump inputY=dump inputW_LR=dump outputB=dump

rm dump