#!/bin/bash

nrow=$1
iter=$2
inputX=$3
inputY=$4
inputW=$5
outputB=$6

if [ $# -ne 6 ]; then
	echo "Usage: $0 [rows] [iteration] [input X] [input Y] [input W] [output]"
	exit
fi

source bashenv
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# create dump file (we don't need files for NMF)
touch dump

spark-submit --master local \
	--conf spark.driver.memory=30g --conf spark.executor.memory=30g \
	--conf spark.driver.maxResultSize=0 \
	--class SystemDSMLAlgorithms target/scala-2.12/SystemDSAlgs-assembly-0.1.jar \
	opType=lr nrow=$nrow iter=$iter \
	inputX=$inputX inputY=$inputY inputW=$inputW outputB=$outputB \
	inputH=dump outputW=dump outputH=dump

rm dump