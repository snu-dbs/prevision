#!/bin/bash

# input arguments
task=$1
data=$2
noi=$3
p=$4
repetition=$5

# static
DATADIR="/data/prevision/slab-benchmark/prevision/output/sysds"

# functions
run_lr() {
        dataset=$1
        bin_tall="$DATADIR"/"$dataset""x100_dense"
        bin_lr_y="$DATADIR"/"$dataset""x1_dense"
        bin_lr_w="$DATADIR"/"100x1_dense"
	
	echo "==============================================="
	echo "Running Linear Regression (LR)"
	echo "Dataset: $dataset rows | Iterations: $noi"
	echo "==============================================="
	for i in $(seq 1 $repetition); do
		echo ">> Run $i out of $repetition"

		cp -r $bin_tall "__TEMP_X"
		cp -r $bin_lr_y "__TEMP_y"
		cp -r $bin_lr_w "__TEMP_w"
		cp -r "${bin_tall}.mtd" "__TEMP_X.mtd"
		cp -r "${bin_lr_y}.mtd" "__TEMP_y.mtd"
		cp -r "${bin_lr_w}.mtd" "__TEMP_w.mtd"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		bash lr.sh $dataset $noi "__TEMP_X" "__TEMP_y" "__TEMP_w" output/res $p 2>&1 | tee -a /tmp/exp_result.log
                
		rm -rf __*
		rm -rf output/*
        done;
}

run_nmf() {
        dataset=$1
        bin_tall="$DATADIR"/"$dataset""x100_dense"
        bin_nmf_w="$DATADIR"/"$dataset""x10_dense"
        bin_nmf_h="$DATADIR"/"10x100_dense"

	echo "==============================================="
        echo "Running Non-negative Matrix Factorization (NMF)"
	echo "Dataset: $dataset rows | Iterations: $noi"
        echo "==============================================="
	for i in $(seq 1 $repetition); do
                echo ">> Run $i out of $repetition"

		cp -r $bin_tall "__TEMP_X"
		cp -r $bin_nmf_w "__TEMP_W"
		cp -r $bin_nmf_h "__TEMP_H"
		cp -r "${bin_tall}.mtd" "__TEMP_X.mtd"
		cp -r "${bin_nmf_w}.mtd" "__TEMP_W.mtd"
		cp -r "${bin_nmf_h}.mtd" "__TEMP_H.mtd"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		bash nmf.sh $dataset $noi "__TEMP_X" "__TEMP_W" "__TEMP_H" output/res_w output/res_h $p 2>&1 | tee -a /tmp/exp_result.log

		rm -rf __*
                rm -rf output/*
        done;
}

# _func and _dataset will be set
if [[ $task == "lr" ]]; then
  _func="run_lr"
elif [[ $task == "nmf" ]]; then
  _func="run_nmf"
fi

if [[ $data == "10m" ]]; then
  _dataset=10000000 
elif [[ $data == "20m" ]]; then
  _dataset=20000000 
elif [[ $data == "40m" ]]; then
  _dataset=40000000 
elif [[ $data == "80m" ]]; then
  _dataset=80000000 
fi

# run
eval $_func $_dataset

# collect result
gawk '{if (match($0, /^Total elapsed time:\t*([0-9]*\.?[0-9]+) sec\./, arr)) {print arr[1]}}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/time-systemds-"$task"-"$data"-"$noi"-"$p".log" 
