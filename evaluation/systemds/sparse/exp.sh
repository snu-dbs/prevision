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
run_sparse_lr() {
        dataset=$1
	dmem=$2
	emem=$3
        nrows=400000000
        bin_tall="$DATADIR""/400000000x100_sparse_""$dataset"
        bin_lr_y="$DATADIR""/400000000x1_sparse_""$dataset"
        bin_lr_w="$DATADIR""/100x1_sparse_""$dataset"

	echo "==============================================="
        echo "Running Sparse Linear Regression (LR)"
        echo "Dataset: 40000000 rows | Sparsity: $dataset | Iterations: $noi"
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
                bash lr.sh $nrows $noi "__TEMP_X" "__TEMP_y" "__TEMP_w" output/res $p $dmem $emem 2>&1 | tee -a /tmp/exp_result.log
              
		rm -rf __*
		rm -rf output/*
        done;
}


run_pagerank() {
        dataset=$DATADIR/$1
        nrows=$2

        echo "==============================================="
        echo "Running PageRank"
        echo "Dataset: $dataset | Iterations: $noi"
        echo "==============================================="
	for i in $(seq 1 $repetition); do
                echo ">> Run $i out of $repetition"
		cp -r $dataset "__TEMP_X"
		cp -r "${dataset}.mtd" "__TEMP_X.mtd"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		bash pagerank.sh $nrows $noi "__TEMP_X" output/res_pr 1 2>&1 | tee -a /tmp/exp_result.log

		rm -rf __*
                rm -rf output/*
        done;
}

# run task
if [[ $task == "slr" ]]; then
	if [[ $data == "0.0125" ]]; then
		run_sparse_lr 0.0125 1 26
	elif [[ $data == "0.025" ]]; then
		run_sparse_lr 0.025 7 20
	elif [[ $data == "0.05" ]]; then
		run_sparse_lr 0.05 1 26
	elif [[ $data == "0.1" ]]; then
		run_sparse_lr 0.1 1 26
	fi
elif [[ $task == "pagerank" ]]; then
	if [[ $data == "enron" ]]; then
		run_pagerank "enron" 36692 3
	elif [[ $data == "epinions" ]]; then
		run_pagerank "epinions" 75888 3
	elif [[ $data == "livejournal" ]]; then
		run_pagerank "livejournal" 4847571 3
	elif [[ $data == "twitter" ]]; then
		run_pagerank "twitter" 61578415 3
	fi
fi

# collect result
gawk '{if (match($0, /^Total elapsed time: *([0-9]*\.?[0-9]+) sec\./, arr)) {print arr[1]}}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/time-systemds-"$task"-"$data"-"$noi"-"$p".log" 
