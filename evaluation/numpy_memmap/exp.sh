#!/bin/bash

# input arguments
task=$1
data=$2
iter=$3
p=$4
repetition=$5

# static
DATADIR="/prevision/slab-benchmark/prevision/output/npy/"

# functions
run_lr() {
	dataset=$1
	noi=$2
	npy_tall="$DATADIR""$dataset""x100_dense.npy"
	npy_lr_y="$DATADIR""$dataset""x1_dense.npy"
	npy_lr_w="$DATADIR""100x1_dense.npy"

	echo "dataset=""$dataset"
	echo 'LR ' $noi
	for i in $(seq 1 $repetition); do
		cp $npy_tall "__TEMP_X.npy"
		cp $npy_lr_y "__TEMP_y.npy"
		cp $npy_lr_w "__TEMP_w.npy"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e, %U, %S' python eval_numpy_memmap.py LR "__TEMP_X.npy" "__TEMP_y.npy" "__TEMP_w.npy" $noi 0.0000001 2>&1 | tee -a /tmp/exp_result.log

		rm __*
	done;
}


run_nmf() {
	dataset=$1
	noi=$2
	npy_tall="$DATADIR""$dataset""x100_dense.npy"
	npy_nmf_w="$DATADIR""$dataset""x10_dense.npy"
	npy_nmf_h="$DATADIR""10x100_dense.npy"

	echo "dataset=""$dataset"
	echo 'NMF ' $noi
	for i in $(seq 1 $repetition); do
		cp $npy_tall "__TEMP_X.npy"
		cp $npy_nmf_w "__TEMP_W.npy"
		cp $npy_nmf_h "__TEMP_H.npy"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e, %U, %S' python eval_numpy_memmap.py NMF "__TEMP_X.npy" "__TEMP_W.npy" "__TEMP_H.npy" $noi 2>&1 | tee -a /tmp/exp_result.log

		rm __*
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
    
# set parallelism
export PARALLELISM=$p

# run
eval $_func $_dataset $iter

# collect result
awk -F "," 'END {print $1}' /tmp/exp_result.log >> "/data/results/time-numpy-"$task"-"$data"-"$iter"-"$p".log" 
