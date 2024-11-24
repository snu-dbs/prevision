#!/bin/bash

# input arguments
task=$1
data=$2
iter=$3
p=$4
repetition=$5

# static
DATADIR="/prevision/slab-benchmark/prevision/output/hdf5/"

# functions
run_lr() {
	dataset=$1
	noi=$2
	npy_tall="$DATADIR""$dataset""x100_dense.hdf5"
	npy_lr_y="$DATADIR""$dataset""x1_dense.hdf5"
	npy_lr_w="$DATADIR""regular/100x1_dense.hdf5"

	echo "dataset=""$dataset"
	echo 'LR'
	for i in $(seq 1 $repetition); do
		cp $npy_tall "__TEMP_X.hdf5"
		cp $npy_lr_y "__TEMP_y.hdf5"
		cp $npy_lr_w "__TEMP_w.hdf5"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python eval_dask.py LR "__TEMP_X.hdf5" "__TEMP_y.hdf5" "__TEMP_w.hdf5" $noi 2>&1 | tee -a /tmp/exp_result.log

		rm *.hdf5
	done;

}

run_nmf() {
	dataset=$1
	noi=$2
	npy_tall="$DATADIR""$dataset""x100_dense.hdf5"
	npy_nmf_w="$DATADIR""$dataset""x10_dense.hdf5"
	npy_nmf_h="$DATADIR""regular/10x100_dense.hdf5"

	echo "dataset=""$dataset"
	echo 'NMF'
	for i in $(seq 1 $repetition); do
		cp $npy_tall "__TEMP_X.hdf5"
		cp $npy_nmf_w "__TEMP_W.hdf5"
		cp $npy_nmf_h "__TEMP_H.hdf5"

		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python eval_dask.py NMF "__TEMP_X.hdf5" "__TEMP_W.hdf5" "__TEMP_H.hdf5" $noi 2>&1 | tee -a /tmp/exp_result.log

		rm *.hdf5
	done;

}

# _func and _dataset will be set
if [[ $task == "lr" ]]; then
  _func="run_lr"
elif [[ $task == "nmf" ]]; then
  _func="run_nmf"
fi

if [[ $data == "10m" ]]; then
  _dataset="regular/10000000"
elif [[ $data == "20m" ]]; then
  _dataset="regular/20000000"
elif [[ $data == "40m" ]]; then
  _dataset="regular/40000000" 
elif [[ $data == "80m" ]]; then
  _dataset="regular/80000000" 
elif [[ $data == "80m_200x1" ]]; then
  _dataset="small/200/80000000" 
elif [[ $data == "80m_400x1" ]]; then
  _dataset="small/400/80000000" 
elif [[ $data == "80m_800x1" ]]; then
  _dataset="small/800/80000000" 
elif [[ $data == "80m_1600x1" ]]; then
  _dataset="small/1600/80000000" 
elif [[ $data == "80m_3200x1" ]]; then
  _dataset="small/3200/80000000" 
fi
    
# set parallelism
export _PREVISION_DASK_THREAD=$p

# run
eval $_func $_dataset $iter

# collect result
awk -F "," 'END {print $1}' /tmp/exp_result.log >> "/data/results/time-dask-"$task"-"$data"-"$iter"-"$p".log" 
