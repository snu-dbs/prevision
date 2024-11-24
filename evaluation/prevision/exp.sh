#!/bin/bash

# input arguments
task=$1
data=$2
iter=$3
p=$4
repetition=$5
replacement=$6
pe=$7
execmethod=$8


# static
DATADIR="/data/previsions/lab-benchmark/prevision/output/prevision"

# functions
function dense_nmf() {
        DATASET=$1
        MAT=$DATADIR"/"$DATASET"x100_dense"
        W=$DATADIR"/"$DATASET"x10_dense"
        H=$DATADIR"/regular/10x100_dense"

        echo "dense_nmf: ./exec_eval NMF $MAT $W $H $iter"
        for i in $(seq 1 $repetition);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*

		cp -r $MAT".tilestore" "__TEMP_X.tilestore"
		cp -r $W".tilestore" "__TEMP_W.tilestore"
		cp -r $H".tilestore" "__TEMP_H.tilestore"

                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval NMF "__TEMP_X" "__TEMP_W" "__TEMP_H" $iter 2>&1 | tee -a /tmp/exp_result.log
        done
}

function dense_lr() {
        DATASET=$1
        MAT=$DATADIR"/"$DATASET"x100_dense"
        y=$DATADIR"/"$DATASET"x1_dense"
        w=$DATADIR"/regular/100x1_dense"

        echo "dense_lr: ./exec_eval LR $MAT $y $w $iter"
        for i in $(seq 1 $repetition);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*

		cp -r $MAT".tilestore" "__TEMP_X.tilestore"
		cp -r $y".tilestore" "__TEMP_y.tilestore"
		cp -r $w".tilestore" "__TEMP_w.tilestore"

                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval LR "__TEMP_X" "__TEMP_y" "__TEMP_w" $iter 2>&1 | tee -a /tmp/exp_result.log
        done
}

function sparse_lr() {
        DATASET=$1
        MAT=$DATADIR"/regular/400000000x100_sparse_"$DATASET
        y=$DATADIR"/regular/400000000x1_sparse_"$DATASET
        w=$DATADIR"/regular/100x1_sparse_"$DATASET

        echo "dense_lr: ./exec_eval LR $MAT $y $w $iter"
        for i in $(seq 1 $repetition);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*

		cp -r $MAT".tilestore" "__TEMP_X.tilestore"
		cp -r $y".tilestore" "__TEMP_y.tilestore"
		cp -r $w".tilestore" "__TEMP_w.tilestore"

                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval LR "__TEMP_X" "__TEMP_y" "__TEMP_w" $iter 2>&1 | tee -a /tmp/exp_result.log
        done
}

function pagerank() {
        DATASET=$1
        MAT=$DATADIR"/regular/"$DATASET
        VEC=$DATADIR"/regular/"$DATASET"_v"

        echo "pagerank: ./exec_eval PAGERANK $MAT $VEC $iter"
        for i in $(seq 1 $repetition);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*

		cp -r $MAT".tilestore" "__TEMP_X.tilestore"
		cp -r $VEC".tilestore" "__TEMP_v.tilestore"

                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval PAGERANK "__TEMP_X" "__TEMP_v" $iter 2>&1 | tee -a /tmp/exp_result.log
        done
}

function set_thread() {
        export OMP_NUM_THREADS=$1
        export OPENBLAS_NUM_THREADS=$1
        export MKL_NUM_THREADS=$1
        export VECLIB_MAXIMUM_THREADS=$1
        export NUMEXPR_NUM_THREADS=$1
        export __PREVISION_NUM_THREADS=$1
}

function set_env() {
        echo "OPT"
        export BF_DATA_SIZE=30000000000
        export BF_IDATA_SIZE=0
        export BF_KEYSTORE_SIZE=134217728
        export BF_BFSTORE_SIZE=134217728

        export BF_EVICTION_POLICY=8
        export BF_LRUK_K=0
        export BF_LRUK_CRP=0
        export BF_PREEMPTIVE_EVICTION=1
}

function set_bf_env_mru() {
        echo "MRU"
        export BF_DATA_SIZE=30000000000
        export BF_IDATA_SIZE=0
        export BF_KEYSTORE_SIZE=134217728
        export BF_BFSTORE_SIZE=134217728

        export BF_EVICTION_POLICY=1
        export BF_LRUK_K=0
        export BF_LRUK_CRP=0
        export BF_PREEMPTIVE_EVICTION=1
}

function set_bf_env_lruk() {
        echo "LRU-K" $1 $2
        export BF_DATA_SIZE=30000000000
        export BF_IDATA_SIZE=0
        export BF_KEYSTORE_SIZE=134217728
        export BF_BFSTORE_SIZE=134217728

        export BF_EVICTION_POLICY=9
        export BF_LRUK_K=$1
        export BF_LRUK_CRP=$2
        export BF_PREEMPTIVE_EVICTION=1
}

# run task
set_thread $p

# Set buffer replacement algorithm
if [[ $replacement == "mru" ]]; then
	set_bf_env_mru
elif [[ $varient == "lruk" ]]; then
	if [[ $task == "lr" ]]; then
		set_bf_env_lruk 2 8
	elif [[ $task == "nmf" ]]; then
		set_bf_env_lruk 2 64
	fi
else
	set_env
fi

# set peemptive eviction (default = 1)
if [[ $pe == "wo_pe" ]]; then
	export BF_PREEMPTIVE_EVICTION=0
fi

# set execution method
if [[ $execmethod == "blocking" ]]; then
	# replace
	mv exec_eval_blocking exec_eval
fi

# run task
if [[ $task == "lr" ]]; then
	if [[ $data == "10m" ]]; then
		dense_lr "regular/10000000"
	elif [[ $data == "20m" ]]; then
		dense_lr "regular/20000000"
	elif [[ $data == "40m" ]]; then
		dense_lr "regular/40000000"
	elif [[ $data == "80m" ]]; then
		dense_lr "regular/80000000"
	elif [[ $data == "80m_200x1" ]]; then
		dense_lr "small/200/80000000"
	elif [[ $data == "80m_400x1" ]]; then
		dense_lr "small/400/80000000"
	elif [[ $data == "80m_800x1" ]]; then
		dense_lr "small/800/80000000"
	elif [[ $data == "80m_1600x1" ]]; then
		dense_lr "small/1600/80000000"
	elif [[ $data == "80m_3200x1" ]]; then
		dense_lr "small/3200/80000000"
	fi
elif [[ $task == "nmf" ]]; then
	if [[ $data == "10m" ]]; then
		dense_nmf "regular/10000000"
	elif [[ $data == "20m" ]]; then
		dense_nmf "regular/20000000"
	elif [[ $data == "40m" ]]; then
		dense_nmf "regular/40000000"
	elif [[ $data == "80m" ]]; then
		dense_nmf "regular/80000000"
	elif [[ $data == "80m_200x1" ]]; then
		dense_nmf "small/200/80000000"
	elif [[ $data == "80m_400x1" ]]; then
		dense_nmf "small/400/80000000"
	elif [[ $data == "80m_800x1" ]]; then
		dense_nmf "small/800/80000000"
	elif [[ $data == "80m_1600x1" ]]; then
		dense_nmf "small/1600/80000000"
	elif [[ $data == "80m_3200x1" ]]; then
		dense_nmf "small/3200/80000000"
	fi
elif [[ $task == "slr" ]]; then
	if [[ $data == "0.0125" ]]; then
		sparse_lr 0.0125
	elif [[ $data == "0.025" ]]; then
		sparse_lr 0.025
	elif [[ $data == "0.05" ]]; then
		sparse_lr 0.05
	elif [[ $data == "0.1" ]]; then
		sparse_lr 0.1
	fi
elif [[ $task == "pagerank" ]]; then
	if [[ $data == "enron" ]]; then
		pagerank enron
	elif [[ $data == "epinions" ]]; then
		pagerank epinions
	elif [[ $data == "livejournal" ]]; then
		pagerank livejournal
	elif [[ $data == "twitter" ]]; then
		pagerank twitter
	fi
fi

# collect result
# get time
gawk -F "\t" '/^total\tbf/ {getline; print $1}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/time-prevision-"$task"-"$data"-"$iter"-"$p"-"$replacement"-"$pe"-"$execmethod".log" 

# get I/O
gawk -F "\t" '/^total\tbf/ {getline; getline; readio = $3 + $4; writeio = $5; print readio "," writeio}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/io-prevision-"$task"-"$data"-"$iter"-"$p"-"$replacement"-"$pe"-"$execmethod".log" 

# get breakdown
gawk -F "\t" '/^total\tbf/ {getline; io = $3 + $4 + $5; plan = $8 + $12; list = $9 + $10; cpu = $1 - io - plan - list; print cpu "," io "," plan "," list}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/breakdown-prevision-"$task"-"$data"-"$iter"-"$p"-"$replacement"-"$pe"-"$execmethod".log" 

# get hitratio
gawk -F "\t" '/^total\tbf/ {getline; print $6 / $7}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/breakdown-prevision-"$task"-"$data"-"$iter"-"$p"-"$replacement"-"$pe"-"$execmethod".log" 
