#!/bin/bash

# input arguments
SYSTEM=$1
TASK=$2
DATA=$3
ITER=$4
PARALLELISM=$5
REPETITION=$6
DATAPATH=$7

# help
if [ $# -ne 7 ]; then
  echo "Usage: $0 [SYSTEM] [TASK] [DATA] [ITER] [PARALLELISM] [REPETITION] [DATAPATH]"
  exit
fi

# print info
echo "/********************************************************************************************/"
echo " * SYSTEM=$SYSTEM TASK=$TASK DATA=$DATA ITER=$ITER PARALLELISM=$PARALLELISM REPETITION=$REPETITION DATAPATH=$DATAPATH"
echo "/********************************************************************************************/"

# function for eacy-to-call
function run() {
    docker run -it \
		--name prevision-ari \
		-m 31g --shm-size=30gb --privileged \
		-v $DATAPATH:/data/prevision/slab-benchmark/prevision/output \
		-v $(pwd)/results:/data/prevision/evaluation/results \
		grammaright/prevision:latest "$@"
    docker rm prevision-ari
}

# run experiment
if [[ $SYSTEM == "numpy" ]]; then
  run sh -c "cd /data/prevision/evaluation/numpy_memmap; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
elif [[ $SYSTEM == "dask" ]]; then
  run sh -c "cd /data/prevision/evaluation/dask; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
elif [[ $SYSTEM == "madlib" ]]; then
  run sh -c "cd /data/prevision/evaluation/madlib; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
elif [[ $SYSTEM == "scidb" ]]; then
  run sh -c "cd /data/prevision/evaluation/scidb; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
elif [[ $SYSTEM == "mllib" ]]; then
  run sh -c "cd /data/prevision/evaluation/mllib; bash auto.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
elif [[ $SYSTEM == "systemds" ]]; then
	if [[ $TASK == "lr" || $TASK == "nmf" ]]; then
		  run sh -c "cd /data/prevision/evaluation/systemds/dense; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
	elif [[ $TASK == "slr" || $TASK == "pagerank" ]]; then
		  run sh -c "cd /data/prevision/evaluation/systemds/sparse; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION"
	fi

elif [[ $SYSTEM == "prevision" ]]; then
  run sh -c "cd /data/prevision/evaluation/prevision; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION opt pe getpos"
elif [[ $SYSTEM == "prevision_mru" ]]; then
  run sh -c "cd /data/prevision/evaluation/prevision; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION mru pe getpos"
elif [[ $SYSTEM == "prevision_lruk" ]]; then
  run sh -c "cd /data/prevision/evaluation/prevision; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION lruk pe getpos"
elif [[ $SYSTEM == "prevision_wo_pe" ]]; then
  run sh -c "cd /data/prevision/evaluation/prevision; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION opt wo_pe getpos"
elif [[ $SYSTEM == "prevision_blocking" ]]; then
  run sh -c "cd /data/prevision/evaluation/prevision; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION opt pe blocking"
elif [[ $SYSTEM == "prevision_blocking_wo_pe" ]]; then
  run sh -c "cd /data/prevision/evaluation/prevision; bash exp.sh $TASK $DATA $ITER $PARALLELISM $REPETITION opt we_po blocking"
fi

