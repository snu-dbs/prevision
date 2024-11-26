#!/bin/bash

# input arguments
task=$1
data=$2
iter=$3
p=$4
repetition=$5

# import and run
if [[ $task == "lr" || $task == "nmf" ]]; then
	# import dataset
	bash ./import-script/dense.sh $task $data

	# run
	cd dense
	bash auto.sh $task $data $iter $p $repetition
elif [[ $task == "slr" || $task == "pagerank" ]]; then
	# import dataset
	bash ./import-script/sparse.sh $task $data

	# run
	cd sparse
	bash auto.sh $task $data $iter $p $repetition
fi
