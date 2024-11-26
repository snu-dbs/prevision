#!/bin/bash

# input arguments
task=$1
data=$2
iter=$3
p=$4
repetition=$5

# static
DATADIR="/data/prevision/slab-benchmark/prevision/output/sequencefile"

function run() {
        alg=$1
        nrow=$2
        density=$3
        dataset=$4
        noi=$5
        driver_mem=$6
        executor_mem=$7
        nthread=$8

        echo "###############################################"
        echo "# Algorithm: $alg"
        echo "# Dataset: nrow= ${nrow}, density=${density}, dataset=${dataset}"
        echo "# Iterations: $noi"
        echo "# Driver Memory: ${driver_mem}MB | Executor Memory: ${executor_mem}MB | Num of Threads: ${nthread}"
        echo "###############################################"

        sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
        $SPARK_ROOT/sbin/start-master.sh -h 127.0.0.1; sleep 3;
        $SPARK_ROOT/sbin/start-worker.sh -c $nthread spark://127.0.0.1:7077; sleep 3;

        spark-submit --conf spark.executor.memory=${executor_mem}m \
                --conf spark.driver.memory=${driver_mem}m \
                --conf spark.executor.cores=${nthread} \
                --conf spark.driver.maxResultSize=0 \
                --class SparkMLAlgorithms \
                --master spark://127.0.0.1:7077 \
                ./target/scala-2.12/MLLibAlgs-assembly-0.1.jar \
                opType=$alg \
                mattype=tall \
                nrow=$nrow \
                ncol=100 \
                density=$density \
                dataset=$dataset \
                noi=$noi \
                fixedAxis=100 \
                step=10 \
                nproc=1 2>&1 | tee -a /tmp/exp_result.log
        
        rm -rf *.sf

        $SPARK_ROOT/sbin/stop-worker.sh
        $SPARK_ROOT/sbin/stop-master.sh
}

# run task
for i in $(seq 1 $repetition); do
	if [[ $task == "lr" ]]; then
		if [[ $data == "10m" ]]; then
			cp -r $DATADIR/10000000x100_dense.sf .
			cp -r $DATADIR/10000000x1_dense.sf .
			cp -r $DATADIR/100x1_dense.sf .

			run logit "10000000" "0" "_" $iter 900 26100 $p
		elif [[ $data == "20m" ]]; then
			cp -r $DATADIR/20000000x100_dense.sf .
			cp -r $DATADIR/20000000x1_dense.sf .
			cp -r $DATADIR/100x1_dense.sf .

			run logit "20000000" "0" "_" $iter 900 26100 $p
		elif [[ $data == "40m" ]]; then
			cp -r $DATADIR/40000000x100_dense.sf .
			cp -r $DATADIR/40000000x1_dense.sf .
			cp -r $DATADIR/100x1_dense.sf .

			run logit "40000000" "0" "_" $iter 900 26100 $p
		elif [[ $data == "80m" ]]; then
			cp -r $DATADIR/80000000x100_dense.sf .
			cp -r $DATADIR/80000000x1_dense.sf .
			cp -r $DATADIR/100x1_dense.sf .

			run logit "80000000" "0" "_" $iter 900 26100 $p
		fi
	elif [[ $task == "nmf" ]]; then
		if [[ $data == "10m" ]]; then
			cp -r $DATADIR/10000000x100_dense.sf .
			cp -r $DATADIR/10000000x10_dense.sf .
			cp -r $DATADIR/10x100_dense.sf .

			run gnmf "10000000" "0" "_" $iter 7200 19800 $p
		elif [[ $data == "20m" ]]; then
			cp -r $DATADIR/20000000x100_dense.sf .
			cp -r $DATADIR/20000000x10_dense.sf .
			cp -r $DATADIR/10x100_dense.sf .

			run gnmf "20000000" "0" "_" $iter 7200 19800 $p
		elif [[ $data == "40m" ]]; then
			cp -r $DATADIR/40000000x100_dense.sf .
			cp -r $DATADIR/40000000x10_dense.sf .
			cp -r $DATADIR/10x100_dense.sf .

			run gnmf "40000000" "0" "_" $iter 7200 19800 $p
		elif [[ $data == "80m" ]]; then
			cp -r $DATADIR/80000000x100_dense.sf .
			cp -r $DATADIR/80000000x10_dense.sf .
			cp -r $DATADIR/10x100_dense.sf .

			run gnmf "80000000" "0" "_" $iter 7200 19800 $p
		fi
	elif [[ $task == "slr" ]]; then
		if [[ $data == "0.0125" ]]; then
			cp -r $DATADIR/400000000x100_sparse_0.0125.sf .
			cp -r $DATADIR/400000000x1_sparse_0.0125.sf .
			cp -r $DATADIR/100x1_sparse_0.0125.sf .

			run slogit "400000000" "0.0125" "_" $iter 7200 19800 $p
		elif [[ $data == "0.025" ]]; then
			cp -r $DATADIR/400000000x100_sparse_0.025.sf .
			cp -r $DATADIR/400000000x1_sparse_0.025.sf .
			cp -r $DATADIR/100x1_sparse_0.025.sf .

			run slogit "400000000" "0.025" "_" $iter 7200 19800 $p
		elif [[ $data == "0.05" ]]; then
			cp -r $DATADIR/400000000x100_sparse_0.05.sf .
			cp -r $DATADIR/400000000x1_sparse_0.05.sf .
			cp -r $DATADIR/100x1_sparse_0.05.sf .

			run slogit "400000000" "0.05" "_" $iter 7200 19800 $p
		elif [[ $data == "0.1" ]]; then
			cp -r $DATADIR/400000000x100_sparse_0.1.sf .
			cp -r $DATADIR/400000000x1_sparse_0.1.sf .
			cp -r $DATADIR/100x1_sparse_0.1.sf .

			run slogit "400000000" "0.1" "_" $iter 3600 23400 $p
		fi
	elif [[ $task == "pagerank" ]]; then
		if [[ $data == "enron" ]]; then
			cp -r $DATADIR/enron.sf .
			cp -r $DATADIR/enron_v.sf .

			run pagerank "36692" "0" "enron" $iter 900 26100 $p
		elif [[ $data == "epinions" ]]; then
			cp -r $DATADIR/epinions.sf .
			cp -r $DATADIR/epinions_v.sf .

			run pagerank "75888" "0" "epinions" $iter 900 26100 $p
		elif [[ $data == "livejournal" ]]; then
			cp -r $DATADIR/livejournal.sf .
			cp -r $DATADIR/livejournal_v.sf .

			run pagerank "4847571" "0" "livejournal" $iter 900 26100 $p
		elif [[ $data == "twitter" ]]; then
			cp -r $DATADIR/twitter_20.sf .
			cp -r $DATADIR/twitter_20_v.sf .

			run pagerank2 "61578415" "0" "twitter_20" $iter 900 26100 $p         # no OOM if 20x20 tiles 
		fi
	fi
done;

# collect result
gawk '{if (match($0, /^Elapsed Time \(s\): ([0-9]*\.?[0-9]+)$/, arr)) {print arr[1]}}' /tmp/exp_result.log >> "/data/prevision/evaluation/results/time-mllib-"$task"-"$data"-"$iter"-"$p".log" 
