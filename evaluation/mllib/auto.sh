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
                nproc=1
        
        rm -rf __*

        $SPARK_ROOT/sbin/stop-worker.sh
        $SPARK_ROOT/sbin/stop-master.sh
}

sbt clean;
sbt assembly;

# NMF
run gnmf "10000000" "0" "_" 3 7200 19800 1
run gnmf "20000000" "0" "_" 3 7200 19800 1
run gnmf "40000000" "0" "_" 3 7200 19800 1
run gnmf "80000000" "0" "_" 3 7200 19800 1

# LR
run logit "10000000" "0" "_" 3 900 26100 1
run logit "20000000" "0" "_" 3 900 26100 1
run logit "40000000" "0" "_" 3 900 26100 1
run logit "80000000" "0" "_" 3 900 26100 1

# Sparse LR
run slogit "400000000" "0.0125" "_" 3 7200 19800 1
run slogit "400000000" "0.025" "_" 3 7200 19800 1
run slogit "400000000" "0.05" "_" 3 7200 19800 1
run slogit "400000000" "0.1" "_" 3 7200 19800 1

# PageRank
run pagerank "36692" "0" "enron" 3 900 26100 1
run pagerank "75888" "0" "epinions" 3 900 26100 1
run pagerank "4847571" "0" "livejournal" 3 900 26100 1
run pagerank "61578415" "0" "twitter" 3 900 26100 1

# num iter 
iterlist=(1 2 4 8 16 32)
for noi in ${iterlist[@]}; do
        echo "==============================================="
	echo "Running NMF with a Varying Number of Iterations"
	echo "Number of Iterations: $noi"
        echo "==============================================="
        run gnmf "10000000" "0" "_" $noi 7200 19800 1

	echo "==============================================="
        echo "Running PageRank with a Varying Number of Iterations"
        echo "Number of Iterations: $noi"
	echo "==============================================="
        run pagerank "61578415" "0" "twitter" $noi 900 26100 1
done;


# parallelism
plist=(2 4 8)
for p in ${plist[@]}; do
	echo "==============================================="
        echo "Running NMF with a Varying Degree of Parallelism"
	echo "Number of Parallelism: $p"
        echo "==============================================="
        run gnmf "10000000" "0" "_" 3 7200 19800 $p

        echo "==============================================="
        echo "Running Sparse LR with a Varying Degree of Parallelism"
        echo "Number of Parallelism: $p"
        echo "==============================================="
        run slogit "400000000" "0.0125" "_" 3 7200 19800 $p
done;
