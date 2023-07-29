function run() {
        alg=$1
        nrow=$2
        density=$3
        dataset=$4
        noi=$5

        echo $alg $nrow $density $dataset $noi

        spark-submit --conf spark.executor.memory=30g --conf spark.driver.memory=30g --conf spark.driver.maxResultSize=0 --class SparkMLAlgorithms --driver-cores 1 ./target/scala-2.12/MLLibAlgs-assembly-0.1.jar opType=$alg mattype=tall nrow=$nrow ncol=100 density=$density dataset=$dataset noi=$noi fixedAxis=100 step=10 nproc=1
}

sbt clean;
sbt assembly;

run gnmf "10000000" "0" "_" 3
run gnmf "20000000" "0" "_" 3
run gnmf "40000000" "0" "_" 3
run gnmf "80000000" "0" "_" 3

run logit "10000000" "0" "_" 3
run logit "20000000" "0" "_" 3
run logit "40000000" "0" "_" 3
run logit "80000000" "0" "_" 3

run slogit "10000000" "0.0125" "_" 3
run slogit "10000000" "0.025" "_" 3
run slogit "10000000" "0.05" "_" 3
run slogit "10000000" "0.1" "_" 3

run pagerank "36692" "0" "enron" 3
run pagerank "75888" "0" "epinions" 3
run pagerank "4847571" "0" "livejournal" 3
run pagerank "61578415" "0" "twitter" 3