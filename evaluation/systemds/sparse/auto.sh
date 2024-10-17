num_of_iter=8
DATADIR="../../../slab-benchmark/prevision/output/sysds"

run_sparse_lr() {
        dataset=$1
        noi=$2
        p=$3
	dmem=$4
	emem=$5
        nrows=400000000
        bin_tall="$DATADIR""/400000000x100_sparse_""$dataset"
        bin_lr_y="$DATADIR""/400000000x1_sparse_""$dataset"
        bin_lr_w="$DATADIR""/100x1_sparse_""$dataset"

	echo "==============================================="
        echo "Running Sparse Linear Regression (LR)"
        echo "Dataset: 40000000 rows | Sparsity: $dataset | Iterations: $noi"
        echo "==============================================="
	for i in $(seq 1 $num_of_iter); do
		echo ">> Run $i out of $num_of_iter"
                bash lr.sh $nrows $noi $bin_tall $bin_lr_y $bin_lr_w output/res $p $dmem $emem
              
		rm -rf output/*
        done;
}


run_pagerank() {
        dataset=$1
        nrows=$2
        noi=$3

        echo "==============================================="
        echo "Running PageRank"
        echo "Dataset: $dataset | Iterations: $noi"
        echo "==============================================="
	for i in $(seq 1 $num_of_iter); do
                echo ">> Run $i out of $num_of_iter"
		bash pagerank.sh $nrows $noi $DATADIR/$dataset output/res_pr 1

                rm -rf output/*
        done;
}

#run_sparse_lr 0.0125 3 1 1 26
#run_sparse_lr 0.025 3 1 7 20
#run_sparse_lr 0.05 3 1 1 26
#run_sparse_lr 0.1 3 1 1 26

#run_pagerank "enron" 36692 3
#run_pagerank "epinions" 75888 3
#run_pagerank "livejournal" 4847571 3
#run_pagerank "twitter" 61578415 3

iterlist=(1 2 4 8 16 32)
for noi in ${iterlist[@]}; do
	echo "==============================================="
        echo "Running PageRank with a Varying Number of Iterations"
        echo "Number of Iterations: $noi"
        run_pagerank "twitter" 61578415 $noi
done;

plist=(2 4 8)
for p in ${plist[@]}; do
        echo "==============================================="
        echo "Running Sparse LR with a Varying Degree of Parallelism"
        echo "Number of Parallelism: $p"
	run_sparse_lr 0.0125 3 $p 1 26
done;
