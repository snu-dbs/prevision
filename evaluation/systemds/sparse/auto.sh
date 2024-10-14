nuum_of_iter=8
DATADIR="../../../slab-benchmark/prevision/output/sysds"

run_sparse_lr() {
        dataset=$1
        noi=$2
        p=$3
        nrows=400000000

        bin_tall="$DATADIR""/400000000x100_sparse_""$dataset"
        bin_lr_y="$DATADIR""/400000000x1_sparse_""$dataset"
        bin_lr_w="$DATADIR""/100x1_sparse_""$dataset"

        echo "dataset=""$dataset"
        echo 'Sparse LR ' $noi
        for i in $(seq 1 $num_of_iter); do
                echo 'started'
                bash lr.sh $nrows $noi $bin_tall $bin_lr_y $bin_lr_w output/res $p
                rm -rf output/*
        done;
}


run_pagerank() {
        dataset=$1
        nrows=$2
        noi=$3

        echo "dataset=""$dataset"
        echo 'PageRank ' $noi
        for i in $(seq 1 $num_of_iter); do
                echo 'started'
                bash pagerank.sh $nrows $noi $DATADIR/$dataset output/res_pr 1
                rm -rf output/*
        done;
}

run_sparse_lr 0.0125 3 1
run_sparse_lr 0.025 3 1
run_sparse_lr 0.05 3 1
run_sparse_lr 0.1 3 1

run_pagerank "enron" 36692 3
run_pagerank "epinions" 75888 3
run_pagerank "livejournal" 4847571 3
run_pagerank "twitter" 61578415 3

iterlist=(1 2 4 8 16 32)
for noi in ${iterlist[@]}; do
        echo "num_of_iter=$noi";
        run_pagerank "twitter" 1 $noi
done;

plist=(2 4 8)
for p in ${plist[@]}; do
        echo "parallelism=$p";
        run_sparse_lr 0.0125 3 $p
done;