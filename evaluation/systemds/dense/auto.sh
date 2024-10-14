num_of_iter=8
DATADIR="../../../slab-benchmark/prevision/output/sysds"

run_lr() {
        dataset=$1
        noi=$2
        p=$3
        bin_tall="$DATADIR"/"$dataset""x100_dense"
        bin_lr_y="$DATADIR"/"$dataset""x1_dense"
        bin_lr_w="$DATADIR"/"100x1_dense"

        echo "dataset=""$dataset"
        echo 'LR ' $noi
        for i in $(seq 1 $num_of_iter); do
                echo 'started'
                bash lr.sh $dataset $noi $bin_tall $bin_lr_y $bin_lr_w output/res $p
                rm -rf output/*
        done;
}

run_nmf() {
        dataset=$1
        noi=$2
        p=$3
        bin_tall="$DATADIR"/"$dataset""x100_dense"
        bin_nmf_w="$DATADIR"/"$dataset""x10_dense"
        bin_nmf_h="$DATADIR"/"10x100_dense"

        echo "dataset=""$dataset"
        echo 'NMF ' $noi
        for i in $(seq 1 $num_of_iter); do
                echo 'started'
                bash nmf.sh $dataset $noi $bin_tall $bin_nmf_w $bin_nmf_h output/res_w output/res_h $p
                rm -rf output/*
        done;
}

run_lr 10000000 3 1
run_lr 20000000 3 1
run_lr 40000000 3 1
run_lr 80000000 3 1

run_nmf 10000000 3 1
run_nmf 20000000 3 1
run_nmf 40000000 3 1
run_nmf 80000000 3 1

iterlist=(1 2 4 8 16 32)
for noi in ${iterlist[@]}; do
        echo "num_of_iter=$noi";
        run_nmf 10000000 $noi 1
done;

plist=(2 4 8)
for p in ${plist[@]}; do
        echo "parallelism=$p";
        run_nmf 10000000 3 $p
done;