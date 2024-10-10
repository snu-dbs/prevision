source bashenv

num_of_iter=8
DATADIR="../../../slab-benchmark/prevision/output/sysds"

run_lr() {
        dataset=$1
        noi=$2
        bin_tall="$DATADIR"/"$dataset""x100_dense"
        bin_lr_y="$DATADIR"/"$dataset""x1_dense"
        bin_lr_w="$DATADIR"/"100x1_dense"

        echo "dataset=""$dataset"
        echo 'LR ' $noi
        for i in $(seq 1 $num_of_iter); do
                sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                sleep 2
                echo 'started'
                bash lr.sh $dataset $noi $bin_tall $bin_lr_y $bin_lr_w output/res
                rm -rf output/*
        done;
}

run_nmf() {
        dataset=$1
        noi=$2
        bin_tall="$DATADIR"/"$dataset""x100_dense"
        bin_nmf_w="$DATADIR"/"$dataset""x10_dense"
        bin_nmf_h="$DATADIR"/"10x100_dense"

        echo "dataset=""$dataset"
        echo 'NMF ' $noi
        for i in $(seq 1 $num_of_iter); do
                sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                sleep 2
                echo 'started'
                bash nmf.sh $dataset $noi $bin_tall $bin_nmf_w $bin_nmf_h output/res_w output/res_h
                rm -rf output/*
        done;
}

run_lr 10000000 3
run_lr 20000000 3
run_lr 40000000 3
run_lr 80000000 3

run_nmf 10000000 3
run_nmf 20000000 3
run_nmf 40000000 3
run_nmf 80000000 3

