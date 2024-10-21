# /bin/bash
iter=1

DATADIR=../../slab-benchmark/prevision/output/prevision

function dense_nmf() {
        DATASET=$1
        noi=$2
        MAT=$DATADIR"/"$DATASET"x100_dense"
        W=$DATADIR"/"$DATASET"x10_dense"
        H=$DATADIR"/regular/10x100_dense"

        echo "dense_nmf: ./exec_eval NMF $MAT $W $H $noi"
        for i in $(seq 1 $iter);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*
                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval NMF $MAT $W $H $noi
        done
}

function dense_lr() {
        DATASET=$1
        noi=$2
        MAT=$DATADIR"/"$DATASET"x100_dense"
        y=$DATADIR"/"$DATASET"x1_dense"
        w=$DATADIR"/regular/100x1_dense"

        echo "dense_lr: ./exec_eval LR $MAT $y $w $noi"
        for i in $(seq 1 $iter);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*
                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval LR $MAT $y $w $noi
        done
}

function sparse_lr() {
        DATASET=$1
        noi=$2
        MAT=$DATADIR"/regular/400000000x100_sparse_"$DATASET
        y=$DATADIR"/regular/400000000x1_sparse_"$DATASET
        w=$DATADIR"/regular/100x1_sparse_"$DATASET

        echo "dense_lr: ./exec_eval LR $MAT $y $w $noi"
        for i in $(seq 1 $iter);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*
                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval LR $MAT $y $w $noi
        done
}

function pagerank() {
        DATASET=$1
        noi=$2
        MAT=$DATADIR"/regular/"$DATASET
        VEC=$DATADIR"/regular/"$DATASET"_v"

        echo "pagerank: ./exec_eval PAGERANK $MAT $VEC $noi"
        for i in $(seq 1 $iter);
        do
                echo "iter=$i"
                rm -rf __*.tilestore
                sudo rm -rf /dev/shm/buffertile*
                sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"
                sleep 1

                ./exec_eval PAGERANK $MAT $VEC $noi
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

# evalaution                                                                                                                                                                              │·············
set_thread 1                                                                                                                                                                              │·············
set_env                                                                                                                                                                                   │·············

# Blocking with PE
dense_lr "regular/80000000" 3                                                                                                                                                            │·············
dense_nmf "regular/80000000" 3                                                                                                                                                           │·············

# Blocking without PE
export BF_PREEMPTIVE_EVICTION=0
dense_lr "regular/80000000" 3
dense_nmf "regular/80000000" 3