function set_env() {
        export OMP_NUM_THREADS=1
        export OPENBLAS_NUM_THREADS=1
        export MKL_NUM_THREADS=1
        export VECLIB_MAXIMUM_THREADS=1
        export NUMEXPR_NUM_THREADS=1

        # If you ran into OOM error, then resize it!
        export BF_DATA_SIZE=30000000000
        export BF_IDATA_SIZE=0
        export BF_KEYSTORE_SIZE=134217728
        export BF_BFSTORE_SIZE=134217728

        export BF_EVICTION_POLICY=8
        export BF_PREEMPTIVE_EVICTION=1
        export BF_LRUK_K=0
        export BF_LRUK_CRP=0
}

TMP=$(pwd)
cd ../..
bash makeall.sh
cd $TMP
set_env

# Build converter for PreVision
gcc -std=c11 -pedantic -g -O2 -g3 -o ./generator/conv_csv_to_prevision ./generator/conv_csv_to_prevision.c ../../tilestore/lib/libtilestore.a -lm -I../../tilestore/include

function conv_dense() {
    name=$1
    row=$2
    col=$3
    trow=$4
    tcol=$5

    ./generator/conv_csv_to_prevision ./output/csv/$name.csv ./output/prevision/regular/$name dense $row $col $trow $tcol
}

function conv_dense_st() {
    name=$1
    oname=$2
    row=$3
    col=$4
    trow=$5
    tcol=$6

    ./generator/conv_csv_to_prevision ./output/csv/$name.csv ./output/prevision/small/$oname dense $row $col $trow $tcol
}

function conv_sparse() {
    name=$1
    row=$2
    col=$3
    trow=$4
    tcol=$5

    ./generator/conv_csv_to_prevision ./output/csv/$name.csv ./output/prevision/$name sparse $row $col $trow $tcol
}

# Dense Generation
conv_dense 10000000x100_dense 10000000 100 100000 100
conv_dense 10000000x10_dense 10000000 10 100000 10
conv_dense 10000000x1_dense 10000000 1 100000 1

conv_dense 20000000x100_dense 20000000 100 200000 100 
conv_dense 20000000x10_dense 20000000 10 200000 10
conv_dense 20000000x1_dense 20000000 1 200000 1 

conv_dense 40000000x100_dense 40000000 100 400000 100
conv_dense 40000000x10_dense 40000000 10 400000 10
conv_dense 40000000x1_dense 40000000 1 400000 1

conv_dense 80000000x100_dense 80000000 100 800000 100 
conv_dense 80000000x10_dense 80000000 10 800000 10
conv_dense 80000000x1_dense 80000000 1 800000 1

conv_dense 100x1_dense 100 1 100 1
conv_dense 10x100_dense 10 100 10 100

# Small Tiles
conv_dense_st 80000000x100_dense "200/80000000x100_dense" 80000000 100 400000 100 
conv_dense_st 80000000x100_dense "400/80000000x100_dense" 80000000 100 200000 100 
conv_dense_st 80000000x100_dense "800/80000000x100_dense" 80000000 100 100000 100 
conv_dense_st 80000000x100_dense "1600/80000000x100_dense" 80000000 100 50000 100 
conv_dense_st 80000000x100_dense "3200/80000000x100_dense" 80000000 100 25000 100 

conv_dense_st 80000000x10_dense "200/80000000x10_dense" 80000000 10 400000 10 
conv_dense_st 80000000x10_dense "400/80000000x10_dense" 80000000 10 200000 10 
conv_dense_st 80000000x10_dense "800/80000000x10_dense" 80000000 10 100000 10 
conv_dense_st 80000000x10_dense "1600/80000000x10_dense" 80000000 10 50000 10 
conv_dense_st 80000000x10_dense "3200/80000000x10_dense" 80000000 10 25000 10 

conv_dense_st 80000000x1_dense "200/80000000x1_dense" 80000000 1 400000 1 
conv_dense_st 80000000x1_dense "400/80000000x1_dense" 80000000 1 200000 1 
conv_dense_st 80000000x1_dense "800/80000000x1_dense" 80000000 1 100000 1 
conv_dense_st 80000000x1_dense "1600/80000000x1_dense" 80000000 1 50000 1 
conv_dense_st 80000000x1_dense "3200/80000000x1_dense" 80000000 1 25000 1 


# Sparse Generation
conv_sparse 400000000x100_sparse_0.0125 400000000 100 4000000 100
conv_sparse 400000000x100_sparse_0.025 400000000 100 4000000 100
conv_sparse 400000000x100_sparse_0.05 400000000 100 4000000 100
conv_sparse 400000000x100_sparse_0.1 400000000 100 4000000 100

conv_sparse 400000000x1_sparse_0.0125 400000000 1 4000000 1
conv_sparse 400000000x1_sparse_0.025 400000000 1 4000000 1
conv_sparse 400000000x1_sparse_0.05 400000000 1 4000000 1
conv_sparse 400000000x1_sparse_0.1 400000000 1 4000000 1

conv_sparse 100x1_sparse_0.0125 100 1 100 1
conv_sparse 100x1_sparse_0.025 100 1 100 1
conv_sparse 100x1_sparse_0.05 100 1 100 1
conv_sparse 100x1_sparse_0.1 100 1 100 1


# PageRank
../../evaluation/prevision/exec_eval CONV 36692 36692 3670 3670 ./output/csv/enron.csv 
mv __*.tilestore ./output/prevision/enron.tilestore

../../evaluation/prevision/exec_eval CONV 75888 75888 7589 7589 ./output/csv/epinions.csv 
mv __*.tilestore ./output/prevision/epinions.tilestore

../../evaluation/prevision/exec_eval CONV 4847571 4847571 484758 484758 ./output/csv/livejournal.csv 
mv __*.tilestore ./output/prevision/livejournal.tilestore

../../evaluation/prevision/exec_eval CONV 61578415 61578415 6157842 6157842 ./output/csv/twitter.csv 
mv __*.tilestore ./output/prevision/twitter.tilestore

../../evaluation/prevision/exec_eval RAND 36692 1 3670 1 0.00002725389
mv __*.tilestore ./output/prevision/enron_v.tilestore

../../evaluation/prevision/exec_eval RAND 75888 1 7589 1 0.00001317731
mv __*.tilestore ./output/prevision/epinions_v.tilestore

../../evaluation/prevision/exec_eval RAND 4847571 1 484758 1 0.00000020628888
mv __*.tilestore ./output/prevision/livejournal_v.tilestore

../../evaluation/prevision/exec_eval RAND 61578415 1 6157842 1 0.00000001623945
mv __*.tilestore ./output/prevision/twitter_v.tilestore


