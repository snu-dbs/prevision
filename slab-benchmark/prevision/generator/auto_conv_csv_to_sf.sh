export SPARK_LOCAL_DIRS=./tmp
export MASTER="local[8]"

MEM_CONF="--driver-memory 80g --executor-memory 80g"
DENSE_INPATH="./output/csv"
DENSE_OUTPATH="./output/sequencefile"
SPARSE_INPATH="./output/csv"
SPARSE_OUTPATH="./output/sequencefile"
PR_SPARSE_INPATH="./output/csv"
PR_SPARSE_OUTPUT="./output/sequencefile"

function dense_conv {
    NROW=$1
    NCOL=$2
    INFILE=$DENSE_INPATH"/"$NROW"x"$NCOL"_dense.csv"
    OUTFILE=$DENSE_OUTPATH"/"$NROW"x"$NCOL"_dense"
    spark-shell $MEM_CONF -i ./generator/conv_csv_to_dense_sf.scala --conf spark.driver.args="$INFILE $OUTFILE $NROW $NCOL"
}

function sparse_conv {
    NROW=$1
    NCOL=$2
    DENSITY=$3
    INFILE=$SPARSE_INPATH"/"$NROW"x"$NCOL"_sparse_"$DENSITY".csv"
    OUTFILE=$DENSE_OUTPATH"/"$NROW"x"$NCOL"_sparse_"$DENSITY
    spark-shell $MEM_CONF -i ./generator/conv_csv_to_sparse_sf.scala --conf spark.driver.args="$INFILE $OUTFILE $NROW $NCOL"
}

function pr_sparse_conv {
    NAME=$1
    N=$2
    INFILE=$PR_SPARSE_PATH"/"$NAME"_pagerank.tsv"
    OUTFILE=$PR_SPARSE_PATH"/"$NAME
    spark-shell $MEM_CONF -i ./generator/conv_csv_to_pagerank_sf.scala --conf spark.driver.args="$INFILE $OUTFILE $N"
}

function pr_dv_gen {
    NAME=$1
    N=$2
    M=$3
    OUTFILE=$PR_SPARSE_PATH"/"$NAME
    spark-shell $MEM_CONF -i ./generator/gen_sf_dv.scala --conf spark.driver.args="$OUTFILE $N $M"
}

# Dense 
dense_conv 10000000 100
dense_conv 20000000 100
dense_conv 40000000 100
dense_conv 80000000 100

dense_conv 10000000 10
dense_conv 20000000 10
dense_conv 40000000 10
dense_conv 80000000 10

dense_conv 10000000 1
dense_conv 20000000 1
dense_conv 40000000 1
dense_conv 80000000 1

dense_conv 10 100
dense_conv 100 1

# Sparse
sparse_conv 400000000 100 0.0125
sparse_conv 400000000 100 0.025 
sparse_conv 400000000 100 0.05 
sparse_conv 400000000 100 0.1

sparse_conv 400000000 1 0.0125
sparse_conv 400000000 1 0.025 
sparse_conv 400000000 1 0.05 
sparse_conv 400000000 1 0.1

sparse_conv 100 1 0.0125
sparse_conv 100 1 0.025 
sparse_conv 100 1 0.05 
sparse_conv 100 1 0.1

# PageRank
pr_sparse_conv enron 36700 
pr_sparse_conv epinions 75890
pr_sparse_conv livejournal 4847580
pr_sparse_conv twitter 61578420

pr_dv_gen enron 36692 3670
pr_dv_gen epinions 75888 7589
pr_dv_gen livejournal 4847571 484758
pr_dv_gen twitter 61578415 6157842