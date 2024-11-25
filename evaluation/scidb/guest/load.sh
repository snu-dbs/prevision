# /bin/bash
task=$1
data=$2

function load_sparse() {
	DIR=/data/prevision/slab-benchmark/prevision/output/csv

	# Import 
	density=$1
	density2=$2

	# 400000000x100_sparse 
	iquery -aq "CREATE ARRAY mat_400Mx100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
	iquery -aq "load(mat_400Mx100_sparse_"$density2"_coo,'$DIR/400000000x100_sparse_"$density".csv', -2, 'CSV');"
	iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2")"
	iquery -aq "remove(mat_400Mx100_sparse_"$density2"_coo)"

	# 400000000x1_sparse 
	iquery -aq "CREATE ARRAY mat_400Mx1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
	iquery -aq "load(mat_400Mx1_sparse_"$density2"_coo,'$DIR/400000000x1_sparse_"$density".csv', -2, 'CSV');"
	iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2")"
	iquery -aq "remove(mat_400Mx1_sparse_"$density2"_coo)"

	# 100x1_sparse
	iquery -aq "CREATE ARRAY mat_100x1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
	iquery -aq "load(mat_100x1_sparse_"$density2"_coo,'$DIR/100x1_sparse_"$density".csv', -2, 'CSV');"
	iquery -aq "store(redimension(mat_100x1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:99:0:100; j=0:0:0:1]), mat_100x1_sparse_"$density2")"
	iquery -aq "remove(mat_100x1_sparse_"$density2"_coo)"
}


function load_pagerank() {
	DIR=/data/prevision/slab-benchmark/prevision/output/scidb

	dataset=$1
	arrsize=$2
	tilesize=$3
	arrend=$((arrsize-1))

	# square matrix
	iquery -aq "CREATE ARRAY mat_"$dataset"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
	iquery -aq "load(mat_"$dataset"_coo,'$DIR/$dataset""_pagerank.tsv', -2, 'TSV');"
	iquery -aq "store(redimension(mat_"$dataset"_coo, <value:double NOT NULL>[i=0:"$arrend":0:"$tilesize"; j=0:"$arrend":0:"$tilesize"]), mat_"$dataset")"
	iquery -aq "remove(mat_"$dataset"_coo)"

	# input vector
	iquery -aq "CREATE ARRAY mat_"$dataset"_v <value:double NOT NULL>[i=0:"$arrend":0:"$tilesize"; j=0:0:0:1];"
	iquery -aq "store(build(mat_"$dataset"_v, double(1)/"$arrsize"), mat_"$dataset"_v);"
}

function load_pagerank_twitter() {
	DIR=/data/prevision/slab-benchmark/prevision/output/scidb

	dataset=$1
	arrsize=$2
	tilesize=$3
	arrend=$((arrsize-1))

	# square matrix
	iquery -aq "CREATE ARRAY mat_"$dataset"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
	iquery -aq "load(mat_"$dataset"_coo,'$DIR/$dataset""_pagerank.tsv', -2, 'TSV');"
	iquery -aq "store(redimension(mat_"$dataset"_coo, <value:double NOT NULL>[i=0:"$arrend":0:"$tilesize"; j=0:"$arrend":0:"$tilesize"]), mat_"$dataset")"
	iquery -aq "remove(mat_"$dataset"_coo)"

	# input vector
	iquery -aq "CREATE ARRAY mat_"$dataset"_v <value:double NOT NULL>[i=0:"$arrend":0:"$tilesize"; j=0:0:0:1];"
	iquery -aq "store(build(mat_"$dataset"_v, double(1)/"$arrsize"), mat_"$dataset"_v);"
}


# run task
if [[ $task == "lr" ]]; then
	DIR=/data/prevision/slab-benchmark/prevision/output/scidb
	if [[ $data == "10m" ]]; then
		# 10000000x100_dense
		iquery -aq "CREATE ARRAY mat_10Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_10Mx100_dense_coo,'$DIR/10000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_10Mx100_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:99:0:1000]), mat_10Mx100_dense)"
		iquery -aq "remove(mat_10Mx100_dense_coo)"

		# 10000000x1_dense
		iquery -aq "CREATE ARRAY mat_10Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_10Mx1_dense_coo,'$DIR/10000000x1_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_10Mx1_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:0:0:1000]), mat_10Mx1_dense)"
		iquery -aq "remove(mat_10Mx1_dense_coo)"
	elif [[ $data == "20m" ]]; then
		# 20000000x100_dense 
		iquery -aq "CREATE ARRAY mat_20Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_20Mx100_dense_coo,'$DIR/20000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_20Mx100_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:99:0:1000]), mat_20Mx100_dense)"
		iquery -aq "remove(mat_20Mx100_dense_coo)"

		# 20000000x1_dense 
		iquery -aq "CREATE ARRAY mat_20Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_20Mx1_dense_coo,'$DIR/20000000x1_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_20Mx1_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:0:0:1000]), mat_20Mx1_dense)"
		iquery -aq "remove(mat_20Mx1_dense_coo)"
	elif [[ $data == "40m" ]]; then
		# 40000000x100_dense 
		iquery -aq "CREATE ARRAY mat_40Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_40Mx100_dense_coo,'$DIR/40000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_40Mx100_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:99:0:1000]), mat_40Mx100_dense)"
		iquery -aq "remove(mat_40Mx100_dense_coo)"

		# 40000000x1_dense 
		iquery -aq "CREATE ARRAY mat_40Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_40Mx1_dense_coo,'$DIR/40000000x1_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_40Mx1_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:0:0:1000]), mat_40Mx1_dense)"
		iquery -aq "remove(mat_40Mx1_dense_coo)"
	elif [[ $data == "80m" ]]; then
		# 80000000x100_dense 
		iquery -aq "CREATE ARRAY mat_80Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_80Mx100_dense_coo,'$DIR/80000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_80Mx100_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:99:0:1000]), mat_80Mx100_dense)"
		iquery -aq "remove(mat_80Mx100_dense_coo)"

		# 80000000x1_dense 
		iquery -aq "CREATE ARRAY mat_80Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_80Mx1_dense_coo,'$DIR/80000000x1_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_80Mx1_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:0:0:1000]), mat_80Mx1_dense)"
		iquery -aq "remove(mat_80Mx1_dense_coo)"
	fi

	# 100x1_dense
	iquery -aq "CREATE ARRAY mat_100x1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
	iquery -aq "load(mat_100x1_dense_coo,'$DIR/100x1_dense_coo.csv', -2, 'CSV');"
	iquery -aq "store(redimension(mat_100x1_dense_coo, <value:double>[i=0:99:0:1000; j=0:0:0:1000]), mat_100x1_dense)"
	iquery -aq "remove(mat_100x1_dense_coo)"
elif [[ $task == "nmf" ]]; then
	DIR=/data/prevision/slab-benchmark/prevision/output/scidb
	if [[ $data == "10m" ]]; then
		# 10000000x100_dense
		iquery -aq "CREATE ARRAY mat_10Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_10Mx100_dense_coo,'$DIR/10000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_10Mx100_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:99:0:1000]), mat_10Mx100_dense)"
		iquery -aq "remove(mat_10Mx100_dense_coo)"

		# 10000000x10_dense
		iquery -aq "CREATE ARRAY mat_10Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_10Mx10_dense_coo,'$DIR/10000000x10_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_10Mx10_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:9:0:1000]), mat_10Mx10_dense)"
		iquery -aq "remove(mat_10Mx10_dense_coo)"
	elif [[ $data == "20m" ]]; then
		# 20000000x100_dense 
		iquery -aq "CREATE ARRAY mat_20Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_20Mx100_dense_coo,'$DIR/20000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_20Mx100_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:99:0:1000]), mat_20Mx100_dense)"
		iquery -aq "remove(mat_20Mx100_dense_coo)"

		# 20000000x10_dense 
		iquery -aq "CREATE ARRAY mat_20Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_20Mx10_dense_coo,'$DIR/20000000x10_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_20Mx10_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:9:0:1000]), mat_20Mx10_dense)"
		iquery -aq "remove(mat_20Mx10_dense_coo)"
	elif [[ $data == "40m" ]]; then
		# 40000000x100_dense 
		iquery -aq "CREATE ARRAY mat_40Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_40Mx100_dense_coo,'$DIR/40000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_40Mx100_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:99:0:1000]), mat_40Mx100_dense)"
		iquery -aq "remove(mat_40Mx100_dense_coo)"

		# 40000000x10_dense 
		iquery -aq "CREATE ARRAY mat_40Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_40Mx10_dense_coo,'$DIR/40000000x10_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_40Mx10_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:9:0:1000]), mat_40Mx10_dense)"
		iquery -aq "remove(mat_40Mx10_dense_coo)"
	elif [[ $data == "80m" ]]; then
		# 80000000x100_dense 
		iquery -aq "CREATE ARRAY mat_80Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_80Mx100_dense_coo,'$DIR/80000000x100_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_80Mx100_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:99:0:1000]), mat_80Mx100_dense)"
		iquery -aq "remove(mat_80Mx100_dense_coo)"

		# 80000000x10_dense 
		iquery -aq "CREATE ARRAY mat_80Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
		iquery -aq "load(mat_80Mx10_dense_coo,'$DIR/80000000x10_dense_coo.csv', -2, 'CSV');"
		iquery -aq "store(redimension(mat_80Mx10_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:9:0:1000]), mat_80Mx10_dense)"
		iquery -aq "remove(mat_80Mx10_dense_coo)"
	fi

	# 10x100_dense
	iquery -aq "CREATE ARRAY mat_10x100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
	iquery -aq "load(mat_10x100_dense_coo,'$DIR/10x100_dense_coo.csv', -2, 'CSV');"
	iquery -aq "store(redimension(mat_10x100_dense_coo, <value:double>[i=0:9:0:1000; j=0:99:0:1000]), mat_10x100_dense)"
	iquery -aq "remove(mat_10x100_dense_coo)"
elif [[ $task == "slr" ]]; then
	if [[ $data == "0.0125" ]]; then
		load_sparse 0.0125 0_0125
	elif [[ $data == "0.025" ]]; then
		load_sparse 0.025 0_025
	elif [[ $data == "0.05" ]]; then
		load_sparse 0.05 0_05
	elif [[ $data == "0.1" ]]; then
		load_sparse 0.1 0_1
	fi
elif [[ $task == "pagerank" ]]; then
	if [[ $data == "enron" ]]; then
		load_pagerank enron 36692 3670
	elif [[ $data == "epinions" ]]; then
		load_pagerank epinions 75888 7589
	elif [[ $data == "livejournal" ]]; then
		load_pagerank livejournal 4847571 484758
	elif [[ $data == "twitter" ]]; then
		load_pagerank_twitter twitter 61578415 3078921
	fi
fi

