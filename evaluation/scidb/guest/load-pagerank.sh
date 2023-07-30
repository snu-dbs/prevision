# /bin/bash
DIR=../../../slab-benchmark/prevision/output/scidb/

import() {
	dataset=$1
	arrsize=$2
	tilesize=$3
	arrend=$((arrsize-1))

	# square matrix
	iquery -aq "CREATE ARRAY mat_"$dataset"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
	#iquery -aq "load(mat_"$dataset"_coo,'$DIR/$dataset/$dataset""_pagerank.tsv', -2, 'TSV');"
	iquery -aq "load(mat_"$dataset"_coo,'/home/scidb/$dataset""_pagerank.tsv', -2, 'TSV');"
	iquery -aq "store(redimension(mat_"$dataset"_coo, <value:double NOT NULL>[i=0:"$arrend":0:"$tilesize"; j=0:"$arrend":0:"$tilesize"]), mat_"$dataset")"
	iquery -aq "remove(mat_"$dataset"_coo)"

	# input vector
	iquery -aq "CREATE ARRAY mat_"$dataset"_v <value:double NOT NULL>[i=0:"$arrend":0:"$tilesize"; j=0:0:0:1];"
	iquery -aq "store(build(mat_"$dataset"_v, double(1)/"$arrsize"), mat_"$dataset"_v);"
}

import enron 36692 3670
import epinions 75888 7589
import livejournal 4847571 484758
import twitter 61578415 6157842
