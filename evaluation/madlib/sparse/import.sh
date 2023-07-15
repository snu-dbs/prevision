function import_pagerank() {
	dataset=$1
	csvpath="/mnt/nas/pgtile/pagerank/"$dataset"/"$dataset"_pagerank.ijv"

	psql -c "CREATE TABLE mat_$dataset (row_id INTEGER, col_id INTEGER, val DOUBLE PRECISION DEFAULT 1 NOT NULL);"
	psql -c "COPY mat_$dataset (row_id, col_id, val) FROM '"$csvpath"' DELIMITER E'\t' CSV;"

	# psql -c "CREATE INDEX ON mat_$dataset USING HASH (row_id);"
	psql -c "CREATE INDEX ON mat_$dataset (row_id);"
	psql -c "CREATE INDEX ON mat_$dataset (col_id);"
	psql -c "CREATE INDEX ON mat_$dataset (row_id, col_id);"
	psql -c "CREATE INDEX ON mat_$dataset (col_id, row_id);"
}

function import() {
	rowsize=$1
	colsize=$2
	density1=$3
	density2=$4

        rowsize2=$5

        # rowsize2=400M
	# colsize=100
	# density1=0.0125
	# density2=0_0125

	csvpath="/mnt/nas/pgtile/slab-dataset/syntactic-sparse/ijv/"$rowsize"x"$colsize"_sparse_"$density1"_modified.ijv"

	psql -c "CREATE TABLE mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id INTEGER, col_id INTEGER, val DOUBLE PRECISION);"
	psql -c "COPY mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id, col_id, val) FROM '"$csvpath"' DELIMITER ' ' CSV;"

	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" USING HASH (row_id);"
	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id);"
	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" (col_id);"
	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id, col_id);"

        # Never ending index creation...
	# We don't create hash index for col_id  since we are thinking of tall-skinny matrix. the collision should be happen a lot.
	# psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" USING HASH (col_id);"
}

import_pagerank enron;
exit;
import_pagerank epinions;
import_pagerank livejournal;
import_pagerank twitter;

exit;
import 400000000 100 0.1 0_1 400M
import 400000000 1 0.1 0_1 400M
import 100 1 0.1 0_1 100
exit;


import 400000000 100 0.05 0_05 400M
import 400000000 1 0.05 0_05 400M
import 100 1 0.05 0_05 100
exit;


import 400000000 100 0.0125 0_0125 400M
import 400000000 1 0.0125 0_0125 400M
import 100 1 0.0125 0_0125 100

import 400000000 100 0.025 0_025 400M
import 400000000 1 0.025 0_025 400M
import 100 1 0.025 0_025 100
