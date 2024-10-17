DATAPATH="../../slab-benchmark/prevision/output/ijv"

function import_pagerank() {
	dataset=$1
	csvpath=$DATAPATH"/"$dataset".ijv"

	psql -c "CREATE TABLE mat_$dataset (row_id INTEGER, col_id INTEGER, val DOUBLE PRECISION DEFAULT 1 NOT NULL);"
	psql -c "\COPY mat_$dataset (row_id, col_id, val) FROM '"$csvpath"' DELIMITER E'\t' CSV;"

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

	csvpath=$DATAPATH"/"$rowsize"x"$colsize"_sparse_"$density1".ijv"

	psql -c "CREATE TABLE mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id INTEGER, col_id INTEGER, val DOUBLE PRECISION);"
	psql -c "\COPY mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id, col_id, val) FROM '"$csvpath"' DELIMITER ' ' CSV;"

	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" USING HASH (row_id);"
	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id);"
	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" (col_id);"
	psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" (row_id, col_id);"

	# Never ending index creation...
	# We don't create hash index for col_id  since we are thinking of tall-skinny matrix. the collision should be happen a lot.
	# psql -c "CREATE INDEX ON mat_"$rowsize2"x"$colsize"_sparse_"$density2" USING HASH (col_id);"
}

function ensure_size() {
	# MADlib could raise a dimension mismatch error when running sparse experiments.
	# This is because MADlib infers matrix sizes using inserted cell values.
	# To solve this issue, just insert an upper-left cell with zero value (i.e., `row_id=1, col_id=1, value=0`) and a lower-right cell with zero value.

	name=$1
	row_id=$2
	col_id=$3

	psql -c "
			INSERT INTO $name (row_id, col_id, val)
			SELECT $row_id, $col_id, 0
			WHERE NOT EXISTS (
					SELECT 1 FROM $name WHERE row_id = $row_id AND col_id = $col_id
			);
			"
}

####################
# Sparse density=0.0125
####################
import 400000000 100 0.0125 0_0125 400M
import 400000000 1 0.0125 0_0125 400M
import 100 1 0.0125 0_0125 100

ensure_size mat_100x1_sparse_0_0125 1 1
ensure_size mat_100x1_sparse_0_0125 100 1
ensure_size mat_400mx1_sparse_0_0125 1 1
ensure_size mat_400mx1_sparse_0_0125 400000000 1
ensure_size mat_400mx100_sparse_0_0125 1 1
ensure_size mat_400mx100_sparse_0_0125 400000000 100

####################
# Sparse density=0.025 (run if you want to test timeout)
####################
# import 400000000 100 0.025 0_025 400M
# import 400000000 1 0.025 0_025 400M
# import 100 1 0.025 0_025 100

# ensure_size mat_100x1_sparse_0_025 1 1
# ensure_size mat_100x1_sparse_0_025 100 1
# ensure_size mat_400mx1_sparse_0_025 1 1
# ensure_size mat_400mx1_sparse_0_025 400000000 1
# ensure_size mat_400mx100_sparse_0_025 1 1
# ensure_size mat_400mx100_sparse_0_025 400000000 100

####################
# Sparse density=0.05 (run if you want to test timeout)
####################
# import 400000000 100 0.05 0_05 400M
# import 400000000 1 0.05 0_05 400M
# import 100 1 0.05 0_05 100

# ensure_size mat_100x1_sparse_0_05 1 1
# ensure_size mat_100x1_sparse_0_05 100 1
# ensure_size mat_400mx1_sparse_0_05 1 1
# ensure_size mat_400mx1_sparse_0_05 400000000 1
# ensure_size mat_400mx100_sparse_0_05 1 1
# ensure_size mat_400mx100_sparse_0_05 400000000 100

####################
# Sparse density=0.1 (run if you want to test timeout)
####################
# import 400000000 100 0.1 0_1 400M
# import 400000000 1 0.1 0_1 400M
# import 100 1 0.1 0_1 100

#ensure_size mat_100x1_sparse_0_1 1 1
#ensure_size mat_100x1_sparse_0_1 100 1
#ensure_size mat_400mx1_sparse_0_1 1 1
#ensure_size mat_400mx1_sparse_0_1 400000000 1
#ensure_size mat_400mx100_sparse_0_1 1 1
#ensure_size mat_400mx100_sparse_0_1 400000000 100


####################
# PageRank
####################

import_pagerank enron;
import_pagerank epinions;
import_pagerank livejournal;
import_pagerank twitter;