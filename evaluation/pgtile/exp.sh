num_of_iter=2

run_nmf() {
	dataset=$1
	npy_tall="./tilestore/""$dataset""x100_dense"
	npy_temp_w="./tilestore/""$dataset""x10_dense"
	npy_temp_h="./tilestore/10x100_dense"

	echo "dataset=""$dataset"

	echo 'NMF'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval NMF $npy_tall $npy_temp_w $npy_temp_h
		rm -rf __*
	done;
}

run_grm_mvm_gmm() {
	dataset=$1
	npy_tall="./tiledb/""$dataset""x100_dense.tiledb"
	npy_tall2="./tiledb/""$dataset""x100_dense.2.tiledb"
	npy_wide="./tiledb/""100x""$dataset""_dense.tiledb"
	npy_vector="./tiledb/""100x1_dense.tiledb"

	echo "dataset=""$dataset"

	echo 'GRM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval GRM $npy_tall
		rm -rf __*
	done;

	echo 'MVM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval MVM $npy_tall $npy_vector
		rm -rf __*
	done;

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval GMM $npy_wide $npy_tall
		rm -rf __*
	done;
}

run_dense() {
	dataset=$1
	npy_tall="./tiledb/""$dataset""x100_dense.tiledb"
	npy_tall2="./tiledb/""$dataset""x100_dense.2.tiledb"
	npy_wide="./tiledb/""100x""$dataset""_dense.tiledb"
	npy_vector="./tiledb/""100x1_dense.tiledb"

	echo "dataset=""$dataset"
	echo 'TRANS'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval TRANS $npy_tall
		rm -rf __*
	done;

	#echo 'NORM'
	#for i in $(seq 1 $num_of_iter); do
	#	sh -c 'echo 3 > /proc/sys/vm/drop_caches'
	#	python eval_numpy.py NORM $npy_tall
	#done;

	echo 'GRM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval GRM $npy_tall
		rm -rf __*
	done;

	echo 'MVM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval MVM $npy_tall $npy_vector
		rm -rf __*
	done;

	echo 'ADD'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval ADD $npy_tall $npy_tall2
		rm -rf __*
	done;

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval GMM $npy_wide $npy_tall
		rm -rf __*
	done;
}

run_sparse_add() {
	dataset=$1
	npy_tall="./tiledb/400000000x100_sparse_""$dataset"".tiledb"
	npy_tall2="./tiledb/400000000x100_sparse_""$dataset"".2.tiledb"
	npy_wide="./tiledb/100x400000000_sparse_""$dataset"".tiledb"
	npy_vector="./tiledb/""100x1_dense.tiledb"

	echo 'ADD'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval ADD $npy_tall $npy_tall2
		rm -rf __*
	done;

}

run_sparse_gmm() {
	dataset=$1
	npy_tall="./tiledb/400000000x100_sparse_""$dataset"".tiledb"
	npy_tall2="./tiledb/400000000x100_sparse_""$dataset"".2.tiledb"
	npy_wide="./tiledb/100x400000000_sparse_""$dataset"".tiledb"
	npy_vector="./tiledb/""100x1_dense.tiledb"

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval GMM $npy_wide $npy_tall
		rm -rf __*
	done;

}

run_sparse_gmm_difftile() {
	dataset=$1
	dirpath=$2
	npy_tall="./tiledb-tilesize/""$dirpath""/400000000x100_sparse_""$dataset"".tiledb"
	npy_wide="./tiledb-tilesize/""$dirpath""/100x400000000_sparse_""$dataset"".tiledb"

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		./exec_eval GMM $npy_wide $npy_tall
		rm -rf __*
	done;

}

set_env() {
	export OMP_NUM_THREADS=1
	export OPENBLAS_NUM_THREADS=1
	export MKL_NUM_THREADS=1
	export VECLIB_MAXIMUM_THREADS=1
	export NUMEXPR_NUM_THREADS=1
}


set_env

num_of_iter=2
run_nmf 10000000
run_nmf 20000000
run_nmf 40000000
run_nmf 80000000
exit;


run_sparse_gmm_difftile 0.0125 4Mx100

num_of_iter=1

run_sparse_gmm_difftile 0.0125 1Mx100
run_sparse_gmm_difftile 0.0125 2Mx100
run_sparse_gmm_difftile 0.0125 8Mx100
run_sparse_gmm_difftile 0.0125 4Mx50

# run_sparse_gmm 0.025
# run_sparse_gmm 0.05
# run_sparse_gmm 0.1

exit;

run_sparse_add 0.1
exit;

run_dense 10000000
run_dense 20000000
run_dense 40000000
run_dense 80000000
exit;

run_grm_mvm_gmm 10000000
run_grm_mvm_gmm 20000000
run_grm_mvm_gmm 40000000
run_grm_mvm_gmm 80000000
exit;

run_sparse_add 0.025
run_sparse_add 0.0125
run_sparse_add 0.05

#run_trans 80000000

#run_add 2500000
#run_add 5000000
#run_add 10000000
#run_add 80000000
#run_add 40000000
#run_add 20000000

#run2 2500000
#run 5000000
#run2 10000000
# run 20000000
# run2 40000000
#run2 40000000
