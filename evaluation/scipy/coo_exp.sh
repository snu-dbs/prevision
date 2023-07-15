num_of_iter=8

run() {
	dataset=$1
	npy_tall="./npz_coo/400000000x100_sparse_""$dataset"".npz.coo.npz"
	npy_tall2="./npz_coo/400000000x100_sparse_""$dataset"".2.npz.coo.npz"
	npy_wide="./npz_coo/100x400000000_sparse_""$dataset"".npz.coo.npz"
	npy_vector="./npz_coo/""100x1_dense.npy"

	echo "dataset=""$dataset"
	echo 'TRANS'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		time python coo_swap.py TRANS $npy_tall
		rm *_RES*
	done;

	echo 'NORM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		time python coo_swap.py NORM $npy_tall
		# rm *_RES.npy
	done;

	echo 'GRM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		time python coo_swap.py GRM $npy_tall
		rm *_RES*
	done;

	echo 'MVM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		time python coo_swap.py MVM $npy_tall $npy_vector
		rm *_RES*
	done;

	echo 'ADD'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		time python coo_swap.py ADD $npy_tall $npy_tall2
		rm *_RES*
	done;

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		time python coo_swap.py GMM $npy_wide $npy_tall
		rm *_RES*
	done;
}

run 0.0125
run 0.025
run 0.05
exit;

run 0.1
