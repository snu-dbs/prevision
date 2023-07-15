num_of_iter=1

run_lr() {
	dataset=$1
	noi=$2
	npy_tall="./npz_csr/400000000x100_sparse_""$dataset"".csr.npz"
	npy_temp_y="./npz_csr/400000000x1_sparse_""$dataset"".csr.npz"
	npy_temp_w="./npz_csr/""100x1_sparse_""$dataset"".csr.npz"

	echo "dataset=""$dataset"
	echo 'LR'
	for i in $(seq 1 $num_of_iter); do
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python3 csr_swap.py LR $npy_tall $npy_temp_y $npy_temp_w $noi
		rm -rf *_RES.*
	done;
}

run_mnf() {
	dataset=$1
	noi=$2
	npy_tall="./npz_csr/400000000x100_sparse_""$dataset"".csr.npz"
	npy_temp_w="./npz_csr/400000000x10_sparse_""$dataset"".csr.npz"
	npy_temp_h="./npz_csr/""10x100_sparse_""$dataset"".csr.npz"

	echo "dataset=""$dataset"
	echo 'NMF'
	for i in $(seq 1 $num_of_iter); do
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python3 csr_swap.py NMF $npy_tall $npy_temp_w $npy_temp_h $noi
		rm -rf *_RES.*
	done;
}


num_of_iter=1

# run_lr 0.0125 3
# run_lr 0.025 3
# run_lr 0.05 3
# run_lr 0.1 3

run_mnf 0.0125 3
# run_mnf 0.025 3
# run_mnf 0.05 3
# run_mnf 0.1 3
exit;

