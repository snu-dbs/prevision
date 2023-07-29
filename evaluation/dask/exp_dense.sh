num_of_iter=8

run() {
	dataset=$1
	npy_tall="./hdf5/""$dataset""x100_dense.hdf5"
	npy_tall2="./hdf5/""$dataset""x100_dense.2.hdf5"
	npy_wide="./hdf5/""100x""$dataset""_dense.hdf5"
	npy_vector="./hdf5/""100x1_dense.hdf5"

	echo "dataset=""$dataset"
	echo 'TRANS'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python eval_dask_dense.py TRANS $npy_tall
		rm *_RES*
	done;

	echo 'NORM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python eval_dask_dense.py NORM $npy_tall
	done;

	echo 'GRM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python eval_dask_dense.py GRM $npy_tall
		rm *_RES*
	done;

	echo 'MVM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python eval_dask_dense.py MVM $npy_tall $npy_vector
		rm *_RES*
	done;

	echo 'ADD'
	cp $npy_tall $npy_tall2
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python eval_dask_dense.py ADD $npy_tall $npy_tall2
		rm *_RES*
	done;
	rm $npy_tall2

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python eval_dask_dense.py GMM $npy_wide $npy_tall
		rm *_RES*
	done;
}

run 10000000
run 20000000
run 40000000
run 80000000
exit;

run 2500000
run 5000000
