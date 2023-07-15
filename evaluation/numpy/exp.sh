num_of_iter=2

run() {
	dataset=$1
	npy_tall="./npy/""$dataset""x100_dense.npy"
	npy_tall2="./npy/""$dataset""x100_dense.2.npy"
	npy_wide="./npy/""100x""$dataset""_dense.npy"
	npy_vector="./npy/""100x1_dense.npy"

	echo "dataset=""$dataset"
	echo 'TRANS'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python profile_numpy.py TRANS $npy_tall
		rm *_RES.npy
	done;

	echo 'NORM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python profile_numpy.py NORM $npy_tall
		# rm *_RES.npy
	done;

	echo 'GRM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python profile_numpy.py GRM $npy_tall
		rm *_RES.npy
	done;

	echo 'MVM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python profile_numpy.py MVM $npy_tall $npy_vector
		rm *_RES.npy
	done;

	echo 'ADD'
	cp $npy_tall $npy_tall2
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python profile_numpy.py ADD $npy_tall $npy_tall2
		rm *_RES.npy
	done;
	rm $npy_tall2

	echo 'GMM'
	for i in $(seq 1 $num_of_iter); do
		sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		python profile_numpy.py GMM $npy_wide $npy_tall
		rm *_RES.npy
	done;
}

run 2500000
run 5000000
run 10000000
run 20000000
run 40000000
run 80000000
