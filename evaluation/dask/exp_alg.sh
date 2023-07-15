num_of_iter=8

run_nmf() {
	dataset=$1
	noi=$2
	npy_tall="./hdf5/""$dataset""x100_dense.hdf5"
	npy_nmf_w="./hdf5/""$dataset""x10_dense.hdf5"
	npy_nmf_h="./hdf5/10x100_dense.hdf5"

	echo "dataset=""$dataset"
	echo 'NMF'
	for i in $(seq 1 $num_of_iter); do
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python eval_dask.py NMF $npy_tall $npy_nmf_w $npy_nmf_h $noi
		rm *.hdf5
	done;

}

run_lr() {
	dataset=$1
	noi=$2
	npy_tall="./hdf5/""$dataset""x100_dense.hdf5"
	npy_lr_y="./hdf5/""$dataset""x1_dense.hdf5"
	npy_lr_w="./hdf5/100x1_dense.hdf5"

	echo "dataset=""$dataset"
	echo 'LR'
	for i in $(seq 1 $num_of_iter); do
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python eval_dask.py LR $npy_tall $npy_lr_y $npy_lr_w $noi
		rm *.hdf5
	done;

}

num_of_iter=2

iterlist=(1 2 3 4 8 16 32)
for noi in ${iterlist[@]}; do
        echo "num_of_iter=$noi";
        echo "LR"
	run_lr 10000000 $noi
	run_lr 80000000 $noi

        echo "NMF"
	run_nmf 10000000 $noi
	run_nmf 80000000 $noi
done;

exit;

run_lr 80000000
run_lr 40000000
run_lr 20000000
run_lr 10000000

run_nmf 10000000
run_nmf 20000000
run_nmf 40000000
run_nmf 80000000


