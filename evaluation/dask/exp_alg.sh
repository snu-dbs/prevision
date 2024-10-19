num_of_iter=1

DATADIR="../../slab-benchmark/prevision/output/hdf5/"

run_nmf() {
	dataset=$1
	noi=$2
	npy_tall="$DATADIR""$dataset""x100_dense.hdf5"
	npy_nmf_w="$DATADIR""$dataset""x10_dense.hdf5"
	npy_nmf_h="$DATADIR""regular/10x100_dense.hdf5"

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
	npy_tall="$DATADIR""$dataset""x100_dense.hdf5"
	npy_lr_y="$DATADIR""$dataset""x1_dense.hdf5"
	npy_lr_w="$DATADIR""regular/100x1_dense.hdf5"

	echo "dataset=""$dataset"
	echo 'LR'
	for i in $(seq 1 $num_of_iter); do
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		/usr/bin/time -f '%e,%U,%S' python eval_dask.py LR $npy_tall $npy_lr_y $npy_lr_w $noi
		rm *.hdf5
	done;

}

run_lr "regular/10000000" 3
run_lr "regular/20000000" 3
run_lr "regular/40000000" 3
run_lr "regular/80000000" 3

run_nmf "regular/10000000" 3
run_nmf "regular/20000000" 3
run_nmf "regular/40000000" 3
run_nmf "regular/80000000" 3

# parallelism
iterlist=(1 2 4 8 16 32)
for noi in ${iterlist[@]}; do
       echo "num_of_iter=$noi";
       echo "NMF"
       run_nmf "regular/10000000" $noi
done;

# small tiles
run_nmf "small/200/80000000" 3
run_nmf "small/400/80000000" 3
run_nmf "small/800/80000000" 3
run_nmf "small/1600/80000000" 3
run_nmf "small/3200/80000000" 3

# num of dask thread
threadlist=(2 4 8)
for thread in ${threadlist[@]}; do
    echo "parallelism=$thread";
    export _PREVISION_DASK_THREAD=$thread
    run_nmf "regular/10000000" 3
done;
