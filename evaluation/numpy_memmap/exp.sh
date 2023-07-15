num_of_iter=8

run_lr() {
	dataset=$1
	noi=$2
	npy_tall="./npy/""$dataset""x100_dense.npy"
	npy_lr_y="./npy/""$dataset""x1_dense.npy"
	npy_lr_w="./npy/100x1_dense.npy"

	echo "dataset=""$dataset"
	echo 'LR ' $noi
	for i in $(seq 1 $num_of_iter); do
		cp $npy_lr_w "__TEMP_w.npy"
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sleep 2
		echo 'started'
		/usr/bin/time -f '%e, %U, %S' python eval_numpy_memmap.py LR $npy_tall $npy_lr_y "__TEMP_w.npy" $noi 0.0000001
		rm __*
	done;
}


run_nmf() {
	dataset=$1
	noi=$2
	npy_tall="./npy/""$dataset""x100_dense.npy"
	npy_nmf_w="./npy/""$dataset""x10_dense.npy"
	npy_nmf_h="./npy/10x100_dense.npy"

	echo "dataset=""$dataset"
	echo 'NMF ' $noi
	for i in $(seq 1 $num_of_iter); do
		cp $npy_nmf_w "__TEMP_W.npy"
		cp $npy_nmf_h "__TEMP_H.npy"
		sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
		sleep 2
		echo 'started'
		/usr/bin/time -f '%e, %U, %S' python eval_numpy_memmap.py NMF $npy_tall "__TEMP_W.npy" "__TEMP_H.npy" $noi
		rm __*
	done;

}

num_of_iter=2

iterlist=(1 2 3 4 8 16 32)
#iterlist=(1 2 3 4 8 16 32 64)
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

run_nmf 10000000
run_nmf 20000000
run_nmf 40000000
run_nmf 80000000

run_lr 10000000
run_lr 20000000
run_lr 40000000
run_lr 80000000

exit;
