run_nmf() {
        dataset=$1
        npy_tall="./npy/""$dataset""x100_dense.npy"
        npy_nmf_w="./npy/""$dataset""x10_dense.npy"
        npy_nmf_h="./npy/10x100_dense.npy"

        # npy_tall="/mnt/hdd/pgtile/data/dense/npy/""$dataset""x100_dense.npy"
        # npy_nmf_w="/mnt/hdd/pgtile/data/dense/npy/""$dataset""x10_dense.npy"
        # npy_nmf_h="/mnt/hdd/pgtile/data/dense/npy/10x100_dense.npy"

        echo "dataset=""$dataset"
        echo 'NMF_3'

        for i in $(seq 1 $num_of_iter); do
                cp $npy_nmf_w "TEMP_W.npy"
                cp $npy_nmf_h "TEMP_H.npy"
                # sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                sleep 2
                echo 'drop caches'
                # /usr/bin/time -f '%e, user: %U, syscall: %S (sec)' python eval_numpy_memmap.py NMF $npy_tall
                /usr/bin/time -f '%e, user: %U, syscall: %S (sec)' python eval_numpy_memmap.py LR $npy_tall ./npy/10000000x1_dense.npy ./npy/100x1_dense.npy
                # rm __*
                rm TEMP_W.npy TEMP_H.npy
        done;

}

run_nmf 10000000
