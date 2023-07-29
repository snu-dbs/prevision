source ./venv3/bin/activate

echo "convert for main comparsion"
python ./generator/conv_npy_to_hdf5.py ./output/npy/10000000x100_dense.npy ./output/hdf5/regular/10000000x100_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/10000000x10_dense.npy ./output/hdf5/regular/10000000x10_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/10000000x1_dense.npy ./output/hdf5/regular/10000000x1_dense.hdf5 100

python ./generator/conv_npy_to_hdf5.py ./output/npy/20000000x100_dense.npy ./output/hdf5/regular/20000000x100_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/20000000x10_dense.npy ./output/hdf5/regular/20000000x10_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/20000000x1_dense.npy ./output/hdf5/regular/20000000x1_dense.hdf5 100

python ./generator/conv_npy_to_hdf5.py ./output/npy/40000000x100_dense.npy ./output/hdf5/regular/40000000x100_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/40000000x10_dense.npy ./output/hdf5/regular/40000000x10_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/40000000x1_dense.npy ./output/hdf5/regular/40000000x1_dense.hdf5 100

python ./generator/conv_npy_to_hdf5.py ./output/npy/80000000x100_dense.npy ./output/hdf5/regular/80000000x100_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/80000000x10_dense.npy ./output/hdf5/regular/80000000x10_dense.hdf5 100
python ./generator/conv_npy_to_hdf5.py ./output/npy/80000000x1_dense.npy ./output/hdf5/regular/80000000x1_dense.hdf5 100

python ./generator/conv_npy_to_hdf5.py ./output/npy/100x1_dense.npy ./output/hdf5/regular/100x1_dense.hdf5 1
python ./generator/conv_npy_to_hdf5.py ./output/npy/10x100_dense.npy ./output/hdf5/regular/10x100_dense.hdf5 1

echo "convert for smaller tiles"
l=(200 400 800 1600 3200)
for i in ${l[@]}
do
    python ./generator/conv_npy_to_hdf5.py ./output/npy/80000000x100_dense.npy ./output/hdf5/small/$i/80000000x100_dense.hdf5 $i
    python ./generator/conv_npy_to_hdf5.py ./output/npy/80000000x10_dense.npy ./output/hdf5/small/$i/80000000x10_dense.hdf5 $i
    # python ./generator/conv_npy_to_hdf5.py ./output/csv/80000000x1_dense.npy ./output/hdf5/small/$i/80000000x1_dense.hdf5 $i
done;

