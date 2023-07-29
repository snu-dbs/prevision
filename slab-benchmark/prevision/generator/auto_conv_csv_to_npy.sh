source ./venv3/bin/activate

python ./generator/conv_csv_to_npy.py ./output/csv/10000000x100_dense.csv ./output/npy/10000000x100_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/10000000x10_dense.csv ./output/npy/10000000x10_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/10000000x1_dense.csv ./output/npy/10000000x1_dense.npy

python ./generator/conv_csv_to_npy.py ./output/csv/20000000x100_dense.csv ./output/npy/20000000x100_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/20000000x10_dense.csv ./output/npy/20000000x10_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/20000000x1_dense.csv ./output/npy/20000000x1_dense.npy

python ./generator/conv_csv_to_npy.py ./output/csv/40000000x100_dense.csv ./output/npy/40000000x100_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/40000000x10_dense.csv ./output/npy/40000000x10_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/40000000x1_dense.csv ./output/npy/40000000x1_dense.npy

python ./generator/conv_csv_to_npy.py ./output/csv/80000000x100_dense.csv ./output/npy/80000000x100_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/80000000x10_dense.csv ./output/npy/80000000x10_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/80000000x1_dense.csv ./output/npy/80000000x1_dense.npy

python ./generator/conv_csv_to_npy.py ./output/csv/100x1_dense.csv ./output/npy/100x1_dense.npy
python ./generator/conv_csv_to_npy.py ./output/csv/10x100_dense.csv ./output/npy/10x100_dense.npy
