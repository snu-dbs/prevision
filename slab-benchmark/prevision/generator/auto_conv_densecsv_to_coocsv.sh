source ./venv3/bin/activate

python ./generator/conv_densecsv_to_coocsv.py ./output/csv/10000000x100_dense.csv ./output/scidb/10000000x100_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/10000000x10_dense.csv ./output/scidb/10000000x10_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/10000000x1_dense.csv ./output/scidb/10000000x1_dense.csv

python ./generator/conv_densecsv_to_coocsv.py ./output/csv/20000000x100_dense.csv ./output/scidb/20000000x100_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/20000000x10_dense.csv ./output/scidb/20000000x10_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/20000000x1_dense.csv ./output/scidb/20000000x1_dense.csv

python ./generator/conv_densecsv_to_coocsv.py ./output/csv/40000000x100_dense.csv ./output/scidb/40000000x100_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/40000000x10_dense.csv ./output/scidb/40000000x10_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/40000000x1_dense.csv ./output/scidb/40000000x1_dense.csv

python ./generator/conv_densecsv_to_coocsv.py ./output/csv/80000000x100_dense.csv ./output/scidb/80000000x100_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/80000000x10_dense.csv ./output/scidb/80000000x10_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/80000000x1_dense.csv ./output/scidb/80000000x1_dense.csv

python ./generator/conv_densecsv_to_coocsv.py ./output/csv/100x1_dense.csv ./output/scidb/100x1_dense.csv
python ./generator/conv_densecsv_to_coocsv.py ./output/csv/10x100_dense.csv ./output/scidb/10x100_dense.csv
