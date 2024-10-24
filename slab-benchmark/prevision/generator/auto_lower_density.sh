source ./venv3/bin/activate

python ./generator/gen_lower_density.py ./output/csv/400000000x100_sparse_0.015.csv ./output/csv/400000000x100_sparse_0.0125.csv 0.0125
python ./generator/gen_lower_density.py ./output/csv/400000000x100_sparse_0.03.csv ./output/csv/400000000x100_sparse_0.025.csv 0.025
python ./generator/gen_lower_density.py ./output/csv/400000000x100_sparse_0.06.csv ./output/csv/400000000x100_sparse_0.05.csv 0.05
python ./generator/gen_lower_density.py ./output/csv/400000000x100_sparse_0.12.csv ./output/csv/400000000x100_sparse_0.1.csv 0.1

python ./generator/gen_lower_density.py ./output/csv/400000000x1_sparse_0.015.csv ./output/csv/400000000x1_sparse_0.0125.csv 0.0125
python ./generator/gen_lower_density.py ./output/csv/400000000x1_sparse_0.03.csv ./output/csv/400000000x1_sparse_0.025.csv 0.025
python ./generator/gen_lower_density.py ./output/csv/400000000x1_sparse_0.06.csv ./output/csv/400000000x1_sparse_0.05.csv 0.05
python ./generator/gen_lower_density.py ./output/csv/400000000x1_sparse_0.12.csv ./output/csv/400000000x1_sparse_0.1.csv 0.1

python ./generator/gen_lower_density.py ./output/csv/100x1_sparse_0.015.csv ./output/csv/100x1_sparse_0.0125.csv 0.0125
python ./generator/gen_lower_density.py ./output/csv/100x1_sparse_0.03.csv ./output/csv/100x1_sparse_0.025.csv 0.025
python ./generator/gen_lower_density.py ./output/csv/100x1_sparse_0.06.csv ./output/csv/100x1_sparse_0.05.csv 0.05
python ./generator/gen_lower_density.py ./output/csv/100x1_sparse_0.12.csv ./output/csv/100x1_sparse_0.1.csv 0.1
