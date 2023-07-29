source ./venv/bin/activate

DATAPATH=../../slab-benchmark/prevision/output/csv

python main.py $DATAPATH/10000000x100_dense.csv 	mat_10Mx100_dense   0
python main.py $DATAPATH/10000000x10_dense.csv 		mat_10Mx10_dense    0
python main.py $DATAPATH/10000000x1_dense.csv 		mat_10Mx1_dense     0

python main.py $DATAPATH/20000000x100_dense.csv 	mat_20Mx100_dense   0
python main.py $DATAPATH/20000000x10_dense.csv 		mat_20Mx10_dense    0
python main.py $DATAPATH/20000000x1_dense.csv 		mat_20Mx1_dense     0

python main.py $DATAPATH/40000000x100_dense.csv 	mat_40Mx100_dense   0
python main.py $DATAPATH/40000000x10_dense.csv 		mat_40Mx10_dense    0
python main.py $DATAPATH/40000000x1_dense.csv 		mat_40Mx1_dense     0

python main.py $DATAPATH/80000000x100_dense.csv 	mat_80Mx100_dense   0
python main.py $DATAPATH/80000000x10_dense.csv 		mat_80Mx10_dense    0
python main.py $DATAPATH/80000000x1_dense.csv 		mat_80Mx1_dense     0

python main.py $DATAPATH/10x100_dense.csv      		mat_10x100_dense    0

python main.py $DATAPATH/100x1_dense.csv      		vec_100x1_dense     1