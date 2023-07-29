source ./venv3/bin/activate

cat ./generator/list_csv_to_npy.txt | parallel --colsep ' ' -j 4 python ./generator/conv_csv_to_npy.py {1} {2}