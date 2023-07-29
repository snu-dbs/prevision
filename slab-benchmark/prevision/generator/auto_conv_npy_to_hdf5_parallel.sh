source ./venv3/bin/activate

echo "convert for main comparsion"
cat ./generator/list_conv_npy_to_hdf5_1.txt | parallel --colsep ' ' -j 4 python ./generator/conv_npy_to_hdf5.py {1} {2} {3}

echo "convert for smaller tiles"
cat ./generator/list_conv_npy_to_hdf5_2.txt | parallel --colsep ' ' -j 4 python ./generator/conv_npy_to_hdf5.py {1} {2} {3}


