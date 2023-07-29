source ./venv2/bin/activate

# Generate dense tall-skinny matrices
cat './generator/list_gen_csv_dense.txt' | parallel --colsep ' ' -j 4 python generator/gen_dense.py {1} {2}


# Generate sparse tall-skinny matrices.
# Since generated data using SLAB does not meet the specified density, 
#     we put more higher density and adjust it after.
# Since a parallel version of sparse generation is too buggy, 
#     we just do it sequentially.
python generator/gen_sparse.py X 0.015
python generator/gen_sparse.py X 0.03
python generator/gen_sparse.py X 0.06
python generator/gen_sparse.py X 0.12

python generator/gen_sparse.py y 0.015
python generator/gen_sparse.py y 0.03
python generator/gen_sparse.py y 0.06
python generator/gen_sparse.py y 0.12

python generator/gen_sparse.py w 0.015
python generator/gen_sparse.py w 0.03
python generator/gen_sparse.py w 0.06
python generator/gen_sparse.py w 0.12
