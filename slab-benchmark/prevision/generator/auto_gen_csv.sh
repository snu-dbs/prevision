source ./venv2/bin/activate

# Generate dense tall-skinny matrices
python generator/gen_dense.py X 8
python generator/gen_dense.py X 16
python generator/gen_dense.py X 32
python generator/gen_dense.py X 64

python generator/gen_dense.py W 8
python generator/gen_dense.py W 16
python generator/gen_dense.py W 32
python generator/gen_dense.py W 64

python generator/gen_dense.py H 1
python generator/gen_dense.py w 1

python generator/gen_dense.py y 8
python generator/gen_dense.py y 16
python generator/gen_dense.py y 32
python generator/gen_dense.py y 64

# Generate sparse tall-skinny matrices.
# Since generated data using SLAB does not meet the specified density, 
#     we put more higher density and adjust it after.
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
