import sys
import dask.array as da
import numpy as np

# input_mat_path = '/home/grammaright/Documents/dataset/pgtile_test_data/slab/dense/npy/10000000x100_dense.npy'
in_mat_path = sys.argv[1]
filename = in_mat_path.split('/')[-1]
shape = (
    int(filename.split('_')[0].split('x')[0]),
    int(filename.split('_')[0].split('x')[1])
)
is_tall = shape[0] > shape[1]
chunks = (int(shape[0] / 100), shape[1])
if is_tall is False:
    chunks = (shape[0], int(shape[1] / 100))

print('===============')
print(in_mat_path)
print(filename)
print(shape)
print(is_tall)
print(chunks)

memmap_data = np.memmap(in_mat_path, shape=shape, dtype='double', mode='r')
dask_data = da.from_array(memmap_data, chunks=chunks)
da.to_hdf5(in_mat_path + '.hdf5', '/data', dask_data)
