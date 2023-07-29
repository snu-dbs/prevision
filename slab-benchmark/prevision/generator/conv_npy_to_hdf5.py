import sys
import dask.array as da
import numpy as np

in_mat_path = sys.argv[1]
out_mat_path = sys.argv[2]
sp = int(sys.argv[3])
filename = in_mat_path.split('/')[-1]
shape = (
    int(filename.split('_')[0].split('x')[0]),
    int(filename.split('_')[0].split('x')[1])
)
is_tall = shape[0] > shape[1]
chunks = (int(shape[0] / sp), 100)

print('===============')
print(in_mat_path)
print(filename)
print(shape)
print(is_tall)
print(chunks)

memmap_data = np.load(in_mat_path, mmap_mode='r')
dask_data = da.from_array(memmap_data, chunks=chunks)
da.to_hdf5(out_mat_path, '/data', dask_data)
