import sys
import tqdm
import csv
import numpy as np
import os
import uuid

in_mat_path = sys.argv[1]
out_mat_path = sys.argv[2]

filename = in_mat_path.split('/')[-1]
shape = (
    int(filename.split('_')[0].split('x')[0]),
    int(filename.split('_')[0].split('x')[1])
)

# DENSE
tempname = uuid.uuid4()
array = np.memmap(f'{tempname}', dtype='double', mode='w+', shape=shape) 
with open(in_mat_path, 'r') as f:
    csv_reader = csv.reader(f)
    row_idx = 0
    for cells in tqdm.tqdm(csv_reader):
        for col_idx, cell in enumerate(cells):
            val = float(cell)
            array[row_idx, col_idx] = val

        row_idx += 1

np.save(out_mat_path, array)
os.remove(f'{tempname}')

