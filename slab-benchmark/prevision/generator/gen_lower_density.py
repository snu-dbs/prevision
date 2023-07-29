from functools import reduce
from numpy.random import default_rng
from tqdm import tqdm
import numpy as np
import sys
import os
import json

in_filename = sys.argv[1]
out_filename = sys.argv[2]
out_sparsity = float(sys.argv[3])

in_sparsity = float('0.' + in_filename.split('_')[2].split('.')[1])
out_nnz = int(out_sparsity * reduce(lambda x, c: float(x) * float(c), in_filename.split('/')[-1].split('_')[0].split('x')))
in_nnz = os.popen(f'wc -l {in_filename}').read()
in_nnz = int(in_nnz.split(' ')[0])

print(f'in_filename={in_filename}, out_sparsity={out_sparsity}, in_sparsity={in_sparsity}, in_nnz={in_nnz}, out_nnz={out_nnz}')

try:
    rng = default_rng()
    numbers = np.append(np.sort(rng.choice(in_nnz - 2, in_nnz - out_nnz, replace=False)), -1)
    last_idx = 0

    with open(in_filename) as f:
        with open(out_filename, 'w') as f2:
            for idx, line in tqdm(enumerate(f)):
                if numbers[last_idx] == idx:
                    last_idx += 1
                    continue

                items = line.split(',')
                f2.write(f'{int(items[0]) - 1},{int(items[1]) - 1},{float(items[2])}\n')

    with open(f'{in_filename}.mtd', 'r') as f:
        d = json.load(f)
        d["nnz"] = out_nnz
        with open(f'{out_filename}.mtd', 'w') as f2:
            json.dump(d, f2)

except ValueError as e:
    os.system(f'cp {in_filename} {out_filename}')
    os.system(f'cp {in_filename}.mtd {out_filename}.mtd')