import sys
import numpy as np
import gen_data as data

# Dense
# target dataset size in GB, candidiates: 2 4 8 16 32 64 
dataset_size = int(sys.argv[2])
k = int(np.ceil((dataset_size*1e9)/float(8*100)))
m = 100
batch = 2**20       # data are generated in a batch way

mattype = sys.argv[1]
if mattype == 'X':
    m = 100
elif mattype == 'W':
    m = 10
elif mattype == 'H':
    k = 10
    m = 100
elif mattype == 'y':
    m = 1
elif mattype == 'w':
    k = 100
    m = 1

data.gen_data_disk('./output/csv/{}x{}_dense.csv'.format(k, m), k, m, batch)
