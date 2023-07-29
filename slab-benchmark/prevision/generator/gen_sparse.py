import os
import sys
import numpy as np
import gen_data as data

# Sparse
density = float(sys.argv[2])          # density candidates = 0.015 0.03 0.06 0.12
k = 400000000
m = 100

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

filename='{}x{}_sparse_{}.csv'.format(k, m, density)
data.gen_data_sparse(
    k, m, density, 
    'm_{}x{}_sparse_{}'.format(k, m, str(density).replace('.', '_')), 
    filename)
data.write_sparse_meta(
    'm_{}x{}_sparse_{}'.format(k, m, str(density).replace('.', '_')), 
    './output/csv/' + filename)

os.system('sudo docker cp some-postgres:/var/lib/postgresql/data/{} ./output/csv/'.format(filename))
os.system('sudo docker exec -it some-postgres rm /var/lib/postgresql/data/{}'.format(filename))

# filepath = os.path.join("./output/csv", filename)
# os.chmod(filepath, 644)
