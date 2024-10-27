
##############################
# Figure 8
# 0 means out-of-memory error and -1 means timeout.
# unit: seconds
##############################

# 10M, 20M, 40M, 80M respectively
fig8_dense_lr_data = [
    [22, 39, 84, 267],          # PreVision
    [29, 55, 1961, 7079],       # systemds
    [372, 1361, 4390, 14929],   # mllib
    [1198, 1870, 0, 0],         # madlib
    [246, 477, 0, 0],           # scidb
    [19, 38, 247, 780],         # numpy
    [32, 386, 965, 2107]        # dask
]

# 10M, 20M, 40M, 80M respectively
fig8_dense_nmf_data = [
    [46, 87, 198, 503],          # PreVision
    [62, 251, 2862, 9868],       # systemds
    [563, 1746, 5418, 17626],    # mllib
    [4851, 9656, 0, 0],          # madlib
    [615, 1166, 0, 0],           # scidb
    [179, 553, 1843, 3828],      # numpy
    [126, 262, 1166, 2581],      # dask
]

# 0.0125, 0.025, 0.05, 0.1 respectively
fig8_sparse_lr_data = [
    [72, 101, 190, 414],         # PreVision
    [934, 1836, 4556, 13860],    # SystemDS
    [396, 1055, 2834, 9335],     # MLlib
    [23567, -1, -1, -1],         # MADlib
    [3003, 5691, 0, 0],          # SciDB
]

# Enron, Epinions, Livejournal, Twitter respectively
fig8_sparse_pr_data = [
    [0.263, 0.309, 5.395, 89.932],          # PreVision
    [0.693, 0.731, 8.746, 0],               # SystemDS
    [5.078, 5.117, 36.704, 1208.962],       # MLlib
    [3.369, 4.890, 437.503, 13876.504],     # MADlib
    [1.025, 1.172, 51.547, 1706.184],       # SciDB
]


##############################
# Figure 9
# unit: seconds
##############################

# iter=1, 2, 4, 8, 16, 32 respectively
fig9_nmf_data = [
    [30, 38, 52, 81, 139, 254],                                   # PreVision
    [39, 51, 74, 120, 210, 395],                                  # SystemDS
    [363, 456, 652, 1041, 1812, 3409],                            # MLlib
    [1525, 3002, 5948, 11842, 23436, 48441],                      # MADlib
    [202, 402, 805, 1607, 3209, 6416],                            # SciDB
    [72, 125, 231, 444, 872, 1732],                               # NumPy
    [59, 92, 187, 297, 571, 1121]                                 # Dask
]

# iter=1, 2, 4, 8, 16, 32 respectively
# Only iter=1-8 data should be used for MADlib 
fig9_pr_data = [
    [65.608, 80.413, 97.284, 129.344, 194.405, 323.680],            # PreVision 
    [820.512, 1015.105, 1403.834, 2181.300, 3731.147, 6835.602],    # MLlib
    [5614.863, 9759.132, 15890.771, 28649.901],                     # MADlib; only four data since OOM occured from iter=16
    [568.626, 1124.186, 2259.482, 4515.786, 9074.289, 18102.633]    # SciDB
]

##############################
# Figure 10
# unit: seconds
##############################

# parallelism=1, 2, 4, 8 respectively
fig10_nmf_data = [
    [46, 42, 39, 50],                             # PreVision
    [62.40, 63.05, 62.82, 62.68],                 # SystemDS
    [551, 337, 241, 209],                         # MLlib
    [4851, 4449, 4424, 4390],                     # MADlib
    [615, 400, 352, 342],                         # SciDB
    [179, 133, 134, 144],                         # NumPy
    [126, 83, 69, 71]                             # Dask
]

# parallelism=1, 2, 4, 8 respectively
# Only parallelism=1 and 2 should be used for SciDB
fig10_slr_data = [
    [72, 68, 65, 65],                         # PreVision
    [931.07, 507.76, 317.41, 310.47],         # SystemDS
    [393, 227, 148, 122],                     # MLlib
    [23567, 19151, 18322, 18343],             # MADlib
    [3003, 2125],                             # SciDB
]

##############################
# Figure 11
# unit: byte
##############################

# read and write, respectively
fig11_lr_data = [
    [134451200800, 800],               # getPos w/ PE
    [135744000800, 4838401600],        # getPos w/o PE
    [241920000800, 800],               # blocking w/ PE
    [241920000800, 4928004000],        # blocking w/o PE
]

# read and write, respectively
fig11_nmf_data = [
    [167680008000, 6400008000],        # getPos w/ PE
    [167680008000, 259968065600],      # getPos w/o PE
    [509248008000, 152192008000],      # blocking w/ PE
    [509248008000, 259840081600],      # blocking w/o PE
]

##############################
# Figure 14
# unit: microseconds
##############################

# I/O, List Maintenance, Query Planning, CPU respectively
fig14_lr_data = [
    [241126257.8, 3528.375, 22915.375, 25226479.349999994],    # OPT
    [242608661.675, 0, 22415.0, 26418022.625],                 # MRU
    [294889187.375, 5740.625, 85255.75, 29363146.850000024]    # LRU-K
]

# I/O, List Maintenance, Query Planning, CPU respectively
fig14_nmf_data = [
    [311602136.75, 22449.375, 61397.75, 190838096.125],        # OPT
    [473905106.65, 0, 66447.75, 197827862.5],                  # MRU
    [340078638.53, 24603, 190165.125, 191264481.745]           # LRU-K
]

##############################
# Figure 15
# unit: microseconds
##############################

# 100, 200, 400, 800, 1600, 3200 respectively
fig15_lr_prevision_data = [
    [265439987, 267635648, 283293168, 312722397, 329868764, 350022933], # prevision total time
    [26594, 64297, 213908, 907918, 3394511, 13663793],  # prevision overhead time
]
fig15_lr_numpy_data = 779710000   # numpy total time

# 100, 200, 400, 800, 1600, 3200 respectively
fig15_nmf_prevision_data = [
    [497009581, 494458933, 531167740, 571411875, 602693258, 643867140], # prevision total time
    [61080, 162726, 538084, 2027090, 7804991, 32119646] # prevision overhead time
]
fig15_nmf_dask_data = [2581000000, 2529000000, 2720000000, 2792000000, 3236000000, 4133000000] # dask total time
