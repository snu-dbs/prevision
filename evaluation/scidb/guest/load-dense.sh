# /bin/bash
DIR=/prevision/output/scidb/

# 80000000x100_dense 
iquery -aq "CREATE ARRAY mat_80Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_80Mx100_dense_coo,'$DIR/80000000x100_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_80Mx100_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:99:0:1000]), mat_80Mx100_dense)"
iquery -aq "remove(mat_80Mx100_dense_coo)"

# 40000000x100_dense 
iquery -aq "CREATE ARRAY mat_40Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_40Mx100_dense_coo,'$DIR/40000000x100_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_40Mx100_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:99:0:1000]), mat_40Mx100_dense)"
iquery -aq "remove(mat_40Mx100_dense_coo)"

# 20000000x100_dense 
iquery -aq "CREATE ARRAY mat_20Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_20Mx100_dense_coo,'$DIR/20000000x100_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_20Mx100_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:99:0:1000]), mat_20Mx100_dense)"
iquery -aq "remove(mat_20Mx100_dense_coo)"

# 10000000x100_dense
iquery -aq "CREATE ARRAY mat_10Mx100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_10Mx100_dense_coo,'$DIR/10000000x100_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_10Mx100_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:99:0:1000]), mat_10Mx100_dense)"
iquery -aq "remove(mat_10Mx100_dense_coo)"

# 80000000x10_dense 
iquery -aq "CREATE ARRAY mat_80Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_80Mx10_dense_coo,'$DIR/80000000x10_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_80Mx10_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:9:0:1000]), mat_80Mx10_dense)"
iquery -aq "remove(mat_80Mx10_dense_coo)"

# 40000000x10_dense 
iquery -aq "CREATE ARRAY mat_40Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_40Mx10_dense_coo,'$DIR/40000000x10_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_40Mx10_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:9:0:1000]), mat_40Mx10_dense)"
iquery -aq "remove(mat_40Mx10_dense_coo)"

# 20000000x10_dense 
iquery -aq "CREATE ARRAY mat_20Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_20Mx10_dense_coo,'$DIR/20000000x10_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_20Mx10_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:9:0:1000]), mat_20Mx10_dense)"
iquery -aq "remove(mat_20Mx10_dense_coo)"

# 10000000x10_dense
iquery -aq "CREATE ARRAY mat_10Mx10_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_10Mx10_dense_coo,'$DIR/10000000x10_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_10Mx10_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:9:0:1000]), mat_10Mx10_dense)"
iquery -aq "remove(mat_10Mx10_dense_coo)"

# 80000000x1_dense 
iquery -aq "CREATE ARRAY mat_80Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_80Mx1_dense_coo,'$DIR/80000000x1_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_80Mx1_dense_coo, <value:double>[i=0:79999999:0:1000; j=0:0:0:1000]), mat_80Mx1_dense)"
iquery -aq "remove(mat_80Mx1_dense_coo)"

# 40000000x1_dense 
iquery -aq "CREATE ARRAY mat_40Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_40Mx1_dense_coo,'$DIR/40000000x1_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_40Mx1_dense_coo, <value:double>[i=0:39999999:0:1000; j=0:0:0:1000]), mat_40Mx1_dense)"
iquery -aq "remove(mat_40Mx1_dense_coo)"

# 20000000x1_dense 
iquery -aq "CREATE ARRAY mat_20Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_20Mx1_dense_coo,'$DIR/20000000x1_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_20Mx1_dense_coo, <value:double>[i=0:19999999:0:1000; j=0:0:0:1000]), mat_20Mx1_dense)"
iquery -aq "remove(mat_20Mx1_dense_coo)"

# 10000000x1_dense
iquery -aq "CREATE ARRAY mat_10Mx1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_10Mx1_dense_coo,'$DIR/10000000x1_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_10Mx1_dense_coo, <value:double>[i=0:9999999:0:1000; j=0:0:0:1000]), mat_10Mx1_dense)"
iquery -aq "remove(mat_10Mx1_dense_coo)"

# 10x100_dense
iquery -aq "CREATE ARRAY mat_10x100_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_10x100_dense_coo,'$DIR/10x100_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_10x100_dense_coo, <value:double>[i=0:9:0:1000; j=0:99:0:1000]), mat_10x100_dense)"
iquery -aq "remove(mat_10x100_dense_coo)"

# 100x1_dense
iquery -aq "CREATE ARRAY mat_100x1_dense_coo <i:int64,j:int64,value:double>[idx=0:*];"
iquery -aq "load(mat_100x1_dense_coo,'$DIR/100x1_dense.csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_100x1_dense_coo, <value:double>[i=0:99:0:1000; j=0:0:0:1000]), mat_100x1_dense)"
iquery -aq "remove(mat_100x1_dense_coo)"

