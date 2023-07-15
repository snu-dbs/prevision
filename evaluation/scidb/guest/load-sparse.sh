# /bin/bash

DIR=/nas/pgtile/slab-dataset/syntactic-sparse/refined_csv
density=0.025
density2=0_025

# 100x1_sparse
iquery -aq "CREATE ARRAY mat_100x1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_100x1_sparse_"$density2"_coo,'$DIR/100x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_100x1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:99:0:100; j=0:0:0:1]), mat_100x1_sparse_"$density2")"
iquery -aq "remove(mat_100x1_sparse_"$density2"_coo)"
exit;


density=0.0125
density2=0_0125

# 100x1_sparse
iquery -aq "CREATE ARRAY mat_100x1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_100x1_sparse_"$density2"_coo,'$DIR/100x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_100x1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:99:0:100; j=0:0:0:1]), mat_100x1_sparse_"$density2")"
iquery -aq "remove(mat_100x1_sparse_"$density2"_coo)"

exit;

# 400000000x100_sparse 
iquery -aq "CREATE ARRAY mat_400Mx100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx100_sparse_"$density2"_coo,'$DIR/400000000x100_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2")"
iquery -aq "remove(mat_400Mx100_sparse_"$density2"_coo)"

# 400000000x1_sparse 
iquery -aq "CREATE ARRAY mat_400Mx1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx1_sparse_"$density2"_coo,'$DIR/400000000x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2")"
iquery -aq "remove(mat_400Mx1_sparse_"$density2"_coo)"

density=0.05
density2=0_05

# 400000000x100_sparse 
iquery -aq "CREATE ARRAY mat_400Mx100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx100_sparse_"$density2"_coo,'$DIR/400000000x100_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2")"
iquery -aq "remove(mat_400Mx100_sparse_"$density2"_coo)"

# 400000000x1_sparse 
iquery -aq "CREATE ARRAY mat_400Mx1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx1_sparse_"$density2"_coo,'$DIR/400000000x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2")"
iquery -aq "remove(mat_400Mx1_sparse_"$density2"_coo)"

exit;

density=0.1
density2=0_1

# 400000000x100_sparse 
iquery -aq "CREATE ARRAY mat_400Mx100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx100_sparse_"$density2"_coo,'$DIR/400000000x100_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2"_k)"
iquery -aq "remove(mat_400Mx100_sparse_"$density2"_coo)"

# 400000000x1_sparse 
iquery -aq "CREATE ARRAY mat_400Mx1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx1_sparse_"$density2"_coo,'$DIR/400000000x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2"_k)"
iquery -aq "remove(mat_400Mx1_sparse_"$density2"_coo)"


# 400000000x100_sparse 
iquery -aq "CREATE ARRAY mat_400Mx100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx100_sparse_"$density2"_coo,'$DIR/400000000x100_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:2000000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2"_half)"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2"_quarter)"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2"_k)"
iquery -aq "remove(mat_400Mx100_sparse_"$density2"_coo)"

# 400000000x10_sparse 
iquery -aq "CREATE ARRAY mat_400Mx10_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx10_sparse_"$density2"_coo,'$DIR/400000000x10_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx10_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:2000000; j=0:9:0:10]), mat_400Mx10_sparse_"$density2"_half)"
iquery -aq "store(redimension(mat_400Mx10_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000000; j=0:9:0:10]), mat_400Mx10_sparse_"$density2"_quarter)"
iquery -aq "store(redimension(mat_400Mx10_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000; j=0:9:0:10]), mat_400Mx10_sparse_"$density2"_k)"
iquery -aq "remove(mat_400Mx10_sparse_"$density2"_coo)"

# 400000000x1_sparse 
iquery -aq "CREATE ARRAY mat_400Mx1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx1_sparse_"$density2"_coo,'$DIR/400000000x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:2000000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2"_half)"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2"_quarter)"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:1000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2"_k)"
iquery -aq "remove(mat_400Mx1_sparse_"$density2"_coo)"
exit;

# 0.0125
# 100x1_sparse
iquery -aq "CREATE ARRAY mat_100x1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_100x1_sparse_"$density2"_coo,'$DIR/100x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_100x1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:99:0:100; j=0:0:0:1]), mat_100x1_sparse_"$density2")"
iquery -aq "remove(mat_100x1_sparse_"$density2"_coo)"

# 400000000x100_sparse 
iquery -aq "CREATE ARRAY mat_400Mx100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx100_sparse_"$density2"_coo,'$DIR/400000000x100_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:99:0:100]), mat_400Mx100_sparse_"$density2")"
iquery -aq "remove(mat_400Mx100_sparse_"$density2"_coo)"

# 400000000x10_sparse 
iquery -aq "CREATE ARRAY mat_400Mx10_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx10_sparse_"$density2"_coo,'$DIR/400000000x10_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx10_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:9:0:10]), mat_400Mx10_sparse_"$density2")"
iquery -aq "remove(mat_400Mx10_sparse_"$density2"_coo)"

# 400000000x1_sparse 
iquery -aq "CREATE ARRAY mat_400Mx1_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_400Mx1_sparse_"$density2"_coo,'$DIR/400000000x1_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_400Mx1_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:399999999:0:4000000; j=0:0:0:1]), mat_400Mx1_sparse_"$density2")"
iquery -aq "remove(mat_400Mx1_sparse_"$density2"_coo)"

# 10x100_sparse
iquery -aq "CREATE ARRAY mat_10x100_sparse_"$density2"_coo <i:int64,j:int64,value:double NOT NULL>[idx=0:*];"
iquery -aq "load(mat_10x100_sparse_"$density2"_coo,'$DIR/10x100_sparse_"$density".csv', -2, 'CSV');"
iquery -aq "store(redimension(mat_10x100_sparse_"$density2"_coo, <value:double NOT NULL>[i=0:9:0:10; j=0:99:0:100]), mat_10x100_sparse_"$density2")"
iquery -aq "remove(mat_10x100_sparse_"$density2"_coo)"
