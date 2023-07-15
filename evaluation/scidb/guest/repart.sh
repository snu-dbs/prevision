iquery -aq "store(repart(mat_20Mx1_dense, <value:double>[i=0:19999999:0; j=0:0:0]), mat_20Mx1_dense_cs)"
iquery -aq "store(repart(mat_20Mx10_dense, <value:double>[i=0:19999999:0; j=0:9:0]), mat_20Mx10_dense_cs)"
iquery -aq "store(repart(mat_20Mx100_dense, <value:double>[i=0:19999999:0; j=0:99:0]), mat_20Mx100_dense_cs)"

iquery -aq "store(repart(mat_40Mx1_dense, <value:double>[i=0:39999999:0; j=0:0:0]), mat_40Mx1_dense_cs)"
iquery -aq "store(repart(mat_40Mx10_dense, <value:double>[i=0:39999999:0; j=0:9:0]), mat_40Mx10_dense_cs)"
iquery -aq "store(repart(mat_40Mx100_dense, <value:double>[i=0:39999999:0; j=0:99:0]), mat_40Mx100_dense_cs)"

iquery -aq "store(repart(mat_80Mx1_dense, <value:double>[i=0:79999999:0; j=0:0:0]), mat_80Mx1_dense_cs)"
iquery -aq "store(repart(mat_80Mx10_dense, <value:double>[i=0:79999999:0; j=0:9:0]), mat_80Mx10_dense_cs)"
iquery -aq "store(repart(mat_80Mx100_dense, <value:double>[i=0:79999999:0; j=0:99:0]), mat_80Mx100_dense_cs)"
exit;

iquery -aq "store(repart(mat_10x100_dense, <value:double>[i=0:9:0; j=0:99:0]), mat_10x100_dense_cs)"
iquery -aq "store(repart(mat_100x1_dense, <value:double>[i=0:99:0; j=0:0:0]), mat_100x1_dense_cs)"

iquery -aq "store(repart(mat_10Mx1_dense, <value:double>[i=0:9999999:0; j=0:0:0]), mat_10Mx1_dense_cs)"
iquery -aq "store(repart(mat_10Mx10_dense, <value:double>[i=0:9999999:0; j=0:9:0]), mat_10Mx10_dense_cs)"
iquery -aq "store(repart(mat_10Mx100_dense, <value:double>[i=0:9999999:0; j=0:99:0]), mat_10Mx100_dense_cs)"
