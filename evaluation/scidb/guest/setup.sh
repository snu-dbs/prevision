iquery -aq "load_library('dense_linear_algebra')"
iquery -aq "load_library('linear_algebra')"

# For LR
iquery -aq 'CREATE ARRAY zero_10Mx1<val:double>[row=0:9999999:0:1000;col=0:0:0:1000]'
iquery -aq 'CREATE ARRAY zero_20Mx1<val:double>[row=0:19999999:0:1000;col=0:0:0:1000]'
iquery -aq 'CREATE ARRAY zero_40Mx1<val:double>[row=0:39999999:0:1000;col=0:0:0:1000]'
iquery -aq 'CREATE ARRAY zero_80Mx1<val:double>[row=0:79999999:0:1000;col=0:0:0:1000]'

iquery -aq 'CREATE ARRAY zero_100x1<val:double>[row=0:99:0:1000;col=0:0:0:1000]'

# For NMF
iquery -aq 'CREATE ARRAY ZXHT_10M<value:double>[row=0:9999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZWHHT_10M<value:double>[row=0:9999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZXHT_20M<value:double>[row=0:19999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZWHHT_20M<value:double>[row=0:19999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZXHT_40M<value:double>[row=0:39999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZWHHT_40M<value:double>[row=0:39999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZXHT_80M<value:double>[row=0:79999999:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY ZWHHT_80M<value:double>[row=0:79999999:0:1000;col=0:9:0:1000]'

iquery -aq 'CREATE ARRAY zero_10x10<value:double>[row=0:9:0:1000;col=0:9:0:1000]'
iquery -aq 'CREATE ARRAY zero_10x100<value:double>[row=0:9:0:1000;col=0:99:0:1000]'
