#!/bin/bash
TIMEFORMAT='%3R'

source alg.sh

bash setup.sh

echo $i

# Dense LR
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
lr 10M 3             # LR
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
lr 20M 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
lr 40M 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
lr 80M 3

# Dense NMF
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
nmf 10M 3            # NMF
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
nmf 20M 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
nmf 40M 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
nmf 80M 3

# Sparse
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sparse_lr 0_0125 3    # LR
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sparse_lr 0_025 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sparse_lr 0_05 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sparse_lr 0_1 3

# PageRank
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
pagerank enron 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
pagerank epinions 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
pagerank livejournal 3
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
pagerank twitter 3

# iteration
iterarray=(1 2 4 8 16 32)
for noi in ${iterarray[@]}
do
    echo $noi
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    nmf 10M $noi

    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    pagerank twitter $noi
done

bash clean.sh